/// Reactive change streams.
///
/// Two pieces live here:
///
/// - [ReactiveStream]: immutable operator chaining over a stream of
///   [DatabaseChangeEvent]s. Every operator returns a NEW stream and keeps
///   its state PER SUBSCRIPTION, so two listeners on the same broadcast
///   source never share timers, counters or distinct sets.
/// - [ChangeStreamHub]: the pattern-subscription registry the database
///   facade publishes into. Controllers are created on demand, removed when
///   their last subscriber leaves, and matched in O(key length) for exact
///   and `prefix*` patterns.
///
/// ## Backpressure
///
/// Database change events are push-based: a write never waits for a
/// listener. An unlistened pattern drops events (there is no controller to
/// hold them). A paused or slow subscription queues its events, up to
/// [ChangeStreamHub.maxBufferedEvents] per subscription; one more event
/// ends that subscription with a [ChangeStreamOverflowException] instead of
/// growing without bound or dropping events silently. A consumer that sees
/// it should re-read the state it cares about and subscribe again. Slow
/// consumers should bound their intake with [ReactiveStream.debounce],
/// [ReactiveStream.throttle], [ReactiveStream.buffer] or
/// [ReactiveStream.bufferTime].
library;

import 'dart:async';
import 'dart:collection';

import '../../domain/entities/database_entity.dart';
import '../errors/exceptions.dart';

/// Immutable fluent wrapper over a stream of database change events.
///
/// Operators never mutate the receiver: each call wraps the previous stream
/// and returns a new [ReactiveStream], and all operator state (timers,
/// counters, seen-keys) is created inside `onListen`, once per subscription.
class ReactiveStream {
  /// Wraps [source] without transforming it.
  const ReactiveStream(Stream<DatabaseChangeEvent> source) : _source = source;

  final Stream<DatabaseChangeEvent> _source;

  /// Keeps only events matching [test].
  ReactiveStream where(bool Function(DatabaseChangeEvent event) test) =>
      ReactiveStream(_source.where(test));

  /// Maps events to values of type [T].
  Stream<T> map<T>(T Function(DatabaseChangeEvent event) convert) =>
      _source.map(convert);

  /// Suppresses events whose key (from [keySelector], defaulting to the
  /// event's `key`) equals the previously emitted event's key.
  ///
  /// Unlike the historical implementation this actually filters: repeated
  /// events for the same key are dropped until a different key appears.
  ReactiveStream distinct([
    Object? Function(DatabaseChangeEvent event)? keySelector,
  ]) {
    final Object? Function(DatabaseChangeEvent) select =
        keySelector ?? (DatabaseChangeEvent e) => e.key;
    final Stream<DatabaseChangeEvent> source = _source;
    return ReactiveStream(
      Stream<DatabaseChangeEvent>.multi((controller) {
        Object? lastKey;
        bool hasLast = false;
        final StreamSubscription<DatabaseChangeEvent> subscription = source
            .listen(
              (DatabaseChangeEvent event) {
                final Object? key = select(event);
                if (!hasLast || key != lastKey) {
                  hasLast = true;
                  lastKey = key;
                  controller.add(event);
                }
              },
              onError: controller.addError,
              onDone: controller.close,
            );
        _wire(controller, subscription);
      }),
    );
  }

  /// Emits an event only after [duration] has passed without a newer one.
  ///
  /// The pending event is emitted exactly once: either by its timer or,
  /// when the source completes first, by the completion flush - never both.
  ReactiveStream debounce(Duration duration) {
    final Stream<DatabaseChangeEvent> source = _source;
    return ReactiveStream(
      Stream<DatabaseChangeEvent>.multi((controller) {
        Timer? timer;
        DatabaseChangeEvent? pending;
        bool hasPending = false;
        final StreamSubscription<DatabaseChangeEvent> subscription = source
            .listen(
              (DatabaseChangeEvent event) {
                pending = event;
                hasPending = true;
                timer?.cancel();
                timer = Timer(duration, () {
                  if (hasPending) {
                    final DatabaseChangeEvent toEmit = pending!;
                    hasPending = false;
                    pending = null;
                    controller.add(toEmit);
                  }
                });
              },
              onError: controller.addError,
              onDone: () {
                timer?.cancel();
                if (hasPending) {
                  final DatabaseChangeEvent toEmit = pending!;
                  hasPending = false;
                  pending = null;
                  controller.add(toEmit);
                }
                controller.close();
              },
            );
        controller.onCancel = () {
          timer?.cancel();
          return subscription.cancel();
        };
        controller.onPause = subscription.pause;
        controller.onResume = subscription.resume;
      }),
    );
  }

  /// Emits an event, then ignores further events for [duration].
  ReactiveStream throttle(Duration duration) {
    final Stream<DatabaseChangeEvent> source = _source;
    return ReactiveStream(
      Stream<DatabaseChangeEvent>.multi((controller) {
        DateTime? lastEmit;
        final StreamSubscription<DatabaseChangeEvent> subscription = source
            .listen(
              (DatabaseChangeEvent event) {
                final DateTime now = DateTime.now();
                final DateTime? last = lastEmit;
                if (last == null || now.difference(last) >= duration) {
                  lastEmit = now;
                  controller.add(event);
                }
              },
              onError: controller.addError,
              onDone: controller.close,
            );
        _wire(controller, subscription);
      }),
    );
  }

  /// Emits only the first [count] events, then completes. Each subscription
  /// gets its own counter.
  ReactiveStream take(int count) {
    final Stream<DatabaseChangeEvent> source = _source;
    return ReactiveStream(
      Stream<DatabaseChangeEvent>.multi((controller) {
        int taken = 0;
        late final StreamSubscription<DatabaseChangeEvent> subscription;
        subscription = source.listen(
          (DatabaseChangeEvent event) {
            if (taken >= count) return;
            taken++;
            controller.add(event);
            if (taken >= count) {
              subscription.cancel();
              controller.close();
            }
          },
          onError: controller.addError,
          onDone: controller.close,
        );
        if (count <= 0) {
          subscription.cancel();
          controller.close();
          return;
        }
        _wire(controller, subscription);
      }),
    );
  }

  /// Skips the first [count] events. Each subscription gets its own counter.
  ReactiveStream skip(int count) {
    final Stream<DatabaseChangeEvent> source = _source;
    return ReactiveStream(
      Stream<DatabaseChangeEvent>.multi((controller) {
        int skipped = 0;
        final StreamSubscription<DatabaseChangeEvent> subscription = source
            .listen(
              (DatabaseChangeEvent event) {
                if (skipped < count) {
                  skipped++;
                  return;
                }
                controller.add(event);
              },
              onError: controller.addError,
              onDone: controller.close,
            );
        _wire(controller, subscription);
      }),
    );
  }

  /// Groups events into lists of [count]; a final partial batch is emitted
  /// when the source completes.
  Stream<List<DatabaseChangeEvent>> buffer(int count) {
    final Stream<DatabaseChangeEvent> source = _source;
    return Stream<List<DatabaseChangeEvent>>.multi((controller) {
      List<DatabaseChangeEvent> batch = [];
      final StreamSubscription<DatabaseChangeEvent> subscription = source
          .listen(
            (DatabaseChangeEvent event) {
              batch.add(event);
              if (batch.length >= count) {
                final List<DatabaseChangeEvent> toEmit = batch;
                batch = [];
                controller.add(toEmit);
              }
            },
            onError: controller.addError,
            onDone: () {
              if (batch.isNotEmpty) {
                controller.add(batch);
                batch = [];
              }
              controller.close();
            },
          );
      controller.onCancel = subscription.cancel;
      controller.onPause = subscription.pause;
      controller.onResume = subscription.resume;
    });
  }

  /// Groups events into lists flushed [duration] after each batch's first
  /// event; a final partial batch is emitted when the source completes.
  Stream<List<DatabaseChangeEvent>> bufferTime(Duration duration) {
    final Stream<DatabaseChangeEvent> source = _source;
    return Stream<List<DatabaseChangeEvent>>.multi((controller) {
      List<DatabaseChangeEvent> batch = [];
      Timer? timer;
      final StreamSubscription<DatabaseChangeEvent> subscription = source
          .listen(
            (DatabaseChangeEvent event) {
              if (batch.isEmpty) {
                timer = Timer(duration, () {
                  if (batch.isNotEmpty) {
                    final List<DatabaseChangeEvent> toEmit = batch;
                    batch = [];
                    controller.add(toEmit);
                  }
                });
              }
              batch.add(event);
            },
            onError: controller.addError,
            onDone: () {
              timer?.cancel();
              if (batch.isNotEmpty) {
                controller.add(batch);
                batch = [];
              }
              controller.close();
            },
          );
      controller.onCancel = () {
        timer?.cancel();
        return subscription.cancel();
      };
      controller.onPause = subscription.pause;
      controller.onResume = subscription.resume;
    });
  }

  /// Subscribes to the composed stream.
  StreamSubscription<DatabaseChangeEvent> listen(
    void Function(DatabaseChangeEvent event) onData, {
    Function? onError,
    void Function()? onDone,
    bool? cancelOnError,
  }) => _source.listen(
    onData,
    onError: onError,
    onDone: onDone,
    cancelOnError: cancelOnError,
  );

  /// The composed stream.
  Stream<DatabaseChangeEvent> asStream() => _source;

  static void _wire(
    MultiStreamController<DatabaseChangeEvent> controller,
    StreamSubscription<DatabaseChangeEvent> subscription,
  ) {
    controller.onCancel = subscription.cancel;
    controller.onPause = subscription.pause;
    controller.onResume = subscription.resume;
  }
}

/// Extension to lift a change stream into a [ReactiveStream].
extension ReactiveStreamExtension on Stream<DatabaseChangeEvent> {
  /// Wraps this stream in a [ReactiveStream].
  ReactiveStream get reactive => ReactiveStream(this);
}

/// Loads the current state matching a watch pattern, expressed as synthetic
/// change events, for initial-snapshot delivery.
typedef SnapshotLoader = Future<List<DatabaseChangeEvent>> Function();

/// Registry of pattern subscriptions that the database facade publishes
/// change events into.
///
/// Supported patterns:
/// - `*` matches every key;
/// - `prefix*` (single trailing `*`) matches keys starting with `prefix`;
/// - any other pattern without `*` matches that exact key;
/// - patterns with `*` elsewhere are treated as globs (each `*` matching any
///   run of characters) and checked per event - supported but O(#globs) per
///   publish, so prefer prefix patterns.
///
/// Controllers are created lazily on first listen and disposed when the last
/// subscriber cancels, so idle patterns hold no memory. Publishing costs
/// O(key length + #glob patterns), not O(#patterns).
final class ChangeStreamHub {
  /// Creates an empty hub whose subscriptions queue at most
  /// [maxBufferedEvents] undelivered events each.
  ChangeStreamHub({this.maxBufferedEvents = defaultMaxBufferedEvents})
    : assert(maxBufferedEvents > 0);

  /// Default for [maxBufferedEvents].
  static const int defaultMaxBufferedEvents = 10000;

  /// Undelivered events one subscription may queue (while paused, or while
  /// its snapshot loads) before it is ended with a
  /// [ChangeStreamOverflowException].
  final int maxBufferedEvents;

  final Map<String, StreamController<DatabaseChangeEvent>> _exact = {};
  final Map<String, StreamController<DatabaseChangeEvent>> _byPrefix = {};
  final Map<String, _GlobWatcher> _globs = {};
  bool _closed = false;

  /// Whether [close] has been called.
  bool get isClosed => _closed;

  /// Number of patterns with live controllers (for diagnostics and tests).
  int get activePatternCount =>
      _exact.length + _byPrefix.length + _globs.length;

  /// Delivers [event] to every matching pattern subscription.
  ///
  /// Must be called only AFTER the write's durability point.
  void publish(DatabaseChangeEvent event) {
    if (_closed) {
      throw const DatabaseClosedException('ChangeStreamHub is closed');
    }
    _exact[event.key]?.add(event);

    if (_byPrefix.isNotEmpty) {
      if (_byPrefix.length <= event.key.length + 1) {
        for (final MapEntry<String, StreamController<DatabaseChangeEvent>> entry
            in _byPrefix.entries) {
          if (event.key.startsWith(entry.key)) {
            entry.value.add(event);
          }
        }
      } else {
        for (int i = 0; i <= event.key.length; i++) {
          _byPrefix[event.key.substring(0, i)]?.add(event);
        }
      }
    }

    for (final _GlobWatcher watcher in _globs.values) {
      if (watcher.regex.hasMatch(event.key)) {
        watcher.controller.add(event);
      }
    }
  }

  /// Returns the live event stream for [pattern].
  ///
  /// When [initialEvents] is provided, each new subscription first receives
  /// that snapshot and then live events. Live events arriving while the
  /// snapshot loads are queued and delivered afterwards, so no write between
  /// a caller's read and its listen is ever silently missed; a write racing
  /// the snapshot may be observed twice (once inside the snapshot and once
  /// live) - consumers needing exactly-once should deduplicate by key and
  /// timestamp.
  Stream<DatabaseChangeEvent> watch(
    String pattern, {
    SnapshotLoader? initialEvents,
  }) {
    if (_closed) {
      throw const DatabaseClosedException('ChangeStreamHub is closed');
    }
    // Multi stream so each listener triggers controller acquisition and
    // release independently, with its own bounded queue.
    return Stream<DatabaseChangeEvent>.multi((controller) {
      final Queue<DatabaseChangeEvent> queued = Queue<DatabaseChangeEvent>();
      bool snapshotDone = initialEvents == null;
      bool paused = false;
      bool ended = false;
      late final StreamSubscription<DatabaseChangeEvent> subscription;

      void drain() {
        while (!ended && !paused && snapshotDone && queued.isNotEmpty) {
          controller.add(queued.removeFirst());
        }
      }

      void overflow() {
        ended = true;
        final int dropped = queued.length + 1;
        queued.clear();
        unawaited(subscription.cancel());
        controller.addError(
          ChangeStreamOverflowException(
            'A change stream for "$pattern" fell $dropped events behind '
            '(limit $maxBufferedEvents) and was closed. Re-read the state you '
            'need and subscribe again, or consume events faster.',
            pattern: pattern,
          ),
        );
        unawaited(controller.close());
      }

      subscription = _streamFor(pattern).listen(
        (DatabaseChangeEvent event) {
          if (ended) return;
          if (!paused && snapshotDone && queued.isEmpty) {
            controller.add(event);
            return;
          }
          if (queued.length >= maxBufferedEvents) {
            overflow();
            return;
          }
          queued.add(event);
        },
        onError: controller.addError,
        onDone: () {
          if (ended) return;
          drain();
          controller.close();
        },
      );
      controller
        ..onCancel = () {
          ended = true;
          return subscription.cancel();
        }
        ..onPause = () {
          paused = true;
        }
        ..onResume = () {
          paused = false;
          drain();
        };

      if (initialEvents != null) {
        Future<void>(() async {
          try {
            final List<DatabaseChangeEvent> snapshot = await initialEvents();
            for (final DatabaseChangeEvent event in snapshot) {
              if (ended) return;
              controller.add(event);
            }
          } catch (error, stackTrace) {
            if (!ended) controller.addError(error, stackTrace);
          } finally {
            snapshotDone = true;
            drain();
          }
        });
      }
    });
  }

  /// Closes every controller and rejects further publishes and watches.
  Future<void> close() async {
    if (_closed) return;
    _closed = true;
    final List<Future<void>> pending = [
      for (final StreamController<DatabaseChangeEvent> c in _exact.values)
        c.close(),
      for (final StreamController<DatabaseChangeEvent> c in _byPrefix.values)
        c.close(),
      for (final _GlobWatcher w in _globs.values) w.controller.close(),
    ];
    _exact.clear();
    _byPrefix.clear();
    _globs.clear();
    await Future.wait(pending);
  }

  Stream<DatabaseChangeEvent> _streamFor(String pattern) {
    final int starIndex = pattern.indexOf('*');
    if (starIndex < 0) {
      return _acquire(_exact, pattern).stream;
    }
    if (starIndex == pattern.length - 1) {
      final String prefix = pattern.substring(0, pattern.length - 1);
      return _acquire(_byPrefix, prefix).stream;
    }
    return _acquireGlob(pattern).controller.stream;
  }

  StreamController<DatabaseChangeEvent> _acquire(
    Map<String, StreamController<DatabaseChangeEvent>> registry,
    String key,
  ) {
    return registry.putIfAbsent(key, () {
      late final StreamController<DatabaseChangeEvent> controller;
      controller = StreamController<DatabaseChangeEvent>.broadcast(
        onCancel: () {
          // Last subscriber left: release the controller.
          if (!controller.hasListener && identical(registry[key], controller)) {
            registry.remove(key);
            controller.close();
          }
        },
      );
      return controller;
    });
  }

  _GlobWatcher _acquireGlob(String pattern) {
    return _globs.putIfAbsent(pattern, () {
      final RegExp regex = RegExp(
        '^${pattern.split('*').map(RegExp.escape).join('.*')}\$',
      );
      late final _GlobWatcher watcher;
      watcher = _GlobWatcher(
        regex,
        StreamController<DatabaseChangeEvent>.broadcast(
          onCancel: () {
            if (!watcher.controller.hasListener &&
                identical(_globs[pattern], watcher)) {
              _globs.remove(pattern);
              watcher.controller.close();
            }
          },
        ),
      );
      return watcher;
    });
  }
}

final class _GlobWatcher {
  _GlobWatcher(this.regex, this.controller);

  final RegExp regex;
  final StreamController<DatabaseChangeEvent> controller;
}
