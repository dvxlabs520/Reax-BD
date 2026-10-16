import 'package:flutter/material.dart';

import 'screens/backup_demo_screen.dart';
import 'screens/chat_demo_screen.dart';
import 'screens/collection_demo_screen.dart';
import 'screens/durability_demo_screen.dart';
import 'screens/iteration_demo_screen.dart';
import 'screens/overview_screen.dart';
import 'screens/simple_demo_screen.dart';
import 'screens/todo_demo_screen.dart';
import 'screens/transactions_demo_screen.dart';
import 'screens/ttl_demo_screen.dart';

void main() {
  runApp(const ReaxDBExampleApp());
}

/// The example application: a catalogue of small, self-contained demos, one
/// per ReaxDB feature.
class ReaxDBExampleApp extends StatelessWidget {
  /// Creates the example app.
  const ReaxDBExampleApp({super.key});

  @override
  Widget build(BuildContext context) {
    return MaterialApp(
      title: 'ReaxDB Example',
      debugShowCheckedModeBanner: false,
      theme: ThemeData(
        useMaterial3: true,
        colorScheme: ColorScheme.fromSeed(seedColor: const Color(0xFF1565C0)),
        appBarTheme: const AppBarTheme(centerTitle: false),
        cardTheme: CardThemeData(
          elevation: 0,
          shape: RoundedRectangleBorder(
            borderRadius: BorderRadius.circular(12),
            side: const BorderSide(color: Color(0x1F000000)),
          ),
        ),
      ),
      home: const HomeScreen(),
    );
  }
}

/// One entry of the demo catalogue.
@immutable
class Demo {
  /// Creates a catalogue entry.
  const Demo({
    required this.title,
    required this.summary,
    required this.api,
    required this.icon,
    required this.builder,
  });

  /// Name of the demo.
  final String title;

  /// One sentence describing what it shows.
  final String summary;

  /// The ReaxDB API the demo is about.
  final String api;

  /// Icon shown in the catalogue.
  final IconData icon;

  /// Builds the demo screen.
  final WidgetBuilder builder;
}

/// The demos this example ships with, in the order they are listed.
final List<Demo> demos = <Demo>[
  Demo(
    title: 'Overview',
    summary:
        'Database information, live cache and storage statistics, '
        'maintenance and a schema migration.',
    api: 'ReaxDB.open, info(), statistics(), flush(), compact(), onUpgrade',
    icon: Icons.dashboard_outlined,
    builder: (_) => const OverviewScreen(),
  ),
  Demo(
    title: 'Key/value basics',
    summary: 'The small API: put, get, delete and pattern queries.',
    api: 'ReaxDB.simple, SimpleReaxDB',
    icon: Icons.widgets_outlined,
    builder: (_) => const SimpleDemoScreen(),
  ),
  Demo(
    title: 'Typed collections',
    summary:
        'Store a Dart class without code generation, index it and query it.',
    api: 'db.collection<T>(), createIndex(), find(), watch()',
    icon: Icons.category_outlined,
    builder: (_) => const CollectionDemoScreen(),
  ),
  Demo(
    title: 'Iteration and scans',
    summary: 'Ordered iteration over the key space: prefixes, ranges, limits.',
    api: 'scan(), scanPrefix(), keys(), range()',
    icon: Icons.list_alt_outlined,
    builder: (_) => const IterationDemoScreen(),
  ),
  Demo(
    title: 'Expiring entries',
    summary: 'Entries that expire on their own, and reclaiming their space.',
    api: 'put(ttl:), ReaxEntry.expiresAt, purgeExpired()',
    icon: Icons.timer_outlined,
    builder: (_) => const TtlDemoScreen(),
  ),
  Demo(
    title: 'Transactions',
    summary: 'An atomic transfer, a rollback, and a compare-and-swap.',
    api: 'transaction(), IsolationLevel, compareAndSwap()',
    icon: Icons.swap_horiz_outlined,
    builder: (_) => const TransactionsDemoScreen(),
  ),
  Demo(
    title: 'Backup and restore',
    summary:
        'Export a snapshot, restore it into an encrypted database, and see '
        'the checksum reject a damaged archive.',
    api: 'exportTo(), ReaxDB.importFrom(), EncryptionConfig.aes256',
    icon: Icons.backup_outlined,
    builder: (_) => const BackupDemoScreen(),
  ),
  Demo(
    title: 'Durability modes',
    summary: 'What each SyncMode costs, measured on this device.',
    api: 'SyncMode.none / os / full, put(sync:)',
    icon: Icons.save_outlined,
    builder: (_) => const DurabilityDemoScreen(),
  ),
  Demo(
    title: 'Todo list',
    summary: 'A small app built on prefix scans and change streams.',
    api: 'scanPrefix(), watchPrefix()',
    icon: Icons.checklist_outlined,
    builder: (_) => const TodoDemoScreen(),
  ),
  Demo(
    title: 'Chat',
    summary: 'An append-only log rendered live from a change stream.',
    api: 'watchPrefix(), scanPrefix(reverse:)',
    icon: Icons.forum_outlined,
    builder: (_) => const ChatDemoScreen(),
  ),
];

/// The catalogue of demos.
class HomeScreen extends StatelessWidget {
  /// Creates the catalogue screen.
  const HomeScreen({super.key});

  @override
  Widget build(BuildContext context) {
    final ThemeData theme = Theme.of(context);
    return Scaffold(
      appBar: AppBar(
        title: const Text('ReaxDB Example'),
        bottom: PreferredSize(
          preferredSize: const Size.fromHeight(28),
          child: Padding(
            padding: const EdgeInsets.only(left: 16, bottom: 12),
            child: Align(
              alignment: Alignment.centerLeft,
              child: Text(
                'One screen per feature of ReaxDB 2.0',
                style: theme.textTheme.bodySmall,
              ),
            ),
          ),
        ),
      ),
      body: ListView.separated(
        padding: const EdgeInsets.all(16),
        itemCount: demos.length,
        separatorBuilder: (_, _) => const SizedBox(height: 8),
        itemBuilder: (BuildContext context, int index) {
          final Demo demo = demos[index];
          return Card(
            child: ListTile(
              contentPadding: const EdgeInsets.symmetric(
                horizontal: 16,
                vertical: 8,
              ),
              leading: Icon(demo.icon, color: theme.colorScheme.primary),
              title: Text(demo.title),
              subtitle: Column(
                crossAxisAlignment: CrossAxisAlignment.start,
                children: <Widget>[
                  const SizedBox(height: 4),
                  Text(demo.summary),
                  const SizedBox(height: 6),
                  Text(
                    demo.api,
                    style: theme.textTheme.bodySmall?.copyWith(
                      fontFamily: 'monospace',
                      color: theme.colorScheme.primary,
                    ),
                  ),
                ],
              ),
              trailing: const Icon(Icons.chevron_right),
              onTap:
                  () => Navigator.of(
                    context,
                  ).push(MaterialPageRoute<void>(builder: demo.builder)),
            ),
          );
        },
      ),
    );
  }
}
