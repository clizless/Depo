
import 'dart:async';
import 'dart:convert';
import 'dart:io';
import 'dart:math';
import 'dart:typed_data';

import 'package:file_picker/file_picker.dart';
import 'package:flutter/material.dart';

const udpPort = 45454;
const tcpPort = 45455;

void main() => runApp(const App());

class App extends StatelessWidget {
  const App({super.key});

  @override
  Widget build(BuildContext context) {
    const seed = Color(0xFF2457F5);
    return MaterialApp(
      title: 'WiFi Dosya Aktar',
      theme: ThemeData(colorSchemeSeed: seed, useMaterial3: true),
      darkTheme: ThemeData(
          colorSchemeSeed: seed,
          brightness: Brightness.dark,
          useMaterial3: true),
      home: const Home(),
    );
  }
}

class Peer {
  final String id;
  String name;
  String ip;
  DateTime seen;
  Peer(this.id, this.name, this.ip, this.seen);
}

class Incoming {
  final String name;
  final int size;
  final String path;
  int got = 0;
  bool done = false;
  bool saved = false;
  Incoming(this.name, this.size, this.path);
}

class Home extends StatefulWidget {
  const Home({super.key});

  @override
  State<Home> createState() => _HomeState();
}

class _HomeState extends State<Home> {
  final id = Random().nextInt(1 << 31).toString();
  late String myName;
  final nameCtl = TextEditingController();
  final peers = <String, Peer>{};
  final trusted = <String>{};
  final incoming = <Incoming>[];
  String status = '';
  RawDatagramSocket? udp;
  ServerSocket? server;
  Timer? timer;

  @override
  void initState() {
    super.initState();
    myName = 'Telefon ${100 + Random().nextInt(900)}';
    nameCtl.text = myName;
    start();
  }

  @override
  void dispose() {
    timer?.cancel();
    udp?.close();
    server?.close();
    nameCtl.dispose();
    super.dispose();
  }

  void note(String s) {
    if (mounted) setState(() => status = s);
  }

  String clean(Object? s) {
    final t = (s ?? '').toString().replaceAll(RegExp(r'[\r\n]'), ' ').trim();
    if (t.isEmpty) return 'Bilinmeyen cihaz';
    return t.length > 30 ? t.substring(0, 30) : t;
  }

  Future<void> start() async {
    try {
      udp = await RawDatagramSocket.bind(InternetAddress.anyIPv4, udpPort,
          reuseAddress: true);
      udp!.broadcastEnabled = true;
      udp!.listen((e) {
        if (e != RawSocketEvent.read) return;
        final d = udp!.receive();
        if (d == null) return;
        try {
          final m = jsonDecode(utf8.decode(d.data)) as Map;
          if (m['id'] == id) return;
          peers['${m['id']}'] =
              Peer('${m['id']}', clean(m['name']), d.address.address, DateTime.now());
          if (mounted) setState(() {});
        } catch (_) {}
      });
      server = await ServerSocket.bind(InternetAddress.anyIPv4, tcpPort,
          shared: true);
      server!.listen(handle);
      timer = Timer.periodic(const Duration(seconds: 2), (_) {
        announce();
        peers.removeWhere(
            (k, p) => DateTime.now().difference(p.seen).inSeconds > 7);
        if (mounted) setState(() {});
      });
      announce();
    } catch (e) {
      note('Ağ başlatılamadı. WiFi\'ye bağlı olduğunuzdan emin olun.');
    }
  }

  void announce() {
    final data = utf8.encode(jsonEncode({'id': id, 'name': myName}));
    udp?.send(data, InternetAddress('255.255.255.255'), udpPort);
  }

  // ---- alma tarafı ----

  Future<bool> ask(String name, String ip) async {
    final ok = await showDialog<bool>(
          context: context,
          barrierDismissible: false,
          builder: (c) => AlertDialog(
            title: Text('$name bağlanmak istiyor'),
            content: const Text(
                'Kabul ederseniz bu cihazla dosya alışverişi yapabilirsiniz.'),
            actions: [
              TextButton(
                  onPressed: () => Navigator.pop(c, false),
                  child: const Text('Reddet')),
              FilledButton(
                  onPressed: () => Navigator.pop(c, true),
                  child: const Text('Kabul et')),
            ],
          ),
        ) ??
        false;
    if (ok) {
      trusted.add(ip);
      if (mounted) setState(() {});
    }
    return ok;
  }

  void handle(Socket s) {
    final ip = s.remoteAddress.address;
    final hdr = <int>[];
    var gotHeader = false;
    IOSink? out;
    Incoming? inc;

    void feed(List<int> data) {
      final i = inc!;
      out!.add(data);
      i.got += data.length;
      if (i.got >= i.size) {
        out!.close();
        out = null;
        i.done = true;
      }
      if (mounted) setState(() {});
    }

    late StreamSubscription<Uint8List> sub;
    sub = s.listen((chunk) {
      if (gotHeader) {
        if (inc != null && !inc!.done) feed(chunk);
        return;
      }
      hdr.addAll(chunk);
      final nl = hdr.indexOf(10);
      if (nl < 0) {
        if (hdr.length > 4096) s.destroy();
        return;
      }
      gotHeader = true;
      Map h;
      try {
        h = jsonDecode(utf8.decode(hdr.sublist(0, nl))) as Map;
      } catch (_) {
        s.destroy();
        return;
      }
      final rest = hdr.sublist(nl + 1);
      if (h['t'] == 'req') {
        sub.pause();
        ask(clean(h['name']), ip).then((ok) async {
          s.write(ok ? '{"t":"ok"}\n' : '{"t":"no"}\n');
          await s.flush();
          await s.close();
        });
      } else if (h['t'] == 'file' && trusted.contains(ip)) {
        final name = clean(h['name']).split(RegExp(r'[\\/]')).last;
        final size = (h['size'] as num).toInt();
        final path =
            '${Directory.systemTemp.path}/aktar_${DateTime.now().microsecondsSinceEpoch}_$name';
        final i = Incoming(name, size, path);
        inc = i;
        out = File(path).openWrite();
        if (mounted) setState(() => incoming.insert(0, i));
        if (size == 0) {
          out!.close();
          out = null;
          i.done = true;
          if (mounted) setState(() {});
        } else if (rest.isNotEmpty) {
          feed(rest);
        }
      } else {
        s.destroy();
      }
    }, onError: (_) => s.destroy(), onDone: () => out?.close());
  }

  Future<void> save(Incoming i) async {
    try {
      final bytes = await File(i.path).readAsBytes();
      final r = await FilePicker.platform.saveFile(fileName: i.name, bytes: bytes);
      if (r != null && mounted) setState(() => i.saved = true);
    } catch (e) {
      note('Kaydedilemedi: $e');
    }
  }

  // ---- gönderme tarafı ----

  Future<void> request(Peer p) async {
    note('${p.name} cihazının onayı bekleniyor…');
    try {
      final s = await Socket.connect(p.ip, tcpPort,
          timeout: const Duration(seconds: 5));
      s.write('${jsonEncode({'t': 'req', 'name': myName})}\n');
      await s.flush();
      final reply =
          await utf8.decodeStream(s).timeout(const Duration(seconds: 60));
      s.destroy();
      if (reply.contains('"ok"')) {
        trusted.add(p.ip);
        note('${p.name} ile bağlandınız.');
      } else {
        note('${p.name} bağlantıyı kabul etmedi.');
      }
    } catch (_) {
      note('Bağlanılamadı. İki telefon aynı WiFi ağında mı?');
    }
  }

  Future<void> sendFiles(Peer p) async {
    final res = await FilePicker.platform.pickFiles(allowMultiple: true);
    if (res == null) return;
    for (final pf in res.files) {
      if (pf.path == null) continue;
      final file = File(pf.path!);
      final size = await file.length();
      var sent = 0;
      try {
        final s = await Socket.connect(p.ip, tcpPort,
            timeout: const Duration(seconds: 5));
        s.write('${jsonEncode({'t': 'file', 'name': pf.name, 'size': size})}\n');
        await s.addStream(file.openRead().map((c) {
          sent += c.length;
          note('${pf.name}: %${size == 0 ? 100 : (sent * 100 ~/ size)}');
          return c;
        }));
        await s.flush();
        await s.close();
        note('${pf.name} gönderildi.');
      } catch (_) {
        note('${pf.name} gönderilemedi.');
      }
    }
  }

  // ---- arayüz ----

  @override
  Widget build(BuildContext context) {
    final t = Theme.of(context).textTheme;
    final list = peers.values.toList();
    return Scaffold(
      appBar: AppBar(title: const Text('WiFi Dosya Aktar')),
      body: ListView(
        padding: const EdgeInsets.all(16),
        children: [
          TextField(
            controller: nameCtl,
            maxLength: 24,
            decoration: const InputDecoration(
                labelText: 'Bu cihazın adı', border: OutlineInputBorder()),
            onChanged: (v) {
              if (v.trim().isNotEmpty) myName = v.trim();
            },
          ),
          const SizedBox(height: 8),
          Text('Yakındaki cihazlar', style: t.titleMedium),
          const SizedBox(height: 8),
          if (list.isEmpty)
            const Text(
                'Henüz cihaz yok. Diğer telefonda da uygulamayı açın ve aynı WiFi ağında olduğunuzdan emin olun.'),
          for (final p in list)
            Card(
              child: ListTile(
                title: Text(p.name),
                subtitle: Text(trusted.contains(p.ip) ? 'Bağlı' : 'Bağlanmaya hazır'),
                trailing: FilledButton(
                  onPressed: () =>
                      trusted.contains(p.ip) ? sendFiles(p) : request(p),
                  child: Text(trusted.contains(p.ip) ? 'Dosya gönder' : 'Bağlan'),
                ),
              ),
            ),
          if (status.isNotEmpty)
            Padding(
              padding: const EdgeInsets.symmetric(vertical: 12),
              child: Text(status, style: t.bodyMedium),
            ),
          const SizedBox(height: 8),
          Text('Gelen dosyalar', style: t.titleMedium),
          const SizedBox(height: 8),
          if (incoming.isEmpty) const Text('Henüz dosya gelmedi.'),
          for (final i in incoming)
            Card(
              child: ListTile(
                title: Text(i.name),
                subtitle: i.done
                    ? Text(i.saved ? 'Kaydedildi' : 'Alındı')
                    : LinearProgressIndicator(
                        value: i.size == 0 ? 1 : i.got / i.size),
                trailing: i.done && !i.saved
                    ? OutlinedButton(
                        onPressed: () => save(i), child: const Text('Kaydet'))
                    : null,
              ),
            ),
        ],
      ),
    );
  }
                              }
