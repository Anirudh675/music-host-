// Smoke test: node test/smoke.mjs (starts the server on a random port).
import { spawn } from 'node:child_process';
import WebSocket from 'ws';
const port = 4000 + Math.floor(Math.random() * 1000);
const srv = spawn('node', ['server.js'], { env: { ...process.env, PORT: String(port) }, stdio: 'inherit' });
const base = `http://localhost:${port}`;
const fail = (m) => { console.error('FAIL', m); srv.kill(); process.exit(1); };
await new Promise((r) => setTimeout(r, 800));
const s = await (await fetch(`${base}/api/sessions`, { method: 'POST', headers: { 'content-type': 'application/json' }, body: '{}' })).json();
if (!s.id || !s.joinUrl.includes('/join/')) fail('create');
const png = await fetch(s.qrUrl); if (png.headers.get('content-type') !== 'image/png') fail('qr');
const host = new WebSocket(`ws://localhost:${port}/ws`);
const guest = new WebSocket(`ws://localhost:${port}/ws`);
const got = [];
host.on('open', () => host.send(JSON.stringify({ type: 'host', id: s.id, token: s.hostToken })));
host.on('message', (d) => {
  const m = JSON.parse(d); got.push('host:' + m.type);
  if (m.type === 'search') host.send(JSON.stringify({ type: 'results', guestId: m.guestId, reqId: m.reqId, songs: [{ id: 'x1', title: 'Song', artist: 'A' }] }));
  if (m.type === 'add') host.send(JSON.stringify({ type: 'addResult', guestId: m.guestId, songId: m.songId, ok: true }));
});
guest.on('open', () => setTimeout(() => guest.send(JSON.stringify({ type: 'guest', id: s.id, name: 'T' })), 200));
guest.on('message', (d) => {
  const m = JSON.parse(d); got.push('guest:' + m.type);
  if (m.type === 'welcome') guest.send(JSON.stringify({ type: 'add', songId: 'nope', mode: 'queue' })), guest.send(JSON.stringify({ type: 'search', q: 'so', reqId: '1' }));
  if (m.type === 'results') setTimeout(() => guest.send(JSON.stringify({ type: 'add', songId: 'x1', mode: 'queue' })), 1600);
  if (m.type === 'added' && m.ok) {
    if (!got.includes('guest:error')) fail('unknown song should be rejected');
    console.log('PASS', got.join(' '));
    srv.kill(); process.exit(0);
  }
});
setTimeout(() => fail('timeout ' + got.join(' ')), 8000);
