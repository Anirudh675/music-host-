// Party queue server for Resonus.
//
// The host app (Resonus, in the car) opens a session here. The server draws a
// QR code that points guests at /join/<id>. A guest searches and adds songs
// from that page. Neither the guest page nor this server ever sees the host's
// music-server address or credentials: searches are relayed to the host app,
// which runs them against its own library, and only (id, title, artist, album)
// travel back.
//
// Env (all optional):
//   PORT          listen port (Render sets it)
//   PUBLIC_URL    e.g. https://my-party.onrender.com  (else derived from the request)
//   HOST_KEY      if set, creating a session needs header x-host-key: <HOST_KEY>
//   MAX_SESSIONS  default 200
import express from 'express';
import compression from 'compression';
import QRCode from 'qrcode';
import { WebSocketServer } from 'ws';
import http from 'node:http';
import crypto from 'node:crypto';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const PORT = Number(process.env.PORT) || 3000;
const HOST_KEY = process.env.HOST_KEY || '';
const MAX_SESSIONS = Number(process.env.MAX_SESSIONS) || 200;
const ID_ALPHABET = 'abcdefghjkmnpqrstuvwxyz23456789'; // no look-alikes
const DEFAULT_TTL_H = 8;
const MAX_TTL_H = 24;
const SEARCH_GAP_MS = 800;
const ADD_GAP_MS = 400; // flood guard only: there is no cap on how many songs a guest adds
const MAX_LIB_SONGS = 10000;
const MAX_LIB_PLAYLISTS = 200;
const MAX_OFFERED = 300; // song ids remembered per guest

/** @type {Map<string, Session>} */
const sessions = new Map();

const rid = (n) => Array.from({ length: n }, () => ID_ALPHABET[crypto.randomInt(ID_ALPHABET.length)]).join('');
const clip = (v, n) => (typeof v === 'string' ? v.slice(0, n) : '');
const safeEqual = (a, b) => {
  const x = Buffer.from(String(a));
  const y = Buffer.from(String(b));
  return x.length === y.length && crypto.timingSafeEqual(x, y);
};

function newSession({ maxPerGuest, ttlHours }) {
  let id;
  do id = rid(8);
  while (sessions.has(id));
  const ttl = Math.min(Math.max(Number(ttlHours) || DEFAULT_TTL_H, 0.25), MAX_TTL_H);
  const s = {
    id,
    hostToken: crypto.randomBytes(24).toString('hex'),
    createdAt: Date.now(),
    expiresAt: Date.now() + ttl * 3600_000,
    // 0 = no limit. Only a number the host asks for is applied.
    maxPerGuest: Number(maxPerGuest) > 0 ? Math.min(Number(maxPerGuest), 1000) : 0,
    total: 0,
    // What the host shared: { songs, playlists, updatedAt } and the ids in it.
    library: null,
    knownIds: new Set(),
    host: null, // ws
    guests: new Map(), // guestId -> { ws, name, added, offered:Set, lastSearch, lastAdd }
    nowPlaying: null,
  };
  sessions.set(id, s);
  return s;
}

function send(ws, obj) {
  if (ws && ws.readyState === 1) ws.send(JSON.stringify(obj));
}
function broadcast(s, obj) {
  for (const g of s.guests.values()) send(g.ws, obj);
}
function endSession(s, reason) {
  broadcast(s, { type: 'ended', reason });
  for (const g of s.guests.values()) g.ws?.close(4000, 'ended');
  s.host?.close(4000, 'ended');
  sessions.delete(s.id);
}

// ── HTTP ────────────────────────────────────────────────────────────────────
const app = express();
app.set('trust proxy', 1);
app.disable('x-powered-by');
app.use(compression());
const small = express.json({ limit: '4kb' });
const big = express.json({ limit: '8mb' });

const baseUrl = (req) =>
  (process.env.PUBLIC_URL || `${req.protocol}://${req.get('host')}`).replace(/\/+$/, '');

app.get('/health', (_req, res) => res.json({ ok: true, sessions: sessions.size }));

// Light per-IP limit on session creation.
const createHits = new Map();
setInterval(() => createHits.clear(), 60_000).unref();

app.post('/api/sessions', small, (req, res) => {
  if (HOST_KEY && !safeEqual(req.get('x-host-key') || '', HOST_KEY)) {
    return res.status(401).json({ error: 'bad host key' });
  }
  const hits = (createHits.get(req.ip) || 0) + 1;
  createHits.set(req.ip, hits);
  if (hits > 10) return res.status(429).json({ error: 'slow down' });
  if (sessions.size >= MAX_SESSIONS) return res.status(503).json({ error: 'server full' });
  const s = newSession(req.body || {});
  const base = baseUrl(req);
  res.json({
    id: s.id,
    hostToken: s.hostToken,
    joinUrl: `${base}/join/${s.id}`,
    qrUrl: `${base}/api/sessions/${s.id}/qr.png`,
    expiresAt: s.expiresAt,
    maxPerGuest: s.maxPerGuest,
  });
});

const str = (v, n) => (typeof v === 'string' ? v.slice(0, n) : '');

// The host app pushes what is in its library so guests can browse it.
// Display fields only; anything else the app sends is dropped here.
app.post('/api/sessions/:id/library', big, (req, res) => {
  const s = sessions.get(req.params.id);
  const token = (req.get('authorization') || '').replace(/^Bearer /, '');
  if (!s || !safeEqual(token, s.hostToken)) return res.status(404).json({ error: 'no such session' });
  const { songs, playlists } = req.body || {};
  if (!Array.isArray(songs) || !Array.isArray(playlists)) {
    return res.status(400).json({ error: 'expected { songs: [], playlists: [] }' });
  }
  const outSongs = [];
  const ids = new Set();
  for (const x of songs.slice(0, MAX_LIB_SONGS)) {
    const id = str(x?.id, 200);
    if (!id || ids.has(id)) continue;
    ids.add(id);
    outSongs.push({ id, title: str(x?.title, 120), artist: str(x?.artist, 120), album: str(x?.album, 120) });
  }
  const outLists = playlists.slice(0, MAX_LIB_PLAYLISTS).map((p) => ({
    id: str(p?.id, 200),
    name: str(p?.name, 120),
    songIds: (Array.isArray(p?.songIds) ? p.songIds : []).slice(0, 5000).map((i) => str(i, 200)).filter((i) => ids.has(i)),
  })).filter((p) => p.id && p.name);
  s.library = { songs: outSongs, playlists: outLists, updatedAt: Date.now() };
  s.knownIds = ids;
  broadcast(s, { type: 'library-updated' });
  res.json({ ok: true, songs: outSongs.length, playlists: outLists.length });
});

// Public on purpose: the guest page loads it. Session ids are unguessable.
app.get('/api/sessions/:id/library', (req, res) => {
  const s = sessions.get(req.params.id);
  if (!s) return res.status(404).json({ error: 'no such session' });
  if (!s.library) return res.status(204).end();
  res.set('Cache-Control', 'no-store').json(s.library);
});

app.delete('/api/sessions/:id', (req, res) => {
  const s = sessions.get(req.params.id);
  const token = (req.get('authorization') || '').replace(/^Bearer /, '');
  if (!s || !safeEqual(token, s.hostToken)) return res.status(404).json({ error: 'no such session' });
  endSession(s, 'host closed');
  res.json({ ok: true });
});

// The QR is public on purpose: the car's media host downloads it as artwork.
app.get('/api/sessions/:id/qr.png', async (req, res) => {
  const s = sessions.get(req.params.id);
  if (!s) return res.status(404).end();
  const png = await QRCode.toBuffer(`${baseUrl(req)}/join/${s.id}`, {
    type: 'png',
    width: 768,
    margin: 4, // quiet zone, matters for scanning off a car screen
    errorCorrectionLevel: 'M',
    color: { dark: '#000000', light: '#ffffff' },
  });
  res.set('Cache-Control', 'public, max-age=600').type('png').send(png);
});

app.get('/join/:id', (req, res) => {
  if (!sessions.has(req.params.id)) {
    return res
      .status(404)
      .type('html')
      .send('<meta name="viewport" content="width=device-width"><body style="font-family:sans-serif;background:#111;color:#eee;text-align:center;padding:3rem"><h2>This session has ended</h2><p>Ask the driver to show a new QR code.</p>');
  }
  res.sendFile(path.join(__dirname, 'public', 'guest.html'));
});

app.use(express.static(path.join(__dirname, 'public'), { index: false }));

// ── WebSocket ───────────────────────────────────────────────────────────────
const server = http.createServer(app);
const wss = new WebSocketServer({ server, path: '/ws', maxPayload: 16 * 1024 });

wss.on('connection', (ws) => {
  ws.isAlive = true;
  ws.on('pong', () => (ws.isAlive = true));
  ws.ctx = null; // { role, session, guestId }
  const helloTimer = setTimeout(() => ws.ctx || ws.close(4001, 'no hello'), 10_000);

  ws.on('message', (raw) => {
    let m;
    try {
      m = JSON.parse(raw.toString());
    } catch {
      return;
    }
    if (!m || typeof m.type !== 'string') return;
    if (!ws.ctx) return hello(ws, m);
    ws.ctx.role === 'host' ? onHost(ws, m) : onGuest(ws, m);
  });

  ws.on('close', () => {
    clearTimeout(helloTimer);
    const c = ws.ctx;
    if (!c) return;
    const s = c.session;
    if (c.role === 'host') {
      if (s.host === ws) {
        s.host = null;
        broadcast(s, { type: 'host', online: false });
      }
    } else {
      const g = s.guests.get(c.guestId);
      if (g && g.ws === ws) g.ws = null; // keep counts so a reconnect cannot reset them
    }
  });
});

function hello(ws, m) {
  const s = sessions.get(clip(m.id, 16));
  if (!s || Date.now() > s.expiresAt) {
    send(ws, { type: 'error', error: 'unknown-session' });
    return ws.close(4004, 'unknown session');
  }
  if (m.type === 'host') {
    if (!safeEqual(clip(m.token, 80), s.hostToken)) return ws.close(4003, 'bad token');
    s.host?.close(4002, 'replaced');
    s.host = ws;
    ws.ctx = { role: 'host', session: s };
    send(ws, { type: 'hello', id: s.id, maxPerGuest: s.maxPerGuest, expiresAt: s.expiresAt });
    broadcast(s, { type: 'host', online: true });
    return;
  }
  if (m.type === 'guest') {
    let guestId = clip(m.guestId, 32);
    if (!/^[a-z0-9]{8,32}$/.test(guestId)) guestId = rid(12);
    let g = s.guests.get(guestId);
    if (!g) {
      if (s.guests.size >= 200) return ws.close(4005, 'full');
      g = { ws: null, name: '', added: 0, offered: new Set(), lastSearch: 0, lastAdd: 0 };
      s.guests.set(guestId, g);
    }
    g.ws?.close(4002, 'replaced');
    g.ws = ws;
    g.name = clip(m.name, 20).replace(/[\r\n]/g, ' ').trim();
    ws.ctx = { role: 'guest', session: s, guestId };
    send(ws, {
      type: 'welcome',
      guestId,
      remaining: s.maxPerGuest > 0 ? Math.max(0, s.maxPerGuest - g.added) : null,
      hostOnline: !!s.host,
      nowPlaying: s.nowPlaying,
    });
    s.host && send(s.host, { type: 'guests', count: [...s.guests.values()].filter((x) => x.ws).length });
  }
}

function onGuest(ws, m) {
  const { session: s, guestId } = ws.ctx;
  const g = s.guests.get(guestId);
  const now = Date.now();
  if (!s.host) return send(ws, { type: 'error', error: 'host-offline', reqId: m.reqId });

  if (m.type === 'name') {
    g.name = clip(m.name, 20).replace(/[\r\n]/g, ' ').trim();
    return;
  }
  if (m.type === 'search') {
    const q = clip(m.q, 80).trim();
    if (q.length < 2) return;
    if (now - g.lastSearch < SEARCH_GAP_MS) return send(ws, { type: 'error', error: 'slow-down', reqId: m.reqId });
    g.lastSearch = now;
    return send(s.host, { type: 'search', guestId, reqId: clip(String(m.reqId ?? ''), 16), q });
  }
  if (m.type === 'add') {
    const songId = clip(m.songId, 200);
    const mode = m.mode === 'next' ? 'next' : 'queue';
    // Only songs the host shared or returned from a search can be added: the
    // page cannot be used to push arbitrary ids at the host's server.
    if (!s.knownIds.has(songId) && !g.offered.has(songId)) {
      return send(ws, { type: 'error', error: 'unknown-song', reqId: m.reqId });
    }
    if (s.maxPerGuest > 0 && g.added >= s.maxPerGuest) {
      return send(ws, { type: 'error', error: 'limit', reqId: m.reqId });
    }
    if (now - g.lastAdd < ADD_GAP_MS) return send(ws, { type: 'error', error: 'slow-down', reqId: m.reqId });
    g.lastAdd = now;
    g.added += 1;
    s.total += 1;
    return send(s.host, { type: 'add', guestId, guestName: g.name, songId, mode });
  }
}

function onHost(ws, m) {
  const s = ws.ctx.session;
  if (m.type === 'results') {
    const g = s.guests.get(clip(m.guestId, 32));
    if (!g) return;
    const songs = (Array.isArray(m.songs) ? m.songs : []).slice(0, 25).map((x) => ({
      id: clip(x?.id, 200),
      title: clip(x?.title, 120),
      artist: clip(x?.artist, 120),
      album: clip(x?.album, 120),
      duration: Number.isFinite(x?.duration) ? Math.round(x.duration) : 0,
    })).filter((x) => x.id && x.title);
    for (const x of songs) {
      if (g.offered.size >= MAX_OFFERED) g.offered.delete(g.offered.values().next().value);
      g.offered.add(x.id);
    }
    return send(g.ws, { type: 'results', reqId: clip(String(m.reqId ?? ''), 16), songs });
  }
  if (m.type === 'addResult') {
    const g = s.guests.get(clip(m.guestId, 32));
    if (!g) return;
    if (!m.ok) {
      g.added = Math.max(0, g.added - 1);
      s.total = Math.max(0, s.total - 1);
    }
    return send(g.ws, {
      type: 'added',
      songId: clip(m.songId, 200),
      ok: !!m.ok,
      reason: clip(m.reason, 60),
      remaining: s.maxPerGuest > 0 ? Math.max(0, s.maxPerGuest - g.added) : null,
    });
  }
  if (m.type === 'state') {
    s.nowPlaying = m.nowPlaying
      ? { title: clip(m.nowPlaying.title, 120), artist: clip(m.nowPlaying.artist, 120) }
      : null;
    return broadcast(s, { type: 'state', nowPlaying: s.nowPlaying });
  }
  if (m.type === 'close') return endSession(s, 'host closed');
}

// Heartbeat (proxies like Render's drop idle sockets) and expiry.
setInterval(() => {
  for (const ws of wss.clients) {
    if (!ws.isAlive) {
      ws.terminate();
      continue;
    }
    ws.isAlive = false;
    ws.ping();
  }
}, 25_000).unref();

setInterval(() => {
  const now = Date.now();
  for (const s of sessions.values()) if (now > s.expiresAt) endSession(s, 'expired');
}, 60_000).unref();

server.listen(PORT, () => console.log(`party server on :${PORT}`));
