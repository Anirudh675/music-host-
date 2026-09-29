// server.js
//
// Thin realtime relay + library snapshot store for the "party mode" feature.
// This server never talks to Navidrome/Subsonic directly. The host app is
// the only thing with real credentials; this server just:
//   1. issues short-lived join codes
//   2. relays search/request/queue messages between host <-> guests (ws)
//   3. stores a lightweight library snapshot per session so the join
//      website can show "here's what exists" without a server round trip
//      per keystroke
//
// Deploy target: Render (Node web service). Listens on process.env.PORT.

import express from 'express';
import compression from 'compression';
import { createServer } from 'http';
import { WebSocketServer } from 'ws';
import { randomBytes } from 'crypto';
import { readFile } from 'fs/promises';
import path from 'path';
import { fileURLToPath } from 'url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const publicDir = path.join(__dirname, 'public');

const PORT = process.env.PORT || 8080;
const CODE_TTL_MS = 4 * 60 * 60 * 1000; // 4 hours
const MAX_LIBRARY_BYTES = 2 * 1024 * 1024; // 2MB raw JSON guard (~3000 songs is far under this)

// ---------------------------------------------------------------------------
// In-memory session store.
//
// sessions: code -> {
//   hostWs: WebSocket | null,
//   guests: Map<guestId, WebSocket>,
//   expiresAt: number,
//   library: { songs: [...], playlists: [...] } | null,
//   libraryUpdatedAt: number | null,
// }
//
// Single-instance only. If you outgrow one Render instance, swap this Map
// for Redis (session hash + TTL) and use Redis pub/sub for the ws relay.
// ---------------------------------------------------------------------------
const sessions = new Map();

function makeCode() {
  // 6 uppercase hex chars, e.g. "7F3A21" - not cryptographically precious,
  // just needs to be hard to guess by chance and easy to read aloud.
  return randomBytes(3).toString('hex').toUpperCase();
}

function createSession() {
  let code = makeCode();
  while (sessions.has(code)) code = makeCode(); // avoid the rare collision
  const session = {
    hostWs: null,
    guests: new Map(),
    expiresAt: Date.now() + CODE_TTL_MS,
    library: null,
    libraryUpdatedAt: null,
  };
  sessions.set(code, session);
  return { code, session };
}

function getLiveSession(code) {
  const session = sessions.get(code);
  if (!session) return null;
  if (session.expiresAt < Date.now()) {
    sessions.delete(code);
    return null;
  }
  return session;
}

// Periodic sweep so expired sessions don't sit in memory forever even if
// nobody ever fetches them again.
setInterval(() => {
  const now = Date.now();
  for (const [code, session] of sessions) {
    if (session.expiresAt < now) sessions.delete(code);
  }
}, 10 * 60 * 1000).unref();

// ---------------------------------------------------------------------------
// HTTP app: library snapshot routes.
// ---------------------------------------------------------------------------
const app = express();
app.use(compression()); // gzips the library JSON response automatically
app.use(express.json({ limit: '3mb' })); // slightly above MAX_LIBRARY_BYTES

// Join page (static). Any /join/<code> path serves the same HTML shell;
// join.js reads the code out of the URL itself client-side, so there's no
// server-side templating to worry about.
app.get('/join/:code', async (req, res) => {
  try {
    const html = await readFile(path.join(publicDir, 'join.html'), 'utf8');
    res.type('html').send(html);
  } catch (e) {
    res.status(500).send('join page missing');
  }
});

app.get('/join.js', async (req, res) => {
  try {
    const js = await readFile(path.join(publicDir, 'join.js'), 'utf8');
    res.type('application/javascript').send(js);
  } catch (e) {
    res.status(404).send('not found');
  }
});

// Host app calls this once at "Start Party" (after session creation) and
// again any time it hits "Refresh Library".
app.post('/session/:code/library', (req, res) => {
  const session = getLiveSession(req.params.code);
  if (!session) return res.status(404).json({ error: 'session-not-found-or-expired' });

  const { songs, playlists } = req.body ?? {};
  if (!Array.isArray(songs) || !Array.isArray(playlists)) {
    return res.status(400).json({ error: 'expected { songs: [], playlists: [] }' });
  }

  const raw = JSON.stringify({ songs, playlists });
  if (Buffer.byteLength(raw) > MAX_LIBRARY_BYTES) {
    return res.status(413).json({ error: 'library-payload-too-large' });
  }

  // Keep only display fields - strip anything extra the app might send by
  // accident (stream URLs, tokens, etc. should never land here).
  session.library = {
    songs: songs.map((s) => ({
      id: s.id,
      title: s.title,
      artist: s.artist,
      album: s.album,
    })),
    playlists: playlists.map((p) => ({
      id: p.id,
      name: p.name,
      trackCount: p.trackCount,
      songIds: Array.isArray(p.songIds) ? p.songIds : [],
    })),
  };
  session.libraryUpdatedAt = Date.now();

  // Tell any guests already on the join page that fresh data is available.
  // They re-fetch via GET rather than us pushing the (possibly large)
  // payload down every open socket.
  broadcastToGuests(session, { type: 'library-updated', updatedAt: session.libraryUpdatedAt });

  res.json({ ok: true, songCount: session.library.songs.length, playlistCount: session.library.playlists.length });
});

// Join website calls this once on page load, and again after a
// "library-updated" event.
app.get('/session/:code/library', (req, res) => {
  const session = getLiveSession(req.params.code);
  if (!session) return res.status(404).json({ error: 'session-not-found-or-expired' });
  if (!session.library) return res.status(204).end(); // session exists, host hasn't pushed a snapshot yet
  res.json({ ...session.library, updatedAt: session.libraryUpdatedAt });
});

// Lets the join page confirm a code is valid before rendering the app shell.
app.get('/session/:code', (req, res) => {
  const session = getLiveSession(req.params.code);
  if (!session) return res.status(404).json({ error: 'session-not-found-or-expired' });
  res.json({ ok: true, hasLibrary: !!session.library });
});

const httpServer = createServer(app);

// ---------------------------------------------------------------------------
// WebSocket relay: host <-> guests (search / request / queueUpdated).
// Unchanged in shape from the earlier sketch, just wired onto the same
// http server so one Render service handles both HTTP and ws.
// ---------------------------------------------------------------------------
const wss = new WebSocketServer({ server: httpServer });

function broadcastToGuests(session, msg) {
  const json = JSON.stringify(msg);
  for (const ws of session.guests.values()) {
    if (ws.readyState === ws.OPEN) ws.send(json);
  }
}

wss.on('connection', (ws) => {
  ws.on('message', (raw) => {
    let msg;
    try {
      msg = JSON.parse(raw.toString());
    } catch {
      return; // ignore malformed frames
    }

    if (msg.type === 'host-create') {
      const { code, session } = createSession();
      session.hostWs = ws;
      ws.sessionCode = code;
      ws.role = 'host';
      ws.send(JSON.stringify({ type: 'session-created', code }));
      return;
    }

    if (msg.type === 'host-resume') {
      // Host app reconnecting to a code it already created (e.g. after a
      // brief network drop) rather than minting a brand new one.
      const session = getLiveSession(msg.code);
      if (!session) {
        ws.send(JSON.stringify({ type: 'error', reason: 'expired-or-invalid' }));
        return;
      }
      session.hostWs = ws;
      ws.sessionCode = msg.code;
      ws.role = 'host';
      ws.send(JSON.stringify({ type: 'session-resumed', code: msg.code }));
      return;
    }

    if (msg.type === 'guest-join') {
      const session = getLiveSession(msg.code);
      if (!session) {
        ws.send(JSON.stringify({ type: 'error', reason: 'expired-or-invalid' }));
        return;
      }
      const guestId = randomBytes(4).toString('hex');
      ws.guestId = guestId;
      ws.sessionCode = msg.code;
      ws.role = 'guest';
      session.guests.set(guestId, ws);
      ws.send(JSON.stringify({ type: 'joined', guestId }));
      return;
    }

    // Guest -> host
    if (ws.role === 'guest' && (msg.type === 'search' || msg.type === 'request')) {
      const session = getLiveSession(ws.sessionCode);
      if (!session?.hostWs || session.hostWs.readyState !== session.hostWs.OPEN) return;
      session.hostWs.send(JSON.stringify({ ...msg, from: ws.guestId }));
      return;
    }

    // Host -> guest(s)
    if (ws.role === 'host' && (msg.type === 'searchResults' || msg.type === 'queueUpdated')) {
      const session = getLiveSession(ws.sessionCode);
      if (!session) return;
      if (msg.type === 'searchResults' && msg.to) {
        const guest = session.guests.get(msg.to);
        if (guest?.readyState === guest.OPEN) guest.send(JSON.stringify(msg));
      } else {
        broadcastToGuests(session, msg);
      }
      return;
    }
  });

  ws.on('close', () => {
    if (!ws.sessionCode) return;
    const session = sessions.get(ws.sessionCode);
    if (!session) return;
    if (ws.role === 'host' && session.hostWs === ws) {
      session.hostWs = null; // don't delete the session outright; host may reconnect within TTL
    } else if (ws.role === 'guest' && ws.guestId) {
      session.guests.delete(ws.guestId);
    }
  });
});

httpServer.listen(PORT, () => {
  console.log(`Party backend listening on :${PORT}`);
});
