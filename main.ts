// main.ts — Deno Deploy entry point.
//
// Same job as the Node version: issue join codes, relay search/request/queue
// messages between host <-> guests, and store a library snapshot per
// session for the join website to read.
//
// Why this differs from the Node/Render version:
// Deno Deploy can run your code in MULTIPLE isolates at once (different
// requests/sockets may land on different instances, even in different
// regions). A plain in-memory Map, like the Node version used, would only
// be visible to the isolate that created it — a host on isolate A and a
// guest on isolate B would never see each other. So:
//   - Deno.openKv() replaces the in-memory session map: any isolate can
//     read/write it, and it supports per-key expiry (expireIn) so we get
//     the same 4-hour TTL behaviour "for free".
//   - BroadcastChannel replaces the direct ws.send() relay: it's a
//     web-standard API that Deno Deploy synchronizes across isolates and
//     regions, so a message published on isolate A is delivered on every
//     isolate that has a matching subscriber, including isolate B.
//
// Local sockets (the actual WebSocket objects) still live in a per-isolate
// Map, because a socket object itself can't be shared across isolates —
// only messages can, via BroadcastChannel. Each isolate forwards a
// broadcast to whichever local sockets it happens to be holding for that
// session.

const kv = await Deno.openKv();

const CODE_TTL_MS = 4 * 60 * 60 * 1000; // 4 hours
const MAX_LIBRARY_BYTES = 2 * 1024 * 1024; // 2MB guard; ~3000 songs is far under this

// ---------------------------------------------------------------------------
// KV helpers
// ---------------------------------------------------------------------------

type SessionMeta = { createdAt: number };
type LibrarySnapshot = {
  songs: { id: string; title: string; artist: string; album: string }[];
  playlists: { id: string; name: string; trackCount: number; songIds: string[] }[];
  updatedAt: number;
};

function makeCode(): string {
  const bytes = new Uint8Array(3);
  crypto.getRandomValues(bytes);
  return Array.from(bytes, (b) => b.toString(16).padStart(2, '0')).join('').toUpperCase();
}

async function createSession(): Promise<string> {
  for (let attempt = 0; attempt < 5; attempt++) {
    const code = makeCode();
    const key = ['session', code];
    const existing = await kv.get<SessionMeta>(key);
    if (existing.value) continue; // rare collision, retry
    await kv.set(key, { createdAt: Date.now() } satisfies SessionMeta, {
      expireIn: CODE_TTL_MS,
    });
    return code;
  }
  throw new Error('Could not allocate a session code');
}

async function sessionExists(code: string): Promise<boolean> {
  const res = await kv.get<SessionMeta>(['session', code]);
  return res.value !== null;
}

async function getLibrary(code: string): Promise<LibrarySnapshot | null> {
  const res = await kv.get<LibrarySnapshot>(['library', code]);
  return res.value;
}

async function setLibrary(code: string, snapshot: LibrarySnapshot): Promise<void> {
  // Keep the library alive as long as the session itself.
  await kv.set(['library', code], snapshot, { expireIn: CODE_TTL_MS });
}

// ---------------------------------------------------------------------------
// Per-isolate local socket registry + BroadcastChannel relay.
//
// One BroadcastChannel per active session code on this isolate, shared by
// every local socket (host or guest) currently connected for that code.
// ---------------------------------------------------------------------------

type LocalSocket = { ws: WebSocket; role: 'host' | 'guest'; guestId?: string };

const localSocketsByCode = new Map<string, Set<LocalSocket>>();
const channelsByCode = new Map<string, BroadcastChannel>();

function getChannel(code: string): BroadcastChannel {
  let channel = channelsByCode.get(code);
  if (channel) return channel;

  channel = new BroadcastChannel(`party-${code}`);
  channel.onmessage = (event) => {
    const msg = event.data as Record<string, unknown>;
    const sockets = localSocketsByCode.get(code);
    if (!sockets) return;

    for (const local of sockets) {
      if (local.ws.readyState !== WebSocket.OPEN) continue;

      // Guest -> host messages are meant for whichever isolate holds the host socket.
      if ((msg.type === 'search' || msg.type === 'request') && local.role === 'host') {
        local.ws.send(JSON.stringify(msg));
        continue;
      }

      // Host -> guest(s): either a targeted reply (searchResults with `to`)
      // or a broadcast (queueUpdated, library-updated).
      if (local.role === 'guest') {
        if (msg.type === 'searchResults') {
          if (msg.to === local.guestId) local.ws.send(JSON.stringify(msg));
        } else if (msg.type === 'queueUpdated' || msg.type === 'library-updated') {
          local.ws.send(JSON.stringify(msg));
        }
      }
    }
  };
  channelsByCode.set(code, channel);
  return channel;
}

function registerLocalSocket(code: string, entry: LocalSocket) {
  getChannel(code); // ensure subscribed
  let set = localSocketsByCode.get(code);
  if (!set) {
    set = new Set();
    localSocketsByCode.set(code, set);
  }
  set.add(entry);
}

function unregisterLocalSocket(code: string, entry: LocalSocket) {
  const set = localSocketsByCode.get(code);
  set?.delete(entry);
  if (set && set.size === 0) {
    localSocketsByCode.delete(code);
    channelsByCode.get(code)?.close();
    channelsByCode.delete(code);
  }
}

// ---------------------------------------------------------------------------
// HTTP + WebSocket handler
// ---------------------------------------------------------------------------

async function gzipJson(body: unknown): Promise<Response> {
  const json = JSON.stringify(body);
  const stream = new Blob([json]).stream().pipeThrough(new CompressionStream('gzip'));
  return new Response(stream, {
    headers: {
      'content-type': 'application/json',
      'content-encoding': 'gzip',
    },
  });
}

Deno.serve(async (req) => {
  const url = new URL(req.url);

  // --- WebSocket upgrade ---------------------------------------------------
  if (url.pathname === '/ws') {
    if (req.headers.get('upgrade') !== 'websocket') {
      return new Response('expected websocket', { status: 400 });
    }
    const { socket, response } = Deno.upgradeWebSocket(req);

    let sessionCode: string | null = null;
    let localEntry: LocalSocket | null = null;

    socket.onmessage = async (event) => {
      let msg: Record<string, unknown>;
      try {
        msg = JSON.parse(event.data);
      } catch {
        return;
      }

      if (msg.type === 'host-create') {
        const code = await createSession();
        sessionCode = code;
        localEntry = { ws: socket, role: 'host' };
        registerLocalSocket(code, localEntry);
        socket.send(JSON.stringify({ type: 'session-created', code }));
        return;
      }

      if (msg.type === 'host-resume') {
        const code = String(msg.code);
        if (!(await sessionExists(code))) {
          socket.send(JSON.stringify({ type: 'error', reason: 'expired-or-invalid' }));
          return;
        }
        sessionCode = code;
        localEntry = { ws: socket, role: 'host' };
        registerLocalSocket(code, localEntry);
        socket.send(JSON.stringify({ type: 'session-resumed', code }));
        return;
      }

      if (msg.type === 'guest-join') {
        const code = String(msg.code);
        if (!(await sessionExists(code))) {
          socket.send(JSON.stringify({ type: 'error', reason: 'expired-or-invalid' }));
          return;
        }
        const guestId = crypto.randomUUID().slice(0, 8);
        sessionCode = code;
        localEntry = { ws: socket, role: 'guest', guestId };
        registerLocalSocket(code, localEntry);
        socket.send(JSON.stringify({ type: 'joined', guestId }));
        return;
      }

      // Anything else just gets published on the channel for this session;
      // getChannel()'s onmessage handler (running on every subscribed
      // isolate, including this one) decides who it's actually for.
      if (sessionCode && localEntry) {
        const outgoing = localEntry.role === 'guest'
          ? { ...msg, from: localEntry.guestId }
          : msg;
        getChannel(sessionCode).postMessage(outgoing);
      }
    };

    socket.onclose = () => {
      if (sessionCode && localEntry) unregisterLocalSocket(sessionCode, localEntry);
    };

    return response;
  }

  // --- Session validity check ----------------------------------------------
  const sessionMatch = url.pathname.match(/^\/session\/([A-Z0-9]+)$/);
  if (sessionMatch && req.method === 'GET') {
    const code = sessionMatch[1];
    if (!(await sessionExists(code))) {
      return Response.json({ error: 'session-not-found-or-expired' }, { status: 404 });
    }
    const library = await getLibrary(code);
    return Response.json({ ok: true, hasLibrary: !!library });
  }

  // --- Library snapshot: host pushes it here (also used for manual refresh) --
  const libraryMatch = url.pathname.match(/^\/session\/([A-Z0-9]+)\/library$/);
  if (libraryMatch && req.method === 'POST') {
    const code = libraryMatch[1];
    if (!(await sessionExists(code))) {
      return Response.json({ error: 'session-not-found-or-expired' }, { status: 404 });
    }

    const raw = await req.text();
    if (new Blob([raw]).size > MAX_LIBRARY_BYTES) {
      return Response.json({ error: 'library-payload-too-large' }, { status: 413 });
    }

    let body: { songs?: unknown; playlists?: unknown };
    try {
      body = JSON.parse(raw);
    } catch {
      return Response.json({ error: 'invalid-json' }, { status: 400 });
    }
    if (!Array.isArray(body.songs) || !Array.isArray(body.playlists)) {
      return Response.json({ error: 'expected { songs: [], playlists: [] }' }, { status: 400 });
    }

    // Strip down to display-only fields, same as the Node version — never
    // trust or forward anything extra (stream URLs, tokens, etc).
    const snapshot: LibrarySnapshot = {
      songs: (body.songs as any[]).map((s) => ({
        id: s.id, title: s.title, artist: s.artist, album: s.album,
      })),
      playlists: (body.playlists as any[]).map((p) => ({
        id: p.id, name: p.name, trackCount: p.trackCount,
        songIds: Array.isArray(p.songIds) ? p.songIds : [],
      })),
      updatedAt: Date.now(),
    };
    await setLibrary(code, snapshot);

    // Cross-isolate broadcast, same channel the ws relay uses.
    getChannel(code).postMessage({ type: 'library-updated', updatedAt: snapshot.updatedAt });

    return Response.json({
      ok: true,
      songCount: snapshot.songs.length,
      playlistCount: snapshot.playlists.length,
    });
  }

  // --- Library snapshot: join website fetches it here ------------------------
  if (libraryMatch && req.method === 'GET') {
    const code = libraryMatch[1];
    if (!(await sessionExists(code))) {
      return Response.json({ error: 'session-not-found-or-expired' }, { status: 404 });
    }
    const library = await getLibrary(code);
    if (!library) return new Response(null, { status: 204 });
    return gzipJson(library);
  }

  return new Response('not found', { status: 404 });
});
