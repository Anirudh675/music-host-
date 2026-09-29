// public/join.js
//
// Vanilla JS on purpose — no build step, no framework, so this can be
// served as a plain static file from the same Deno Deploy app as the API.
//
// Handles:
//  - fetching the library snapshot (GET /session/:code/library)
//  - a fixed-row-height virtual list so 3000 rows scroll smoothly
//  - Playlists / All Songs tabs + drill into a single playlist
//  - client-side text filter (no server round trip)
//  - websocket connection for sending "request" (add song) messages and
//    receiving "library-updated" pushes

const CODE = window.__PARTY_CODE__;
const ROW_HEIGHT = 56;

const state = {
  library: null,        // { songs, playlists, updatedAt }
  view: 'playlists',    // 'playlists' | 'songs' | 'playlistDetail'
  activePlaylist: null, // playlist object when view === 'playlistDetail'
  query: '',
  ws: null,
  guestId: null,
  addedIds: new Set(),  // songs already requested this session (for the button state)
};

const el = {
  container: document.getElementById('list-container'),
  search: document.getElementById('search'),
  tabs: document.querySelectorAll('.tab'),
  banner: document.getElementById('banner'),
  backBtn: document.getElementById('back-btn'),
  title: document.getElementById('party-title'),
};

function wsUrl() {
  const proto = location.protocol === 'https:' ? 'wss:' : 'ws:';
  return `${proto}//${location.host}/ws`;
}

function connectSocket() {
  const ws = new WebSocket(wsUrl());
  state.ws = ws;

  ws.onopen = () => ws.send(JSON.stringify({ type: 'guest-join', code: CODE }));

  ws.onmessage = (event) => {
    const msg = JSON.parse(event.data);
    if (msg.type === 'joined') {
      state.guestId = msg.guestId;
    } else if (msg.type === 'error') {
      el.container.innerHTML = '<div id="status">This party link has expired.</div>';
    } else if (msg.type === 'library-updated') {
      el.banner.style.display = 'block';
    }
  };

  ws.onclose = () => {
    // Simple reconnect with backoff; a dropped wifi shouldn't strand a guest.
    setTimeout(connectSocket, 2000);
  };
}

async function fetchLibrary() {
  const res = await fetch(`/session/${CODE}/library`);
  if (res.status === 204) {
    el.container.innerHTML = '<div id="status">The host hasn\u2019t shared their library yet.</div>';
    return;
  }
  if (!res.ok) {
    el.container.innerHTML = '<div id="status">Couldn\u2019t load the library.</div>';
    return;
  }
  state.library = await res.json();
  render();
}

el.banner.addEventListener('click', () => {
  el.banner.style.display = 'none';
  fetchLibrary();
});

el.backBtn.addEventListener('click', () => {
  state.view = 'playlists';
  state.activePlaylist = null;
  state.query = '';
  el.search.value = '';
  render();
});

el.tabs.forEach((tab) => {
  tab.addEventListener('click', () => {
    el.tabs.forEach((t) => t.classList.remove('active'));
    tab.classList.add('active');
    state.view = tab.dataset.view;
    state.activePlaylist = null;
    state.query = '';
    el.search.value = '';
    render();
  });
});

let searchDebounce;
el.search.addEventListener('input', () => {
  clearTimeout(searchDebounce);
  searchDebounce = setTimeout(() => {
    state.query = el.search.value.trim().toLowerCase();
    render();
  }, 80);
});

function currentSongs() {
  if (!state.library) return [];
  let songs;
  if (state.view === 'playlistDetail' && state.activePlaylist) {
    const idSet = new Set(state.activePlaylist.songIds);
    songs = state.library.songs.filter((s) => idSet.has(s.id));
  } else {
    songs = state.library.songs;
  }
  if (!state.query) return songs;
  return songs.filter((s) =>
    s.title?.toLowerCase().includes(state.query) ||
    s.artist?.toLowerCase().includes(state.query) ||
    s.album?.toLowerCase().includes(state.query)
  );
}

function currentPlaylists() {
  if (!state.library) return [];
  if (!state.query) return state.library.playlists;
  return state.library.playlists.filter((p) => p.name?.toLowerCase().includes(state.query));
}

function requestSong(song) {
  if (!state.ws || state.ws.readyState !== WebSocket.OPEN) return;
  state.ws.send(JSON.stringify({
    type: 'request',
    trackId: song.id,
    title: song.title,
    artist: song.artist,
  }));
  state.addedIds.add(song.id);
}

// --- Virtualized list ------------------------------------------------------
// Renders only the rows currently in (or just outside) the scroll viewport.
// Works the same way for songs and playlists; `rowRenderer` decides content.

function mountVirtualList(items, rowRenderer) {
  el.container.innerHTML = '';
  const spacer = document.createElement('div');
  spacer.style.height = `${items.length * ROW_HEIGHT}px`;
  spacer.style.position = 'relative';
  el.container.appendChild(spacer);

  const rowsHost = document.createElement('div');
  spacer.appendChild(rowsHost);

  function draw() {
    const scrollTop = el.container.scrollTop;
    const viewportHeight = el.container.clientHeight;
    const overscan = 6;
    const startIdx = Math.max(0, Math.floor(scrollTop / ROW_HEIGHT) - overscan);
    const endIdx = Math.min(
      items.length,
      Math.ceil((scrollTop + viewportHeight) / ROW_HEIGHT) + overscan
    );

    rowsHost.innerHTML = '';
    for (let i = startIdx; i < endIdx; i++) {
      const rowEl = rowRenderer(items[i], i);
      rowEl.style.top = `${i * ROW_HEIGHT}px`;
      rowEl.style.height = `${ROW_HEIGHT}px`;
      rowsHost.appendChild(rowEl);
    }
  }

  el.container.onscroll = draw;
  draw();

  if (items.length === 0) {
    const empty = document.createElement('div');
    empty.id = 'status';
    empty.textContent = state.query ? 'No matches.' : 'Nothing here yet.';
    el.container.appendChild(empty);
  }
}

function songRow(song) {
  const row = document.createElement('div');
  row.className = 'row';
  const added = state.addedIds.has(song.id);
  row.innerHTML = `
    <div class="info">
      <div class="title">${escapeHtml(song.title || 'Untitled')}</div>
      <div class="subtitle">${escapeHtml(song.artist || '')}${song.album ? ' — ' + escapeHtml(song.album) : ''}</div>
    </div>
    <button class="${added ? 'added' : ''}">${added ? 'Added' : 'Add'}</button>
  `;
  row.querySelector('button').addEventListener('click', (e) => {
    e.stopPropagation();
    if (state.addedIds.has(song.id)) return;
    requestSong(song);
    row.querySelector('button').textContent = 'Added';
    row.querySelector('button').classList.add('added');
  });
  return row;
}

function playlistRow(playlist) {
  const row = document.createElement('div');
  row.className = 'row playlist-row';
  row.innerHTML = `
    <div class="info">
      <div class="title">${escapeHtml(playlist.name || 'Untitled playlist')}</div>
      <div class="subtitle">${playlist.trackCount ?? playlist.songIds?.length ?? 0} songs</div>
    </div>
  `;
  row.addEventListener('click', () => {
    state.view = 'playlistDetail';
    state.activePlaylist = playlist;
    state.query = '';
    el.search.value = '';
    render();
  });
  return row;
}

function escapeHtml(str) {
  return String(str).replace(/[&<>"']/g, (c) => ({
    '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;',
  }[c]));
}

function render() {
  el.backBtn.style.display = state.view === 'playlistDetail' ? 'block' : 'none';
  el.title.textContent = state.view === 'playlistDetail'
    ? state.activePlaylist?.name || 'Playlist'
    : 'Musly Party';

  if (state.view === 'playlists') {
    mountVirtualList(currentPlaylists(), playlistRow);
  } else {
    mountVirtualList(currentSongs(), songRow);
  }
}

connectSocket();
fetchLibrary();
