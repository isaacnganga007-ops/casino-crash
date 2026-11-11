// Casino Four – Wi‑Fi table server. No installs needed: run `node casino-server.js`
// with casino.html in the same folder, then open the printed address on every phone
// connected to the same Wi‑Fi. Works without internet.
const http = require('http');
const crypto = require('crypto');
const fs = require('fs');
const path = require('path');
const os = require('os');

const PORT = +process.env.PORT || 8080;
const PAGE = path.join(__dirname, 'casino.html');
const rooms = new Map(); // code -> Map(id -> {id, pr, streams:Set, timer, ip, since})
const roomInfo = new Map(); // code -> {created, rounds}
const kicked = new Map(); // code+':dev:'+device / code+':ip:'+address / code+':'+id -> until (ms)
const TRUST_PROXY = process.env.TRUST_PROXY === '1'; // set only when a reverse proxy you control sets X-Forwarded-For
const TRANSLATE = process.env.TRANSLATE !== '0';      // TRANSLATE=0 keeps chat on this network (no MyMemory calls)
const ROOMS_MAX = 500;
const clientIp = req => (TRUST_PROXY && String(req.headers['x-forwarded-for'] || '').split(',')[0].trim()) || req.socket.remoteAddress || '?';
const okDev = d => /^[a-z0-9]{12,24}$/.test(d || '');
function isBanned(code, id, ip, dev) {
  const now = Date.now();
  return [code + ':' + id, code + ':ip:' + ip, dev ? code + ':dev:' + dev : ''].some(k => k && (kicked.get(k) || 0) > now);
}
// Removing someone bans this browser (device id) and, unless another player at the table shares it, their network address.
function ban(code, p, ms, others) {
  const until = Date.now() + ms;
  kicked.set(code + ':' + p.id, until);
  if (p.dev) kicked.set(code + ':dev:' + p.dev, until);
  if (p.ip && !(others || []).some(o => o !== p && o.ip === p.ip)) kicked.set(code + ':ip:' + p.ip, until);
}

/* ---------- admin: activity log ---------- */
const ADMIN_PASSWORD = process.env.ADMIN_PASSWORD || '';
const LOG_MAX = 1000;
const activity = []; // {t, kind, room, text, name?}
let seq = 0;
const stats = { started: Date.now(), tables: 0, joins: 0, rounds: 0, wins: 0, chats: 0 };
const clean = v => String(v == null ? '' : v).replace(/[\u0000-\u001f\u007f]/g, ' ').slice(0, 200);
function logEvent(kind, room, text, extra) {
  const e = { n: ++seq, t: Date.now(), kind, room, text: clean(text), ...(extra || {}) };
  activity.push(e); if (activity.length > LOG_MAX) activity.shift();
  console.log(`[${new Date(e.t).toISOString()}] ${kind.toUpperCase()} ${room} ${e.text}`);
}
const nameOf = pr => clean(pr && pr.n).slice(0, 14) || 'Someone';
function trackPresence(code, peer, oldPr, pr) {
  if (pr.j && pr.n && !(oldPr && oldPr.j && oldPr.n)) {
    stats.joins++;
    logEvent('join', code, `${nameOf(pr)} ${pr.host ? 'opened' : 'joined'} the table`);
    if (pr.host) { stats.tables++; }
  }
  const g = pr.g, og = oldPr && oldPr.g;
  if (pr.host && g && typeof g === 'object' && (!og || og.v !== g.v)) {
    const ps = Array.isArray(g.ps) ? g.ps : [];
    if (g.ev && g.ev.t === 'deal') {
      stats.rounds++; const ri = roomInfo.get(code); if (ri) ri.rounds++;
      logEvent('round', code, `New ${g.gm === 'flower' ? 'Flower' : 'AK47'} round: ${ps.map(p => clean(p.n).slice(0, 14)).join(', ')}`);
    }
    if (g.win && !(og && og.win)) {
      const w = ps[g.win.p];
      stats.wins++;
      logEvent('win', code, `${w ? clean(w.n).slice(0, 14) : 'Someone'} won (${clean(g.win.w)}) in ${Math.round((+g.win.el || 0) / 1000)}s`);
    }
  }
  const seen = (oldPr && Array.isArray(oldPr.ch)) ? Math.max(0, ...oldPr.ch.map(m => Array.isArray(m) ? +m[0] || 0 : 0)) : 0;
  if (Array.isArray(pr.ch)) for (const m of pr.ch) {
    if (!Array.isArray(m) || !(+m[0] > seen)) continue;
    stats.chats++;
    logEvent('chat', code, clean(m[1]), { name: nameOf(pr) });
  }
}

const okCode = c => /^[A-Z0-9]{4,8}$/.test(c || '');
const okId = i => /^[a-z0-9]{4,16}$/.test(i || '');

function getRoom(code) {
  if (!rooms.has(code)) rooms.set(code, new Map());
  return rooms.get(code);
}
function getPeer(code, id) {
  const r = getRoom(code);
  if (!roomInfo.has(code)) roomInfo.set(code, { created: Date.now(), rounds: 0, host: null });
  if (!r.has(id)) r.set(id, { id, pr: {}, streams: new Set(), timer: null, since: Date.now() });
  return r.get(id);
}
// Each phone gets the table with every other player's cards blanked out (-1 per card, so counts still show).
// Hands are only revealed once the round has a winner.
function hideHands(pr, viewerId) {
  const g = pr && pr.host && pr.g;
  if (!g || typeof g !== 'object' || g.win || !Array.isArray(g.ps)) return pr;
  return { ...pr, g: { ...g, ps: g.ps.map(x => (!x || typeof x !== 'object' || x.id === viewerId) ? x : { ...x, h: Array.isArray(x.h) ? x.h.map(() => -1) : [] }) } };
}
function broadcast(code) {
  const r = rooms.get(code);
  if (!r) return;
  const list = [...r.values()].map(p => ({ id: p.id, pr: p.pr }));
  for (const viewer of r.values()) {
    if (!viewer.streams.size) continue;
    const msg = `event: peers\ndata: ${JSON.stringify(list.map(p => ({ id: p.id, pr: p.id === viewer.id ? p.pr : hideHands(p.pr, viewer.id) })))}\n\n`;
    for (const s of viewer.streams) s.write(msg);
  }
}
function removePeer(code, id) {
  const r = rooms.get(code);
  if (!r || !r.has(id)) return;
  clearTimeout(r.get(id).timer);
  const gone = r.get(id);
  r.delete(id);
  const ri = roomInfo.get(code); if (ri && ri.host === id) ri.host = null;
  if (gone && gone.pr && gone.pr.n) logEvent('leave', code, `${nameOf(gone.pr)} left the table`);
  if (!r.size) { rooms.delete(code); roomInfo.delete(code); dropMusic(code); logEvent('close', code, 'Table closed (everyone left)'); } else broadcast(code);
}
function sendBye(p, reason) { const msg = `event: bye\ndata: ${JSON.stringify({ reason })}\n\n`; for (const s of p.streams) { try { s.write(msg); s.end(); } catch (e) {} } }

// Background music: the host's song, kept in memory per table, freed when the table empties.
const MUSIC_MAX = 15 * 1024 * 1024, MUSIC_TOTAL = 200 * 1024 * 1024;
const music = new Map(); // code -> {buf, type, v}
let musicBytes = 0;
function dropMusic(code) { const m = music.get(code); if (m) { musicBytes -= m.buf.length; music.delete(code); } dropLogo(code); }

// The host's table picture, kept in memory per table like the music.
const LOGO_MAX = 600 * 1024, LOGO_TOTAL = 60 * 1024 * 1024;
const logos = new Map(); // code -> {buf, type, v}
let logoBytes = 0;
function dropLogo(code) { const m = logos.get(code); if (m) { logoBytes -= m.buf.length; logos.delete(code); } }
function logoUpload(req, res, code, id) {
  const r = rooms.get(code);
  if (!r || !r.has(id)) { res.writeHead(403); return res.end(); }
  const type = String(req.headers['content-type'] || '').split(';')[0].trim();
  if (!/^image\/(png|jpeg|webp)$/.test(type)) { res.writeHead(415); return res.end(); }
  const chunks = []; let size = 0, done = false;
  req.on('data', d => { if (done) return; size += d.length; if (size > LOGO_MAX) { done = true; res.writeHead(413); res.end(); req.destroy(); return; } chunks.push(d); });
  req.on('end', () => {
    if (done) return;
    const old = logos.get(code), v = (old ? old.v : 0) + 1, oldLen = old ? old.buf.length : 0;
    if (logoBytes - oldLen + size > LOGO_TOTAL) { res.writeHead(507); return res.end(); }
    logoBytes += size - oldLen;
    logos.set(code, { buf: Buffer.concat(chunks), type, v });
    res.writeHead(200, { 'content-type': 'application/json' }); res.end(JSON.stringify({ v }));
  });
}
function logoGet(res, code) {
  const m = logos.get(code);
  if (!m) { res.writeHead(404); return res.end(); }
  res.writeHead(200, { 'content-type': m.type, 'content-length': m.buf.length, 'cache-control': 'private, max-age=86400', 'x-content-type-options': 'nosniff' });
  res.end(m.buf);
}
function musicUpload(req, res, code, id) {
  const r = rooms.get(code);
  if (!r || !r.has(id)) { res.writeHead(403); return res.end(); }
  const type = String(req.headers['content-type'] || 'audio/mpeg').split(';')[0].trim();
  if (!/^audio\/[\w.+-]{1,40}$/.test(type)) { res.writeHead(415); return res.end(); }
  if (+req.headers['content-length'] > MUSIC_MAX) { res.writeHead(413); res.end(); return req.destroy(); }
  const chunks = []; let size = 0, done = false;
  req.on('data', d => {
    if (done) return;
    size += d.length;
    if (size > MUSIC_MAX) { done = true; res.writeHead(413); res.end(); req.destroy(); return; }
    chunks.push(d);
  });
  req.on('end', () => {
    if (done) return;
    const old = music.get(code), oldLen = old ? old.buf.length : 0;
    if (musicBytes - oldLen + size > MUSIC_TOTAL) { res.writeHead(507); return res.end(); }
    musicBytes += size - oldLen;
    const v = (old ? old.v : 0) + 1;
    music.set(code, { buf: Buffer.concat(chunks), type, v });
    res.writeHead(200, { 'content-type': 'application/json' }); res.end(JSON.stringify({ v }));
  });
}
function musicStream(req, res, code) {
  const m = music.get(code);
  if (!m) { res.writeHead(404); return res.end(); }
  const total = m.buf.length, range = req.headers.range;
  const head = { 'content-type': m.type, 'accept-ranges': 'bytes', 'cache-control': 'no-store' };
  if (range) {
    const mm = /^bytes=(\d*)-(\d*)$/.exec(range.trim());
    let start = 0, end = total - 1;
    if (mm && mm[1]) { start = +mm[1]; if (mm[2]) end = Math.min(+mm[2], total - 1); }
    else if (mm && mm[2]) { start = Math.max(0, total - +mm[2]); }
    if (!mm || start >= total || start > end) { res.writeHead(416, { 'content-range': `bytes */${total}` }); return res.end(); }
    res.writeHead(206, { ...head, 'content-range': `bytes ${start}-${end}/${total}`, 'content-length': end - start + 1 });
    return res.end(m.buf.subarray(start, end + 1));
  }
  res.writeHead(200, { ...head, 'content-length': total });
  res.end(m.buf);
}

function sendPage(res) {
  fs.readFile(PAGE, 'utf8', (err, body) => {
    if (err) { res.writeHead(500, { 'content-type': 'text/plain' }); return res.end('casino.html was not found next to casino-server.js'); }
    res.writeHead(200, { 'content-type': 'text/html; charset=utf-8', 'cache-control': 'no-store' });
    const cut = body.indexOf('</style>') + '</style>'.length; // everything up to the stylesheet belongs in <head>
    const head = cut > 7 ? body.slice(0, cut) : '<meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1,viewport-fit=cover">';
    res.end('<!doctype html><html lang="en"><head>' + head + '</head><body>' + (cut > 7 ? body.slice(cut) : body) + '</body></html>');
  });
}

const LANG_RE = /^[a-z]{2,3}(-[A-Z]{2})?$/;
const trCache = new Map();
const trHits = new Map(); // ip -> [timestamps]
async function translate(req, res, u) {
  const json = (code, obj) => { res.writeHead(code, { 'content-type': 'application/json' }); res.end(JSON.stringify(obj)); };
  if (!TRANSLATE) return json(501, { error: 'translation is turned off on this server' });
  const room = rooms.get(String(u.searchParams.get('room') || '').toUpperCase());
  if (!room || !room.has(u.searchParams.get('id') || '')) return json(403, { error: 'join a table first' });
  const q = String(u.searchParams.get('q') || '').slice(0, 500);
  const from = u.searchParams.get('from') || 'en';
  const to = u.searchParams.get('to') || 'en';
  if (!q || !LANG_RE.test(from) || !LANG_RE.test(to)) return json(400, { error: 'bad request' });
  if (from === to) return json(200, { text: q });
  const key = from + '|' + to + '|' + q;
  if (trCache.has(key)) return json(200, { text: trCache.get(key) });
  const ip = req.socket.remoteAddress || '?';
  const now = Date.now();
  const hits = (trHits.get(ip) || []).filter(t => now - t < 60000);
  if (hits.length >= 30) return json(429, { error: 'slow down' });
  hits.push(now); trHits.set(ip, hits);
  if (typeof fetch !== 'function') return json(501, { error: 'Node 18 or newer is needed for translation' });
  try {
    const r = await fetch('https://api.mymemory.translated.net/get?' + new URLSearchParams({ q, langpair: from + '|' + to }));
    const j = await r.json();
    const text = j && j.responseData && j.responseData.translatedText;
    if (!r.ok || !text || +j.responseStatus >= 400) return json(502, { error: 'translation failed' });
    if (trCache.size > 2000) trCache.clear();
    trCache.set(key, text);
    json(200, { text });
  } catch (e) {
    json(502, { error: 'translation service unreachable' });
  }
}

/* ---------- admin: login and API ---------- */
const ADMIN_PAGE = path.join(__dirname, 'admin.html');
const sessions = new Map(); // token -> expires
const loginTries = new Map(); // ip -> [timestamps]
const SESSION_MS = 12 * 3600 * 1000;
const sha = s => crypto.createHash('sha256').update(String(s)).digest();
function getCookie(req, name) { const m = new RegExp('(?:^|;\\s*)' + name + '=([^;]+)').exec(req.headers.cookie || ''); return m ? m[1] : ''; }
function isAdmin(req) { const t = getCookie(req, 'casino_admin'); const exp = t && sessions.get(t); if (!exp) return false; if (exp < Date.now()) { sessions.delete(t); return false; } return true; }
function sendAdmin(res) {
  fs.readFile(ADMIN_PAGE, 'utf8', (err, body) => {
    if (err) { res.writeHead(500, { 'content-type': 'text/plain' }); return res.end('admin.html was not found next to casino-server.js'); }
    res.writeHead(200, { 'content-type': 'text/html; charset=utf-8', 'cache-control': 'no-store', 'x-frame-options': 'DENY', 'referrer-policy': 'no-referrer' });
    res.end(body);
  });
}
function readJson(req, cb) { let b = ''; req.on('data', d => { b += d; if (b.length > 4096) req.destroy(); }); req.on('end', () => { let j = null; try { j = JSON.parse(b || '{}'); } catch (e) {} cb(j && typeof j === 'object' ? j : {}); }); }
function adminApi(req, res, u) {
  const json = (code, obj, head) => { res.writeHead(code, { 'content-type': 'application/json', 'cache-control': 'no-store', ...(head || {}) }); res.end(JSON.stringify(obj)); };
  const action = u.pathname.slice('/admin/api/'.length);
  if (!ADMIN_PASSWORD) return json(503, { error: 'not_configured' });
  if (action === 'login' && req.method === 'POST') {
    const ip = clientIp(req), now = Date.now();
    const tries = (loginTries.get(ip) || []).filter(t => now - t < 10 * 60000);
    if (tries.length >= 8) return json(429, { error: 'too_many' });
    return readJson(req, b => {
      const ok = crypto.timingSafeEqual(sha(b.password || ''), sha(ADMIN_PASSWORD));
      if (!ok) { tries.push(now); loginTries.set(ip, tries); logEvent('admin', '-', `Failed admin sign-in from ${ip}`); return json(401, { error: 'wrong_password' }); }
      loginTries.delete(ip);
      const token = crypto.randomBytes(24).toString('hex');
      sessions.set(token, now + SESSION_MS);
      const secure = TRUST_PROXY && String(req.headers['x-forwarded-proto'] || '').includes('https') ? '; Secure' : '';
      logEvent('admin', '-', `Admin signed in from ${ip}`);
      json(200, { ok: true }, { 'set-cookie': `casino_admin=${token}; HttpOnly; SameSite=Strict; Path=/admin; Max-Age=${SESSION_MS / 1000}${secure}` });
    });
  }
  if (!isAdmin(req)) return json(401, { error: 'signed_out' });
  if (req.method === 'POST' && (req.headers['content-type'] || '').indexOf('application/json') !== 0) return json(415, { error: 'json_only' });
  if (action === 'logout' && req.method === 'POST') { sessions.delete(getCookie(req, 'casino_admin')); return json(200, { ok: true }, { 'set-cookie': 'casino_admin=; HttpOnly; SameSite=Strict; Path=/admin; Max-Age=0' }); }
  if (action === 'state' && req.method === 'GET') {
    const tables = [...rooms.entries()].map(([code, r]) => {
      const ps = [...r.values()];
      const host = ps.find(p => p.pr && p.pr.host);
      const g = host && host.pr.g && typeof host.pr.g === 'object' ? host.pr.g : null;
      const ri = roomInfo.get(code) || {};
      return { code, created: ri.created || null, rounds: ri.rounds || 0,
        game: g ? (g.gm === 'flower' ? 'Flower' : 'AK47') : (host && host.pr.gm === 'flower' ? 'Flower' : 'AK47'),
        status: !g ? 'In the lobby' : g.win ? 'Round finished' : 'Playing',
        music: music.has(code),
        players: ps.filter(p => p.pr && p.pr.n).map(p => ({ id: p.id, name: nameOf(p.pr), host: !!p.pr.host, online: p.streams.size > 0, since: p.since,
          cards: g && Array.isArray(g.ps) ? ((g.ps.find(x => x.id === p.id) || {}).h || []).length : null })) };
    });
    const since = +u.searchParams.get('since') || 0;
    return json(200, { now: Date.now(), stats, tables, log: activity.filter(e => e.n > since).slice(-300) });
  }
  if ((action === 'kick' || action === 'close') && req.method === 'POST') {
    return readJson(req, b => {
      const code = String(b.room || '').toUpperCase(), r = rooms.get(code);
      if (!okCode(code) || !r) return json(404, { error: 'no_table' });
      if (action === 'kick') {
        const p = r.get(String(b.id || ''));
        if (!p) return json(404, { error: 'no_player' });
        ban(code, p, 6 * 3600 * 1000, [...r.values()]);
        logEvent('admin', code, `Admin removed ${nameOf(p.pr)}`);
        sendBye(p, 'removed'); removePeer(code, p.id);
        return json(200, { ok: true });
      }
      logEvent('admin', code, 'Admin closed the table');
      for (const p of [...r.values()]) { ban(code, p, 3600 * 1000, []); sendBye(p, 'closed'); clearTimeout(p.timer); }
      rooms.delete(code); roomInfo.delete(code); dropMusic(code);
      return json(200, { ok: true });
    });
  }
  json(404, { error: 'unknown' });
}

const server = http.createServer((req, res) => {
  const u = new URL(req.url, 'http://x');
  const code = (u.searchParams.get('room') || '').toUpperCase();
  const id = u.searchParams.get('id') || '';

  if (u.pathname === '/' || u.pathname === '/index.html') return sendPage(res);

  if (u.pathname === '/api/ping') {
    res.writeHead(200, { 'content-type': 'application/json' });
    return res.end(JSON.stringify({ casino: true, tr: TRANSLATE && typeof fetch === 'function', mu: true, admin: !!ADMIN_PASSWORD }));
  }

  // Chat translation through the free MyMemory service (needs internet).
  if (u.pathname === '/api/translate') return translate(req, res, u);

  if (u.pathname === '/api/logo' && req.method === 'POST' && okCode(code) && okId(id)) return logoUpload(req, res, code, id);
  if (u.pathname === '/api/logo' && req.method === 'GET' && okCode(code)) return logoGet(res, code);
  if (u.pathname === '/api/music' && req.method === 'POST' && okCode(code) && okId(id)) return musicUpload(req, res, code, id);
  if (u.pathname === '/api/music' && (req.method === 'GET' || req.method === 'HEAD') && okCode(code)) return musicStream(req, res, code);

  // Open tables for the "Find a table" list (hosts can hide theirs).
  if (u.pathname === '/api/tables' && req.method === 'GET') {
    const tables = [];
    for (const [c, r] of rooms) {
      const ps = [...r.values()].filter(p => p.pr && p.pr.j && p.pr.n);
      const host = ps.find(p => p.pr.host);
      if (!host || host.pr.ls === 0) continue;
      const g = host.pr.g && typeof host.pr.g === 'object' ? host.pr.g : null;
      tables.push({ c, h: nameOf(host.pr), g: (g ? g.gm : host.pr.gm) === 'flower' ? 'flower' : 'ak47', n: Math.min(ps.length, 6), s: !g ? 'lobby' : g.win ? 'done' : 'play', p: ps.slice(0, 8).map(p => nameOf(p.pr)) });
      if (tables.length >= 100) break;
    }
    res.writeHead(200, { 'content-type': 'application/json', 'cache-control': 'no-store' });
    return res.end(JSON.stringify({ tables }));
  }

  if (u.pathname === '/admin' || u.pathname === '/admin/') return sendAdmin(res);
  if (u.pathname.startsWith('/admin/api/')) return adminApi(req, res, u);

  if (u.pathname === '/api/events' && okCode(code) && okId(id)) {
    const ip = clientIp(req), dev = okDev(u.searchParams.get('dev')) ? u.searchParams.get('dev') : '';
    if (isBanned(code, id, ip, dev)) { res.writeHead(200, { 'content-type': 'text/event-stream', 'cache-control': 'no-store' }); return res.end(`retry: 600000\nevent: bye\ndata: ${JSON.stringify({ reason: 'removed' })}\n\n`); }
    if (!rooms.has(code) && rooms.size >= ROOMS_MAX) { res.writeHead(503); return res.end(); }
    res.writeHead(200, { 'content-type': 'text/event-stream', 'cache-control': 'no-store', connection: 'keep-alive', 'x-accel-buffering': 'no' });
    res.write('retry: 1500\n\n');
    const p = getPeer(code, id);
    p.ip = ip; if (dev) p.dev = dev;
    clearTimeout(p.timer);
    p.streams.add(res);
    broadcast(code);
    const ka = setInterval(() => res.write(': ka\n\n'), 20000);
    req.on('close', () => {
      clearInterval(ka);
      p.streams.delete(res);
      if (!p.streams.size) p.timer = setTimeout(() => removePeer(code, id), 8000); // grace for reconnects
    });
    return;
  }

  if (u.pathname === '/api/presence' && req.method === 'POST' && okCode(code) && okId(id)) {
    let body = '';
    req.on('data', d => { body += d; if (body.length > 32768) req.destroy(); });
    req.on('end', () => {
      let pr;
      try { pr = JSON.parse(body); } catch (e) { res.writeHead(400); return res.end(); }
      if (!pr || typeof pr !== 'object' || Array.isArray(pr)) { res.writeHead(400); return res.end(); }
      const dev = okDev(u.searchParams.get('dev')) ? u.searchParams.get('dev') : '';
      if (isBanned(code, id, clientIp(req), dev)) { res.writeHead(403); return res.end(); }
      if (!rooms.has(code) || !rooms.get(code).has(id)) { res.writeHead(409); return res.end(); }
      const peer = getPeer(code, id), oldPr = peer.pr, ri = roomInfo.get(code);
      // One host per table: the first player to claim it keeps it until they leave. Anyone else's claim,
      // table state and listing flag are dropped so other phones never follow a fake host.
      if (pr.host) { if (!ri.host || ri.host === id) ri.host = id; else { pr.host = null; pr.g = null; pr.ls = null; } }
      else if (ri.host === id) ri.host = null;
      peer.pr = pr;
      try { trackPresence(code, peer, oldPr, pr); } catch (e) { console.error('log error', e); }
      broadcast(code);
      res.writeHead(204); res.end();
    });
    return;
  }

  if (u.pathname === '/api/leave' && req.method === 'POST' && okCode(code) && okId(id)) {
    removePeer(code, id);
    res.writeHead(204); return res.end();
  }

  res.writeHead(404, { 'content-type': 'text/plain' });
  res.end('Not found');
});

// Drop expired bans, sign-ins and rate-limit records so these maps can't grow forever.
setInterval(() => {
  const now = Date.now();
  for (const [k, until] of kicked) if (until <= now) kicked.delete(k);
  for (const [k, exp] of sessions) if (exp <= now) sessions.delete(k);
  for (const [ip, ts] of trHits) { const keep = ts.filter(t => now - t < 60000); if (keep.length) trHits.set(ip, keep); else trHits.delete(ip); }
  for (const [ip, ts] of loginTries) { const keep = ts.filter(t => now - t < 10 * 60000); if (keep.length) loginTries.set(ip, keep); else loginTries.delete(ip); }
}, 5 * 60000).unref();

server.listen(PORT, '0.0.0.0', () => {
  const addrs = Object.values(os.networkInterfaces()).flat()
    .filter(a => a && a.family === 'IPv4' && !a.internal).map(a => a.address);
  console.log('\n  Casino Four table server is running.\n');
  console.log('  Open one of these on every phone on the same Wi‑Fi:');
  (addrs.length ? addrs : ['localhost']).forEach(a => console.log(`    http://${a}:${PORT}`));
  console.log(ADMIN_PASSWORD ? `\n  Admin page: /admin (password from ADMIN_PASSWORD)` : `\n  Admin page is off. Set ADMIN_PASSWORD to turn it on.`);
  console.log('\n  Press Ctrl+C to close the table.\n');
});
