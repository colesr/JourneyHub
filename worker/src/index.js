import { Hono } from 'hono';
import { setCookie, getCookie, deleteCookie } from 'hono/cookie';
import { handleDb } from './documents.js';
import { ConversationRoom } from './conversation_room.js';
import { maybeSpawnGhost } from './ghost.js';

export { ConversationRoom };

const app = new Hono();

const PBKDF2_ITERATIONS = 100_000;
const SESSION_TTL_MS = 30 * 24 * 60 * 60 * 1000;

function b64(bytes) {
  return btoa(String.fromCharCode(...bytes));
}
function b64decode(str) {
  return Uint8Array.from(atob(str), (c) => c.charCodeAt(0));
}

async function hashPassword(password, saltBytes) {
  const enc = new TextEncoder();
  const salt = saltBytes || crypto.getRandomValues(new Uint8Array(16));
  const keyMaterial = await crypto.subtle.importKey(
    'raw',
    enc.encode(password),
    { name: 'PBKDF2' },
    false,
    ['deriveBits'],
  );
  const bits = await crypto.subtle.deriveBits(
    { name: 'PBKDF2', salt, iterations: PBKDF2_ITERATIONS, hash: 'SHA-256' },
    keyMaterial,
    256,
  );
  return { hash: b64(new Uint8Array(bits)), salt: b64(salt) };
}

async function verifyPassword(password, storedHash, storedSalt) {
  const { hash } = await hashPassword(password, b64decode(storedSalt));
  return hash === storedHash;
}

function newSessionToken() {
  const bytes = crypto.getRandomValues(new Uint8Array(32));
  return b64(bytes).replace(/\+/g, '-').replace(/\//g, '_').replace(/=/g, '');
}

function sessionCookieOpts() {
  return {
    httpOnly: true,
    secure: true,
    sameSite: 'Lax',
    path: '/',
    maxAge: Math.floor(SESSION_TTL_MS / 1000),
  };
}

app.use('*', async (c, next) => {
  const token = getCookie(c, 'session');
  if (token) {
    const row = await c.env.DB.prepare(
      `SELECT s.user_id, u.email, u.username
         FROM sessions s
         JOIN users u ON u.id = s.user_id
        WHERE s.token = ? AND s.expires_at > ?`,
    )
      .bind(token, Date.now())
      .first();
    if (row) c.set('user', row);
  }
  await next();
});

app.get('/api/health', (c) => c.json({ ok: true, service: 'journeyhub', time: Date.now() }));

app.post('/api/auth/register', async (c) => {
  const body = await c.req.json().catch(() => null);
  if (!body) return c.json({ error: 'Invalid JSON' }, 400);
  const { email, password, username } = body;
  if (!email || !password || !username) {
    return c.json({ error: 'Missing email, password, or username' }, 400);
  }
  if (password.length < 8) {
    return c.json({ error: 'Password must be at least 8 characters' }, 400);
  }
  const existing = await c.env.DB.prepare(
    'SELECT id FROM users WHERE email = ? OR username = ?',
  )
    .bind(email, username)
    .first();
  if (existing) return c.json({ error: 'Email or username already taken' }, 409);

  const { hash, salt } = await hashPassword(password);
  const userId = crypto.randomUUID();
  const now = Date.now();

  await c.env.DB.prepare(
    `INSERT INTO users (id, email, username, password_hash, password_salt, created_at)
     VALUES (?, ?, ?, ?, ?, ?)`,
  )
    .bind(userId, email, username, hash, salt, now)
    .run();

  const token = newSessionToken();
  await c.env.DB.prepare(
    'INSERT INTO sessions (token, user_id, expires_at, created_at) VALUES (?, ?, ?, ?)',
  )
    .bind(token, userId, now + SESSION_TTL_MS, now)
    .run();

  setCookie(c, 'session', token, sessionCookieOpts());
  return c.json({ id: userId, email, username });
});

app.post('/api/auth/login', async (c) => {
  const body = await c.req.json().catch(() => null);
  if (!body) return c.json({ error: 'Invalid JSON' }, 400);
  const { email, password } = body;
  if (!email || !password) return c.json({ error: 'Missing email or password' }, 400);

  const user = await c.env.DB.prepare(
    'SELECT id, email, username, password_hash, password_salt FROM users WHERE email = ?',
  )
    .bind(email)
    .first();
  if (!user) return c.json({ error: 'Invalid credentials' }, 401);

  const ok = await verifyPassword(password, user.password_hash, user.password_salt);
  if (!ok) return c.json({ error: 'Invalid credentials' }, 401);

  const token = newSessionToken();
  const now = Date.now();
  await c.env.DB.prepare(
    'INSERT INTO sessions (token, user_id, expires_at, created_at) VALUES (?, ?, ?, ?)',
  )
    .bind(token, user.id, now + SESSION_TTL_MS, now)
    .run();

  setCookie(c, 'session', token, sessionCookieOpts());
  return c.json({ id: user.id, email: user.email, username: user.username });
});

app.post('/api/auth/logout', async (c) => {
  const token = getCookie(c, 'session');
  if (token) {
    await c.env.DB.prepare('DELETE FROM sessions WHERE token = ?').bind(token).run();
  }
  deleteCookie(c, 'session', { path: '/' });
  return c.json({ ok: true });
});

app.post('/api/db', handleDb);

// AI inference via Cloudflare Workers AI. Uses Llama 3.1 8B Instruct by default;
// callers can override the model per request. Auth required — AI calls cost
// Neurons from our free-tier budget, so anonymous access is closed.
const DEFAULT_AI_MODEL = '@cf/meta/llama-3.1-8b-instruct';

app.post('/api/ai', async (c) => {
  const user = c.get('user');
  if (!user) return c.json({ error: 'Unauthorized' }, 401);

  const body = await c.req.json().catch(() => null);
  if (!body || typeof body.prompt !== 'string' || !body.prompt.trim()) {
    return c.json({ error: 'Missing prompt' }, 400);
  }

  const model = typeof body.model === 'string' ? body.model : DEFAULT_AI_MODEL;
  const temperature = typeof body.temperature === 'number' ? body.temperature : 0.7;
  const maxTokens = Math.min(typeof body.maxTokens === 'number' ? body.maxTokens : 1024, 2048);

  const messages = [];
  if (typeof body.systemInstruction === 'string' && body.systemInstruction.trim()) {
    messages.push({ role: 'system', content: body.systemInstruction });
  }
  messages.push({ role: 'user', content: body.prompt });

  try {
    const result = await c.env.AI.run(model, {
      messages,
      temperature,
      max_tokens: maxTokens,
    });
    const text = typeof result?.response === 'string' ? result.response : '';
    return c.json({ text });
  } catch (e) {
    console.error('AI error:', e);
    return c.json({ error: 'AI request failed: ' + (e?.message || 'unknown') }, 500);
  }
});

app.get('/api/ws/:convId', async (c) => {
  if (c.req.header('upgrade') !== 'websocket') {
    return c.json({ error: 'Expected WebSocket' }, 426);
  }
  const user = c.get('user');
  if (!user) return c.json({ error: 'Unauthorized' }, 401);

  const convId = c.req.param('convId');
  const row = await c.env.DB.prepare('SELECT data FROM documents WHERE path = ?')
    .bind('conversations/' + convId).first();
  if (!row) return c.json({ error: 'Conversation not found' }, 404);
  const conv = JSON.parse(row.data);
  const participants = Array.isArray(conv.participants) ? conv.participants : [];
  if (!participants.includes(user.user_id)) {
    return c.json({ error: 'Forbidden' }, 403);
  }

  const id = c.env.CONVROOMS.idFromName(convId);
  const stub = c.env.CONVROOMS.get(id);
  return stub.fetch(c.req.raw);
});

const MAX_UPLOAD_BYTES = 5 * 1024 * 1024;

app.post('/api/upload', async (c) => {
  const user = c.get('user');
  if (!user) return c.json({ error: 'Unauthorized' }, 401);

  const path = c.req.query('path');
  if (!path || path.includes('..') || path.startsWith('/')) {
    return c.json({ error: 'Invalid path' }, 400);
  }

  const contentLength = parseInt(c.req.header('content-length') || '0', 10);
  if (contentLength > MAX_UPLOAD_BYTES) {
    return c.json({ error: 'File too large (5 MB max)' }, 413);
  }

  const body = await c.req.arrayBuffer();
  if (body.byteLength > MAX_UPLOAD_BYTES) {
    return c.json({ error: 'File too large (5 MB max)' }, 413);
  }

  const contentType = c.req.header('content-type') || 'application/octet-stream';
  await c.env.IMAGES.put(path, body, { httpMetadata: { contentType } });
  return c.json({ path, url: '/r2/' + path });
});

app.delete('/api/upload', async (c) => {
  const user = c.get('user');
  if (!user) return c.json({ error: 'Unauthorized' }, 401);
  const path = c.req.query('path');
  if (!path) return c.json({ error: 'Missing path' }, 400);
  await c.env.IMAGES.delete(path);
  return c.json({ ok: true });
});

app.get('/r2/*', async (c) => {
  const key = c.req.path.slice('/r2/'.length);
  if (!key) return c.notFound();
  const obj = await c.env.IMAGES.get(key);
  if (!obj) return c.text('Not found', 404);
  return new Response(obj.body, {
    headers: {
      'Content-Type': obj.httpMetadata?.contentType || 'application/octet-stream',
      'Cache-Control': 'public, max-age=31536000, immutable',
      'ETag': obj.httpEtag,
    },
  });
});

app.get('/api/auth/me', (c) => {
  const user = c.get('user');
  if (!user) return c.json({ user: null });
  c.executionCtx.waitUntil(
    maybeSpawnGhost(c.env, user.user_id).catch((e) => console.error('ghost tick failed:', e)),
  );
  return c.json({
    user: { id: user.user_id, email: user.email, username: user.username },
  });
});

app.notFound((c) => c.json({ error: 'Not found' }, 404));
app.onError((err, c) => {
  console.error(err);
  return c.json({ error: 'Internal error' }, 500);
});

export default app;
