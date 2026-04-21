// Generic JSON document store backed by D1.
// Each Firestore-style path (e.g. "threads/abc" or "conversations/xyz/messages/m1")
// is one row in the `documents` table. Constraint evaluation (where/orderBy/limit)
// runs in-memory on the Worker after loading the collection.

const AUTO_ID_CHARS = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789';

function autoId() {
  const buf = crypto.getRandomValues(new Uint8Array(20));
  let s = '';
  for (let i = 0; i < 20; i++) s += AUTO_ID_CHARS[buf[i] % AUTO_ID_CHARS.length];
  return s;
}

function getFieldValue(data, field) {
  if (!data) return undefined;
  return field.split('.').reduce((acc, k) => (acc == null ? undefined : acc[k]), data);
}

function deepEqual(a, b) {
  if (a === b) return true;
  if (a == null || b == null) return false;
  if (typeof a !== typeof b) return false;
  if (typeof a !== 'object') return false;
  if (Array.isArray(a) !== Array.isArray(b)) return false;
  if (Array.isArray(a)) {
    if (a.length !== b.length) return false;
    for (let i = 0; i < a.length; i++) if (!deepEqual(a[i], b[i])) return false;
    return true;
  }
  const ka = Object.keys(a);
  const kb = Object.keys(b);
  if (ka.length !== kb.length) return false;
  for (const k of ka) if (!deepEqual(a[k], b[k])) return false;
  return true;
}

function compareValues(a, b) {
  // Hydrate dehydrated timestamps: {__t: 'ts', m: millis}
  if (a && typeof a === 'object' && a.__t === 'ts') a = a.m;
  if (b && typeof b === 'object' && b.__t === 'ts') b = b.m;
  if (a == null && b == null) return 0;
  if (a == null) return -1;
  if (b == null) return 1;
  if (typeof a === 'number' && typeof b === 'number') return a - b;
  if (typeof a === 'string' && typeof b === 'string') return a < b ? -1 : a > b ? 1 : 0;
  const sa = JSON.stringify(a);
  const sb = JSON.stringify(b);
  return sa < sb ? -1 : sa > sb ? 1 : 0;
}

function evalWhere(data, c) {
  const v = getFieldValue(data, c.field);
  const target = c.value;
  switch (c.op) {
    case '==': return deepEqual(v, target);
    case '!=': return !deepEqual(v, target);
    case '>': return compareValues(v, target) > 0;
    case '>=': return compareValues(v, target) >= 0;
    case '<': return compareValues(v, target) < 0;
    case '<=': return compareValues(v, target) <= 0;
    case 'array-contains':
      return Array.isArray(v) && v.some((x) => deepEqual(x, target));
    case 'array-contains-any':
      return Array.isArray(v) && Array.isArray(target) && v.some((x) => target.some((t) => deepEqual(x, t)));
    case 'in':
      return Array.isArray(target) && target.some((t) => deepEqual(v, t));
    case 'not-in':
      return Array.isArray(target) && !target.some((t) => deepEqual(v, t));
    default: return false;
  }
}

function deepMerge(target, source) {
  if (!source || typeof source !== 'object' || Array.isArray(source)) return source;
  const out = target && typeof target === 'object' && !Array.isArray(target) ? { ...target } : {};
  for (const k of Object.keys(source)) {
    const v = source[k];
    if (
      v && typeof v === 'object' && !Array.isArray(v) &&
      out[k] && typeof out[k] === 'object' && !Array.isArray(out[k]) &&
      !(v.__t === 'ts')
    ) {
      out[k] = deepMerge(out[k], v);
    } else {
      out[k] = v;
    }
  }
  return out;
}

function splitPath(path) {
  const segs = path.split('/').filter(Boolean);
  if (segs.length % 2 !== 0) return null;
  return {
    collection: segs.slice(0, -1).join('/'),
    doc_id: segs[segs.length - 1],
  };
}

async function opGetDoc(DB, { path }) {
  if (!path) return { status: 400, body: { error: 'Missing path' } };
  const row = await DB.prepare('SELECT data FROM documents WHERE path = ?').bind(path).first();
  return { status: 200, body: { data: row ? JSON.parse(row.data) : null } };
}

async function opGetDocs(DB, { collection, constraints = [] }) {
  if (!collection) return { status: 400, body: { error: 'Missing collection' } };
  const rows = await DB.prepare('SELECT doc_id, data FROM documents WHERE collection = ?')
    .bind(collection).all();
  let docs = rows.results.map((r) => ({ id: r.doc_id, data: JSON.parse(r.data) }));

  for (const c of constraints) {
    if (c.kind === 'where') docs = docs.filter((d) => evalWhere(d.data, c));
  }
  const orderBys = constraints.filter((c) => c.kind === 'orderBy');
  if (orderBys.length) {
    docs.sort((a, b) => {
      for (const ob of orderBys) {
        const av = getFieldValue(a.data, ob.field);
        const bv = getFieldValue(b.data, ob.field);
        const cmp = compareValues(av, bv);
        if (cmp !== 0) return ob.direction === 'desc' ? -cmp : cmp;
      }
      return 0;
    });
  }
  const lim = constraints.find((c) => c.kind === 'limit');
  if (lim) docs = docs.slice(0, lim.count);

  return { status: 200, body: { docs } };
}

async function opSetDoc(DB, { path, data, merge }) {
  if (!path || data === undefined) return { status: 400, body: { error: 'Missing path or data' } };
  const p = splitPath(path);
  if (!p) return { status: 400, body: { error: 'Invalid path' } };
  const now = Date.now();

  if (merge) {
    const existing = await DB.prepare('SELECT data, created_at FROM documents WHERE path = ?').bind(path).first();
    const existingData = existing ? JSON.parse(existing.data) : {};
    const merged = deepMerge(existingData, data);
    const createdAt = existing ? existing.created_at : now;
    await DB.prepare(
      `INSERT INTO documents (path, collection, doc_id, data, created_at, updated_at)
       VALUES (?, ?, ?, ?, ?, ?)
       ON CONFLICT(path) DO UPDATE SET data = excluded.data, updated_at = excluded.updated_at`,
    ).bind(path, p.collection, p.doc_id, JSON.stringify(merged), createdAt, now).run();
  } else {
    const existing = await DB.prepare('SELECT created_at FROM documents WHERE path = ?').bind(path).first();
    const createdAt = existing ? existing.created_at : now;
    await DB.prepare(
      `INSERT INTO documents (path, collection, doc_id, data, created_at, updated_at)
       VALUES (?, ?, ?, ?, ?, ?)
       ON CONFLICT(path) DO UPDATE SET data = excluded.data, updated_at = excluded.updated_at`,
    ).bind(path, p.collection, p.doc_id, JSON.stringify(data), createdAt, now).run();
  }
  return { status: 200, body: { ok: true } };
}

async function opUpdateDoc(DB, { path, data }) {
  if (!path || data === undefined) return { status: 400, body: { error: 'Missing path or data' } };
  const existing = await DB.prepare('SELECT data FROM documents WHERE path = ?').bind(path).first();
  if (!existing) return { status: 404, body: { error: 'No document to update' } };
  const merged = { ...JSON.parse(existing.data), ...data };
  await DB.prepare('UPDATE documents SET data = ?, updated_at = ? WHERE path = ?')
    .bind(JSON.stringify(merged), Date.now(), path).run();
  return { status: 200, body: { ok: true } };
}

async function opAddDoc(DB, { collection, data }) {
  if (!collection || data === undefined) return { status: 400, body: { error: 'Missing collection or data' } };
  const segs = collection.split('/').filter(Boolean);
  if (segs.length % 2 !== 1) return { status: 400, body: { error: 'Invalid collection path' } };
  const id = autoId();
  const path = collection + '/' + id;
  const now = Date.now();
  await DB.prepare(
    `INSERT INTO documents (path, collection, doc_id, data, created_at, updated_at)
     VALUES (?, ?, ?, ?, ?, ?)`,
  ).bind(path, collection, id, JSON.stringify(data), now, now).run();
  return { status: 200, body: { id, path } };
}

async function opDeleteDoc(DB, { path }) {
  if (!path) return { status: 400, body: { error: 'Missing path' } };
  await DB.prepare('DELETE FROM documents WHERE path = ?').bind(path).run();
  return { status: 200, body: { ok: true } };
}

export async function handleDb(c) {
  const user = c.get('user');
  if (!user) return c.json({ error: 'Unauthorized' }, 401);

  const body = await c.req.json().catch(() => null);
  if (!body || !body.op) return c.json({ error: 'Missing op' }, 400);

  const DB = c.env.DB;
  let result;
  switch (body.op) {
    case 'getDoc': result = await opGetDoc(DB, body); break;
    case 'getDocs': result = await opGetDocs(DB, body); break;
    case 'setDoc': result = await opSetDoc(DB, body); break;
    case 'updateDoc': result = await opUpdateDoc(DB, body); break;
    case 'addDoc': result = await opAddDoc(DB, body); break;
    case 'deleteDoc': result = await opDeleteDoc(DB, body); break;
    default: return c.json({ error: `Unknown op: ${body.op}` }, 400);
  }
  return c.json(result.body, result.status);
}
