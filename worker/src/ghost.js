// Ghost of Future You — weekly DM written by the user's own one-year-future self.
// Triggered from /api/auth/me via waitUntil() so the /me response never blocks
// on AI inference. First-time users get a message immediately; subsequent
// messages are gated by a 7-day cooldown.

export const FUTURE_SELF_BOT_ID = '__journeyhub_future_self__';
const COOLDOWN_MS = 7 * 24 * 60 * 60 * 1000;
const MODEL = '@cf/meta/llama-3.1-8b-instruct';

const AUTO_ID_CHARS = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789';
function autoId() {
  const buf = crypto.getRandomValues(new Uint8Array(20));
  let s = '';
  for (let i = 0; i < 20; i++) s += AUTO_ID_CHARS[buf[i] % AUTO_ID_CHARS.length];
  return s;
}

async function readDoc(DB, path) {
  const row = await DB.prepare('SELECT data FROM documents WHERE path = ?').bind(path).first();
  return row ? JSON.parse(row.data) : null;
}

async function writeDocRaw(DB, path, data) {
  const segs = path.split('/').filter(Boolean);
  const collection = segs.slice(0, -1).join('/');
  const doc_id = segs[segs.length - 1];
  const now = Date.now();
  const existing = await DB.prepare('SELECT created_at FROM documents WHERE path = ?').bind(path).first();
  const createdAt = existing ? existing.created_at : now;
  await DB.prepare(
    `INSERT INTO documents (path, collection, doc_id, data, created_at, updated_at)
     VALUES (?, ?, ?, ?, ?, ?)
     ON CONFLICT(path) DO UPDATE SET data = excluded.data, updated_at = excluded.updated_at`,
  ).bind(path, collection, doc_id, JSON.stringify(data), createdAt, now).run();
}

function ts(ms) {
  return { __t: 'ts', m: ms };
}

export async function maybeSpawnGhost(env, userId) {
  if (!userId || !env?.DB || !env?.AI) return;

  const DB = env.DB;
  const convId = `future__${userId}`;
  const convPath = `conversations/${convId}`;
  const messagesColl = `conversations/${convId}/messages`;

  // Last message from the Future Self bot in this conversation
  const lastBotRow = await DB.prepare(
    `SELECT data, updated_at FROM documents
      WHERE collection = ?
        AND json_extract(data, '$.authorId') = ?
      ORDER BY updated_at DESC LIMIT 1`,
  ).bind(messagesColl, FUTURE_SELF_BOT_ID).first();

  if (lastBotRow && (Date.now() - lastBotRow.updated_at) < COOLDOWN_MS) {
    return; // cooldown active
  }

  // Load user profile
  const user = await readDoc(DB, `users/${userId}`);
  const username = user?.username || 'you';
  const bio = (user?.bio || '').slice(0, 400);
  const interests = Array.isArray(user?.interests) ? user.interests.slice(0, 8).join(', ') : '';
  const expertise = Array.isArray(user?.expertiseAreas) ? user.expertiseAreas.slice(0, 5).join(', ') : '';
  const headline = user?.headline || '';

  // Recent authored threads
  const threadRows = await DB.prepare(
    `SELECT data FROM documents
      WHERE collection = 'threads'
        AND json_extract(data, '$.authorId') = ?
      ORDER BY updated_at DESC LIMIT 5`,
  ).bind(userId).all();
  const threadsBlock = (threadRows.results || [])
    .map((r) => JSON.parse(r.data))
    .map((t) => `- "${(t.title || '').slice(0, 120)}": ${(t.content || '').slice(0, 240)}`)
    .join('\n') || '(no threads yet)';

  // Recent comments
  const commentRows = await DB.prepare(
    `SELECT data FROM documents
      WHERE collection = 'comments'
        AND json_extract(data, '$.authorId') = ?
      ORDER BY updated_at DESC LIMIT 5`,
  ).bind(userId).all();
  const commentsBlock = (commentRows.results || [])
    .map((r) => JSON.parse(r.data))
    .map((c) => `- ${(c.content || '').slice(0, 240)}`)
    .join('\n') || '(no comments yet)';

  const isFirst = !lastBotRow;
  const prevContent = lastBotRow ? (JSON.parse(lastBotRow.data).content || '') : '';
  const nextYear = new Date().getFullYear() + 1;

  const system = `You are the user's own voice from one year in the future, writing them a private DM. You ARE them, just a year older. Speak in first person ("I"/"we"), never "you should." You care deeply about the version of yourself you're becoming.

Voice rules:
- Specific. Reference exact things they've written, built, or worried about.
- Personal. You know this person intimately — you are them. Drop platitudes.
- Honest. Sometimes encouraging, sometimes challenging. Never fake, never preachy.
- Length: 3-5 sentences. No preamble. No sign-off.
- Output ONLY the message text. No markdown, no greeting like "Hey,".`;

  const userPrompt = isFirst
    ? `This is your FIRST message to present-you. Introduce yourself as them one year from now (${nextYear}). Acknowledge what you can see from their current profile and activity. Don't promise anything specific about the future — you're not a fortune teller — but make clear you're watching the path being built right now and you'll keep in touch.

Present-you's profile:
Username: ${username}
${headline ? `Headline: ${headline}\n` : ''}Bio: ${bio || '(empty)'}
Interests: ${interests || '(none listed)'}
Expertise: ${expertise || '(none listed)'}

Recent threads:
${threadsBlock}

Recent comments:
${commentsBlock}

Write the opening message.`
    : `Write this week's message to present-you.

Present-you's profile:
Username: ${username}
Bio: ${bio || '(empty)'}
Interests: ${interests || '(none listed)'}

Recent threads:
${threadsBlock}

Recent comments:
${commentsBlock}

The last thing you wrote to them:
"${prevContent}"

Reference specific recent activity. Write what you want to say this week.`;

  let text;
  try {
    const result = await env.AI.run(MODEL, {
      messages: [
        { role: 'system', content: system },
        { role: 'user', content: userPrompt },
      ],
      temperature: 0.85,
      max_tokens: 400,
    });
    text = typeof result?.response === 'string' ? result.response.trim() : '';
  } catch (e) {
    console.error('Ghost AI call failed:', e);
    return;
  }
  if (!text) return;

  // Ensure conversation doc exists and update metadata
  const now = Date.now();
  const existingConv = await readDoc(DB, convPath);
  const conv = existingConv || {
    participants: [userId, FUTURE_SELF_BOT_ID],
    participantNames: [username, `You (${nextYear})`],
    botType: 'futureSelf',
    botUserId: FUTURE_SELF_BOT_ID,
    createdAt: ts(now),
  };
  conv.lastMessage = text.slice(0, 120);
  conv.lastMessageAt = ts(now);
  conv.unreadBy = [userId];
  await writeDocRaw(DB, convPath, conv);

  // Write the message
  const msgId = autoId();
  await writeDocRaw(DB, `${messagesColl}/${msgId}`, {
    author: `You (${nextYear})`,
    authorId: FUTURE_SELF_BOT_ID,
    content: text,
    timestamp: ts(now),
  });

  // Broadcast to any connected listeners
  try {
    const doId = env.CONVROOMS.idFromName(convId);
    const stub = env.CONVROOMS.get(doId);
    await stub.broadcast({ type: 'message', convId, msgId });
  } catch (e) {
    console.error('Ghost broadcast failed:', e);
  }
}
