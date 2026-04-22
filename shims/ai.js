// ai.js — thin client for the Worker's /api/ai endpoint. The Worker holds the
// AI provider (currently Cloudflare Workers AI, Llama 3.1 8B by default), so
// the user never sees or enters an API key.

export async function callAI(prompt, opts = {}) {
  const res = await fetch('/api/ai', {
    method: 'POST',
    credentials: 'include',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      prompt,
      systemInstruction: opts.systemInstruction,
      temperature: opts.temperature,
      maxTokens: opts.maxTokens,
      model: opts.model,
    }),
  });
  const data = await res.json().catch(() => ({}));
  if (!res.ok) {
    throw new Error(data.error || `AI error ${res.status}`);
  }
  return typeof data.text === 'string' ? data.text : '';
}

// Strip markdown code fences (```json ... ```) and parse JSON.
export function parseJsonLoose(raw) {
  if (!raw) return null;
  let s = String(raw).trim();
  s = s.replace(/^```(?:json)?\s*/i, '').replace(/```$/i, '').trim();
  try { return JSON.parse(s); } catch {}
  const m = s.match(/\{[\s\S]*\}/);
  if (m) { try { return JSON.parse(m[0]); } catch {} }
  return null;
}
