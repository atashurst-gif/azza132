#!/usr/bin/env node
/* Fretwise local server.
   - Serves the app on http://localhost:8000 (localhost counts as a secure origin, so the microphone works).
   - Optional AI coach: POST /api/chat forwards the learner's message plus a compact lesson/memory snapshot to Claude and
     returns the SAME reply contract the built-in rules coach uses: {say, actions[], remember[], honest}.
     The API key stays on this server and is never sent to the browser.
   - With no ANTHROPIC_API_KEY (or no SDK installed) /api/status reports llm:false and the browser keeps using the
     rules-based coach, so typed lessons always work.

   Run:  node server.mjs            (demo coach)
         npm install && ANTHROPIC_API_KEY=... node server.mjs   (AI coach)                                    */
import http from 'node:http';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const here = path.dirname(fileURLToPath(import.meta.url));
loadDotEnv(path.join(here, '.env'));
const PORT = Number(process.env.PORT || 8000);
const HOST = process.env.HOST || '127.0.0.1';
const MODEL = process.env.FRETWISE_MODEL || 'claude-opus-5-5';
const EFFORT = process.env.FRETWISE_EFFORT || 'low';   // short conversational turns; raise for richer answers

function loadDotEnv(file) {
  try {
    for (const line of fs.readFileSync(file, 'utf8').split(/\r?\n/)) {
      const m = line.match(/^\s*([A-Z0-9_]+)\s*=\s*(.*)\s*$/);
      if (m && process.env[m[1]] === undefined) process.env[m[1]] = m[2].replace(/^['"]|['"]$/g, '');
    }
  } catch { /* no .env is fine */ }
}

let client = null, sdk = null, llmError = null;
async function initLLM() {
  if (!process.env.ANTHROPIC_API_KEY && !process.env.ANTHROPIC_AUTH_TOKEN) { llmError = 'No ANTHROPIC_API_KEY set'; return; }
  try {
    sdk = (await import('@anthropic-ai/sdk')).default;
    client = new sdk();
  } catch (e) {
    llmError = 'Anthropic SDK not installed (run npm install)';
  }
}

/* ---------- static files ---------- */
const TYPES = { '.html': 'text/html; charset=utf-8', '.js': 'text/javascript; charset=utf-8', '.mjs': 'text/javascript; charset=utf-8', '.css': 'text/css; charset=utf-8', '.json': 'application/json; charset=utf-8', '.jpeg': 'image/jpeg', '.jpg': 'image/jpeg', '.png': 'image/png', '.svg': 'image/svg+xml', '.ico': 'image/x-icon', '.glb': 'model/gltf-binary', '.mp3': 'audio/mpeg', '.webp': 'image/webp' };
const PUBLIC = new Set(['index.html', 'styles.css', 'app.js', 'music.js', 'performance.js', 'audio.js', 'tutor.js', 'teacher3d.js', 'sample_original_lesson.json']);
function serveStatic(req, res) {
  const url = new URL(req.url, 'http://localhost');
  let rel = decodeURIComponent(url.pathname).replace(/^\/+/, '') || 'index.html';
  const okAsset = /^assets\/[\w.-]+\.(jpe?g|png|svg)$/i.test(rel)
    || /^assets\/models\/[\w.-]+\.glb$/i.test(rel)
    || /^assets\/teacher\/[\w.-]+\.(png|jpe?g|webp)$/i.test(rel)
    || /^assets\/samples\/[\w.-]+\/[\w#.-]+\.mp3$/i.test(rel)
    || /^vendor\/three\/[\w./-]+\.js$/i.test(rel) && !rel.includes('..')
    || (process.env.FRETWISE_LAB && /^lab\/[\w.-]+\.(html|js)$/.test(rel));
  if (!PUBLIC.has(rel) && !okAsset) { res.writeHead(404, { 'Content-Type': 'text/plain' }); res.end('Not found'); return; }
  const file = path.join(here, rel);
  fs.readFile(file, (err, data) => {
    if (err) { res.writeHead(404); res.end('Not found'); return; }
    res.writeHead(200, { 'Content-Type': TYPES[path.extname(file).toLowerCase()] || 'application/octet-stream', 'Cache-Control': 'no-cache' });
    res.end(data);
  });
}

/* ---------- AI coach ---------- */
// Stable instructions first so the prompt prefix caches across turns; the volatile snapshot goes in the user turn.
const SYSTEM = `You are Fret, a patient, warm guitar tutor inside the Fretwise app, speaking British English. You teach ONE learner on a Yamaha F310 acoustic.

How you teach:
- The app's virtual guitar demonstrates; you talk. Prefer "watch me play it" over describing it. Default to showing the teacher's hands, not fretboard dots.
- Follow the learner's teaching preferences in memory.teaching. If explanation is "short": ONE instruction and at most one short question, under 30 words, no lists. "medium": up to 3 short sentences. "detailed": up to 5 sentences.
- Teaching loop: identify the goal, pick a tiny passage, demonstrate, let them try, ask how it went, give ONE concrete correction, retry, and move on only when they're ready.
- Only show fretboard dots (action "hints" show:true) when the learner asks ("show me the dots", "which finger", "which fret") or after repeated difficulty and they agree. Hide them when asked.
- Ground every fret, string, finger and chord in the lesson data you are given. Never invent notes, tab, chords or timings for songs. If the learner asks for a commercial song (e.g. Sultans of Swing) and no imported arrangement is present in the lesson data, say plainly you don't have an accurate licensed transcription, set honest:true, and offer to work from an arrangement they import or from the existing exercises.
- Capo: the shape stays the same, the sounding chord moves up one semitone per fret. Use the "sounding" values provided.
- Pain or discomfort: tell them to stop, never encourage playing through pain, suggest lighter pressure, wrist/thumb position, or a setup check. You cannot judge finger pressure or full chords from a microphone.
- Requests to change the app's software (buttons, screens, features) are NOT lessons: reply briefly and return the action owner_request with their words. Never claim you changed code.
- Save memory only for durable facts: "playing" for their abilities/instrument/struggles, "teaching" for what helps or confuses them.

Actions you may return (the app executes only these):
- {"type":"play","from":i,"to":j,"loop":n,"countIn":bool}  demonstrate steps i..j (0-based) n times (1-8)
- {"type":"select","from":i,"to":j,"confirm":bool}  mark a section; confirm:true asks "is this the part?"
- {"type":"stop"} | {"type":"tempo","bpm":50-160} | {"type":"slow","on":bool}
- {"type":"hints","show":bool} | {"type":"capo","fret":0-7}
- {"type":"lesson","id":"capo"|"barre"|"changes"|"lead"|<imported id>}
- {"type":"prefs","explanation":"short"|"medium"|"detailed","pace":"gentle"|"normal"}
- {"type":"prompt_attempt"}  show "your turn" buttons after a demo
- {"type":"owner_request","text":"..."}
Order matters: e.g. lesson, then select, then play, then prompt_attempt. Keep actions to what this turn needs.
Set session.awaiting to one of: null, "demo_offer", "confirm_section", "attempt_report", "hint_offer" so the app knows what a bare "yes"/"no" refers to.`;

const REPLY_SCHEMA = {
  type: 'object',
  additionalProperties: false,
  required: ['say', 'actions', 'remember', 'honest', 'session'],
  properties: {
    say: { type: 'string' },
    honest: { type: 'boolean' },
    actions: {
      type: 'array',
      items: {
        type: 'object',
        additionalProperties: false,
        required: ['type'],
        properties: {
          type: { type: 'string', enum: ['play', 'stop', 'select', 'tempo', 'slow', 'hints', 'lesson', 'page', 'owner_request', 'prefs', 'prompt_attempt', 'capo'] },
          from: { type: 'integer' }, to: { type: 'integer' }, loop: { type: 'integer' }, countIn: { type: 'boolean' }, confirm: { type: 'boolean' },
          bpm: { type: 'integer' }, on: { type: 'boolean' }, show: { type: 'boolean' }, fret: { type: 'integer' }, id: { type: 'string' }, text: { type: 'string' },
          explanation: { type: 'string', enum: ['short', 'medium', 'detailed'] }, pace: { type: 'string', enum: ['gentle', 'normal'] }
        }
      }
    },
    remember: {
      type: 'array',
      items: { type: 'object', additionalProperties: false, required: ['track', 'text'], properties: { track: { type: 'string', enum: ['playing', 'teaching'] }, text: { type: 'string' } } }
    },
    session: {
      type: 'object', additionalProperties: false, required: ['awaiting'],
      properties: { awaiting: { anyOf: [{ type: 'string', enum: ['demo_offer', 'confirm_section', 'attempt_report', 'hint_offer'] }, { type: 'null' }] } }
    }
  }
};

function snapshot(ctx) {
  // Keep the per-turn payload compact and free of anything the model should not need.
  const c = ctx || {};
  return JSON.stringify({
    lesson: c.lesson, capo: c.capo, tempo: c.tempo, baseTempo: c.baseTempo, cursor: c.cursor, selection: c.selection,
    hintsVisible: c.hintsVisible, micOn: c.micOn, session: c.session, memory: c.memory,
    recentConversation: Array.isArray(c.history) ? c.history.slice(-12).map(m => ({ who: m.kind, text: String(m.text).slice(0, 400) })) : []
  });
}

async function coachReply(message, context) {
  const request = {
    model: MODEL,
    max_tokens: 4000,
    betas: ['server-side-fallback-2026-07-01'],
    fallbacks: 'default',
    system: [{ type: 'text', text: SYSTEM, cache_control: { type: 'ephemeral' } }],
    output_config: { effort: EFFORT, format: { type: 'json_schema', schema: REPLY_SCHEMA } },
    messages: [{ role: 'user', content: `App state (JSON):\n${snapshot(context)}\n\nLearner says: ${String(message).slice(0, 1200)}` }]
  };
  const response = await client.beta.messages.create(request);
  if (response.stop_reason === 'refusal') throw new Error('refusal');
  const text = response.content.filter(b => b.type === 'text').map(b => b.text).join('');
  const data = JSON.parse(text);
  if (typeof data.say !== 'string') throw new Error('malformed reply');
  return data;
}

function readBody(req, limit = 64 * 1024) {
  return new Promise((resolve, reject) => {
    let size = 0; const chunks = [];
    req.on('data', c => { size += c.length; if (size > limit) { reject(new Error('too large')); req.destroy(); } else chunks.push(c); });
    req.on('end', () => resolve(Buffer.concat(chunks).toString('utf8')));
    req.on('error', reject);
  });
}
function json(res, status, body) { res.writeHead(status, { 'Content-Type': 'application/json; charset=utf-8', 'Cache-Control': 'no-store' }); res.end(JSON.stringify(body)); }

const server = http.createServer(async (req, res) => {
  const url = new URL(req.url, 'http://localhost');
  if (url.pathname === '/api/status' && req.method === 'GET') {
    const portrait = ['portrait.webp', 'portrait.png', 'portrait.jpg', 'portrait.jpeg'].find(f => fs.existsSync(path.join(here, 'assets', 'teacher', f)));
    return json(res, 200, { llm: !!client, model: client ? MODEL : null, reason: client ? null : llmError, teacherPortrait: portrait ? `./assets/teacher/${portrait}` : null });
  }
  if (url.pathname === '/api/chat') {
    if (req.method !== 'POST') return json(res, 405, { error: 'POST only' });
    if (!client) return json(res, 503, { error: llmError || 'AI coach not configured' });
    try {
      const { message, context } = JSON.parse(await readBody(req));
      if (typeof message !== 'string' || !message.trim()) return json(res, 400, { error: 'message required' });
      return json(res, 200, await coachReply(message, context));
    } catch (e) {
      const status = sdk && e instanceof sdk.RateLimitError ? 429 : sdk && e instanceof sdk.APIError ? 502 : 500;
      console.warn('[coach]', e?.message || e);
      return json(res, status, { error: 'AI coach unavailable' });   // the browser falls back to the rules coach
    }
  }
  if (req.method !== 'GET' && req.method !== 'HEAD') return json(res, 405, { error: 'method not allowed' });
  serveStatic(req, res);
});

await initLLM();
server.listen(PORT, HOST, () => {
  console.log(`Fretwise running at http://localhost:${PORT}`);
  console.log(client ? `AI coach: ${MODEL} (effort ${EFFORT})` : `AI coach: off (${llmError}) — using the built-in rules coach`);
});
