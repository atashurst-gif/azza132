/* Exercises the AI-coach path end to end without a real key: a local mock of the Messages API records the request the
   server sends and returns a structured reply; the browser must apply it. Also checks the outage fallback.
   Run: node tests/coach-mock.mjs */
import http from 'node:http';
import { spawn } from 'node:child_process';
import path from 'node:path';
import { createRequire } from 'node:module';
import { fileURLToPath } from 'node:url';
const require = createRequire(import.meta.url);
let pw; try { pw = require('playwright'); } catch { pw = require('/opt/node-tools/node_modules/playwright'); }
const root = path.join(path.dirname(fileURLToPath(import.meta.url)), '..');
let seen = null, failNext = false;
const mock = http.createServer((req, res) => {
  let body = ''; req.on('data', c => body += c); req.on('end', () => {
    seen = { url: req.url, headers: req.headers, body: JSON.parse(body || '{}') };
    if (failNext) { res.writeHead(529, { 'content-type': 'application/json' }); res.end(JSON.stringify({ type: 'error', error: { type: 'overloaded_error', message: 'overloaded' } })); return; }
    const reply = { say: 'Watch my fretting hand on the G shape.', honest: false, actions: [{ type: 'select', from: 1, to: 1 }, { type: 'play', from: 1, to: 1, loop: 2, countIn: false }, { type: 'prompt_attempt' }], remember: [{ track: 'teaching', text: 'Likes to watch first.' }], session: { awaiting: 'attempt_report' } };
    res.writeHead(200, { 'content-type': 'application/json' });
    res.end(JSON.stringify({ id: 'msg_mock', type: 'message', role: 'assistant', model: seen.body.model, content: [{ type: 'text', text: JSON.stringify(reply) }], stop_reason: 'end_turn', stop_sequence: null, usage: { input_tokens: 10, output_tokens: 10 } }));
  });
});
await new Promise(r => mock.listen(0, '127.0.0.1', r));
const PORT = 9100 + Math.floor(Math.random() * 500);
const server = spawn(process.execPath, ['server.mjs'], { cwd: root, env: { ...process.env, PORT: String(PORT), ANTHROPIC_API_KEY: 'test-key-not-real', ANTHROPIC_BASE_URL: `http://127.0.0.1:${mock.address().port}` }, stdio: ['ignore', 'pipe', 'pipe'] });
await new Promise((resolve, reject) => { const t = setTimeout(() => reject(new Error('server did not start')), 8000); server.stdout.on('data', d => { if (String(d).includes('AI coach')) { clearTimeout(t); resolve(); } }); });
let ok = 0, bad = 0; const check = (c, m) => { if (c) { ok++; console.log('PASS ' + m); } else { bad++; console.error('FAIL ' + m); } };
const browser = await pw.chromium.launch({ args: ['--autoplay-policy=no-user-gesture-required'] });
const page = await browser.newPage();
await page.goto(`http://localhost:${PORT}/#studio`); await page.evaluate(() => localStorage.clear()); await page.reload(); await page.waitForTimeout(500);
check(/AI coach connected/.test(await page.textContent('#coachMode')), 'browser detects the AI coach');
await page.fill('#messageInput', 'Can you show me the G chord?'); await page.press('#messageInput', 'Enter');
await page.waitForFunction(() => /G shape/.test(document.querySelector('.bubble.tutor:last-child')?.textContent || ''), null, { timeout: 8000 }).catch(() => {});
const b = seen && seen.body;
check(b && b.model === 'claude-opus-5-5', 'request uses claude-opus-5-5');
check(b && b.fallbacks === 'default' && /server-side-fallback-2026-07-01/.test(seen.headers['anthropic-beta'] || ''), 'request opts into server-side fallbacks');
check(b && b.output_config && b.output_config.format && b.output_config.format.type === 'json_schema' && b.output_config.effort === 'low', 'request asks for structured JSON at low effort');
check(b && !('thinking' in b) && !('temperature' in b), 'no thinking/sampling params that Opus 5.5 rejects');
check(b && Array.isArray(b.system) && b.system[0].cache_control, 'system prompt is cacheable');
check(b && /Learner says: Can you show me the G chord\?/.test(b.messages[0].content) && /"capo":5/.test(b.messages[0].content), 'lesson state and message are sent');
check(!JSON.stringify(b || {}).includes('test-key-not-real'), 'API key never appears in the request body');
const state = await page.evaluate(() => ({ sel: window.FretwiseDebug.state.selection, playing: window.FretwiseDebug.player()?.isPlaying(), mem: JSON.stringify(window.FretwiseDebug.state.memory.teaching.notes), attempt: !document.getElementById('attemptBar').hidden }));
check(state.sel.from === 1 && state.sel.to === 1 && state.playing, 'AI actions drive the guitar (select + play)');
check(/Likes to watch first/.test(state.mem) && state.attempt, 'AI memory and your-turn prompt applied');
failNext = true;
await page.click('#stopDemo');
await page.fill('#messageInput', 'hello'); await page.press('#messageInput', 'Enter');
await page.waitForFunction(() => /warm up/.test(document.querySelector('.bubble.tutor:last-child')?.textContent || ''), null, { timeout: 20000 }).catch(() => {});
check(/warm up/.test(await page.textContent('.bubble.tutor:last-child')), 'API outage falls back to the rules coach');
check(/unreachable/.test(await page.textContent('#coachMode')), 'outage is reported honestly');
const html = await (await fetch(`http://localhost:${PORT}/`)).text();
check(!html.includes('test-key-not-real'), 'key is not served to the browser');
await browser.close(); server.kill(); mock.close();
console.log(`\n${ok}/${ok + bad} coach checks passed`); process.exit(bad ? 1 : 0);
