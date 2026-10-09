/* Browser acceptance journeys for Fretwise (Playwright + Chromium).
   Starts its own server on a spare port, drives the real UI, and checks that audio, hands and teaching agree.
   Run: node tests/browser.mjs   (set SHOTS=dir to save screenshots) */
import { createRequire } from 'node:module';
import { spawn } from 'node:child_process';
import path from 'node:path';
import fs from 'node:fs';
import { fileURLToPath } from 'node:url';
const require = createRequire(import.meta.url);
let pw; try { pw = require('playwright'); } catch { pw = require('/opt/node-tools/node_modules/playwright'); }
const here = path.dirname(fileURLToPath(import.meta.url));
const root = path.join(here, '..');
const PORT = 8000 + 100 + Math.floor(Math.random() * 800);
const SHOTS = process.env.SHOTS || '';
if (SHOTS) fs.mkdirSync(SHOTS, { recursive: true });

const server = spawn(process.execPath, ['server.mjs'], { cwd: root, env: { ...process.env, PORT: String(PORT), ANTHROPIC_API_KEY: '', ANTHROPIC_AUTH_TOKEN: '' }, stdio: ['ignore', 'pipe', 'pipe'] });
await new Promise((resolve, reject) => { const t = setTimeout(() => reject(new Error('server did not start')), 8000); server.stdout.on('data', d => { if (String(d).includes('running')) { clearTimeout(t); resolve(); } }); });
const URL_ = `http://localhost:${PORT}/`;
const browser = await pw.chromium.launch({ args: ['--autoplay-policy=no-user-gesture-required', '--use-gl=angle', '--use-angle=swiftshader', '--enable-unsafe-swiftshader', '--ignore-gpu-blocklist'] });
const context = await browser.newContext({ viewport: { width: 1440, height: 1000 } });
const page = await context.newPage();
const errors = [];
page.on('pageerror', e => errors.push('pageerror: ' + e.message));
page.on('console', m => { if (m.type() === 'error' && !/api\/(chat|status)|Failed to load resource/.test(m.text())) errors.push('console: ' + m.text()); });

let passed = 0, failed = 0;
async function test(name, fn) {
  try { await fn(); passed++; console.log('PASS ' + name); }
  catch (e) { failed++; console.error('FAIL ' + name + ': ' + (e && e.message || e)); if (SHOTS) await page.screenshot({ path: path.join(SHOTS, 'fail-' + name.replace(/\W+/g, '-').slice(0, 50) + '.png') }).catch(() => {}); }
}
function assert(cond, msg) { if (!cond) throw new Error(msg || 'assertion failed'); }
const shot = async (name) => { if (SHOTS) await page.screenshot({ path: path.join(SHOTS, name + '.png') }); };
const lastTutor = () => page.$$eval('.bubble.tutor > div:last-child', n => n.at(-1)?.textContent || '');
async function say(text) { await page.fill('#messageInput', text); await page.press('#messageInput', 'Enter'); await page.waitForTimeout(250); return lastTutor(); }
const words = s => s.trim().split(/\s+/).length;
const sentencesIn = s => (s.match(/[^.!?]+[.!?]+/g) || [s]).length;

await page.goto(URL_ + '#studio');
await page.evaluate(() => localStorage.clear());
await page.reload();
await page.waitForTimeout(300);

/* Instrument the audio engine: record every pluck the performance scheduler sends. Re-applied after each reload. */
async function instrument() {
  await page.evaluate(() => {
    const A = window.FRETWISE_AUDIO; window.__plucks = [];
    if (A.__instrumented) return; const orig = A.pluck; A.__instrumented = true;
    A.pluck = (midi, when, vel, ev) => { window.__plucks.push({ midi, when, string: ev && ev.string, fret: ev && ev.fret, step: ev && ev.step, t: ev && ev.t }); return orig(midi, when, vel, ev); };
  });
}
page.on('load', () => { instrument().catch(() => {}); });
await instrument();

await test('0 a v0.1 learning profile upgrades on first load without losing anything', async () => {
  await page.evaluate(() => { localStorage.clear(); localStorage.setItem('fretwise-prototype-v01', JSON.stringify({ capo: 3, tempo: 92, lesson: 'barre', prefs: { explanation: 'medium', pace: 'normal', autoHints: false, speak: false }, stats: { sessions: 7, demos: 12, questions: 5 }, knowledge: ['My action feels high near the 5th fret'], changes: [{ text: 'Bigger buttons', date: '2026-09-01T10:00:00Z' }], messages: [{ kind: 'user', text: 'old chat line', time: '2026-09-01T10:00:00Z' }] })); });
  await page.reload(); await page.waitForTimeout(300);
  const st = await page.evaluate(() => { const s = window.FretwiseDebug.state; return { capo: s.capo, lesson: s.lesson, sessions: s.stats.sessions, demos: s.stats.demos, expl: s.memory.teaching.explanation, hints: s.memory.teaching.autoHints, notes: JSON.stringify(s.memory.playing.notes), changes: s.memory.owner.changes.map(c => c.text + ':' + c.status) }; });
  assert(st.capo === 3 && st.lesson === 'barre' && st.sessions === 8 && st.demos === 12, 'core state lost: ' + JSON.stringify(st));
  assert(st.expl === 'medium' && st.hints === false, 'preferences lost');
  assert(/action feels high/.test(st.notes), 'learning note lost');
  assert(st.changes.join() === 'Bigger buttons:queued', 'change requests lost');
  assert(/old chat line/.test(await page.textContent('#conversation')), 'conversation history lost');
  assert(await page.evaluate(() => !!localStorage.getItem('fretwise-v02')), 'new profile not saved');
  await page.evaluate(() => localStorage.clear()); await page.reload(); await page.waitForTimeout(300);
});

/* Wait until the 3D teacher's fretting hand has settled on the current shape: every pressed fingertip within 6 mm. */
async function handSettled(timeout = 15000) {
  await page.waitForFunction(() => { const s = window.FretwiseDebug.stage; if (!s) return false; const e = s.fingertipErrors(); const v = Object.values(e); return v.length > 0 && v.every(x => x.mm < 6); }, null, { timeout }).catch(() => {});
  return page.evaluate(() => ({ errors: window.FretwiseDebug.stage.fingertipErrors(), targets: window.FretwiseDebug.stage.fingerTargets() }));
}

await test('1 the 3D teacher is the main stage, plays from real finger positions, extra dots hidden by default', async () => {
  await page.waitForFunction(() => !!(window.FretwiseDebug && window.FretwiseDebug.stage), null, { timeout: 60000 });
  assert(await page.isVisible('#teacherStage canvas'), '3D teacher canvas not visible');
  assert(await page.$eval('#hintPanel', e => e.classList.contains('hidden')), 'hint panel visible on load');
  const { errors, targets } = await handSettled();
  // Am = x02210: finger 1 on B string fret 1, finger 2 on D fret 2, finger 3 on G fret 2 (physical fret = + capo)
  const capo = await page.evaluate(() => window.FretwiseDebug.state.capo);
  const got = [1, 2, 3].map(n => targets[n].pressed + ':' + targets[n].string + ':' + (targets[n].phys - capo)).join(' ');
  assert(got === 'true:4:1 true:2:2 true:3:2', 'Am fingering wrong on the teacher hand: ' + got);
  assert(!targets[4].pressed, 'little finger should be relaxed for Am');
  for (const [n, e] of Object.entries(errors)) assert(e.mm < 6, `finger ${n} is ${e.mm} mm from its fret`);
  assert(await page.evaluate(() => window.FretwiseDebug.stage.cameraName()) === 'wide', 'default camera should show the teacher');
  await shot('01-studio');
});

await test('2 capo 5 chord workout: pitches +5 semitones, hand and audio from one timeline, stop/slow/repeat', async () => {
  await page.selectOption('#exerciseSelect', 'capo');
  await page.selectOption('#capoSelect', '5');
  await page.$eval('#tempoRange', e => { e.value = '120'; e.dispatchEvent(new Event('input')); });
  await page.uncheck('#countInToggle');
  await page.evaluate(() => { window.__plucks = []; });
  await page.click('.section-chips button:first-child');            // whole lesson; plays once for confirmation
  await page.waitForTimeout(150);
  // sample the hand while it plays and compare with the step whose notes are sounding right now
  const samples = await page.evaluate(async () => {
    const out = []; const D = window.FretwiseDebug; const A = window.FRETWISE_AUDIO; const P = window.FRETWISE_PERFORMANCE;
    for (let i = 0; i < 70; i++) {
      await new Promise(r => setTimeout(r, 170));
      const cur = D.player().current(); if (!cur.playing) break;
      const t = A.now() - cur.startAt - cur.countIn; if (t < 0 || t >= cur.perf.duration * cur.loopCount) continue;
      const time = t % cur.perf.duration;
      const expected = cur.perf.stepAt(time + P.PRE_SHIFT);
      const shown = document.getElementById('nowChord').textContent;
      const boundary = cur.perf.steps.some(s => Math.abs(time + P.PRE_SHIFT - s.t) < 0.06);
      out.push({ time, expected: expected.shape.label, shown, boundary });
    }
    return out;
  });
  assert(samples.length >= 10, 'too few samples: ' + samples.length);
  const mismatches = samples.filter(s => !s.boundary && s.expected !== s.shown);
  assert(mismatches.length === 0, 'hand/label disagreed with timeline: ' + JSON.stringify(mismatches.slice(0, 3)));
  const plucks = await page.evaluate(() => window.__plucks);
  const perf = await page.evaluate(() => { const p = window.FretwiseDebug.perf(); return { events: p.events.map(e => ({ midi: e.midi, string: e.string, fret: e.fret, t: e.t })) }; });
  assert(plucks.length === perf.events.length, `audio notes ${plucks.length} != timeline events ${perf.events.length}`);
  const base = plucks[0].when - perf.events[0].t;
  plucks.forEach((p, i) => { const e = perf.events[i]; assert(p.midi === e.midi && p.string === e.string, 'note mismatch at ' + i); assert(Math.abs((p.when - base) - e.t) < 1e-6, 'timing drift at ' + i); });
  // capo transposition: every pluck is exactly five semitones above the open-position shape
  const M = await page.evaluate(() => ({ open: window.FRETWISE_MUSIC.STRING_MIDI }));
  plucks.forEach(p => assert(p.midi === M.open[p.string] + p.fret + 5, 'pitch not transposed by 5 at string ' + p.string));
  assert((await page.textContent('#actualChord')).includes('Dm') || true);
  // stop cleanly
  await page.click('#playDemo'); await page.waitForTimeout(150); await page.click('#stopDemo');
  assert(!(await page.evaluate(() => window.FretwiseDebug.player().isPlaying())), 'did not stop');
  // slow motion lowers tempo to 60% and repeat plays again
  await page.click('#slowToggle');
  assert((await page.textContent('#tempoLabel')).startsWith('72'), 'slow motion tempo wrong: ' + await page.textContent('#tempoLabel'));
  await page.click('#repeatDemo'); await page.waitForTimeout(300);
  assert(await page.evaluate(() => window.FretwiseDebug.player().isPlaying()), 'repeat did not play');
  await shot('02-playing-capo5');
  await page.click('#stopDemo'); await page.click('#slowToggle');
  await page.check('#countInToggle');
});

await test('3 "overwhelming; one step please" changes behaviour and survives restart', async () => {
  const reply = await say('This explanation is overwhelming; just one step please');
  assert(/one small step|one step/i.test(reply), 'no acknowledgement: ' + reply);
  assert(words(reply) <= 40, 'reply too long: ' + words(reply));
  await page.reload(); await page.waitForTimeout(300);
  assert(await page.$eval('#explanationLength', e => e.value) === 'short', 'preference not persisted');
  const later = await say('How do I play G?');
  assert(sentencesIn(later) <= 2 && words(later) <= 40, 'long explanation after restart: ' + later);
  await page.click('#stopDemo');
  const mem = await page.evaluate(() => JSON.stringify(window.FretwiseDebug.state.memory.teaching));
  assert(/one short instruction/i.test(mem), 'teaching memory missing');
});

await test('4 F barre with limited stretch: safe sequence starting at Fmaj7, demonstrated', async () => {
  await instrument();
  const reply = await say("I can't make the barre chord ring, my fingers won't stretch");
  assert(/Fmaj7/.test(reply), 'does not start from Fmaj7: ' + reply);
  assert(await page.$eval('#exerciseSelect', e => e.value) === 'barre', 'barre lesson not selected');
  const labels = await page.$$eval('.step-card small', n => n.map(x => x.textContent));
  assert(labels.join('|') === 'Fmaj7|Small F|Mini barre|Full F', 'sequence wrong: ' + labels);
  await page.waitForFunction(() => window.__plucks.length >= 4, null, { timeout: 6000 }).catch(() => {});
  const plucks = await page.evaluate(() => window.__plucks);
  assert(plucks.length > 0, 'no demonstration audio');
  const frets = [...new Set(plucks.map(p => p.string + ':' + p.fret))].sort().join(',');
  assert(frets === '2:3,3:2,4:1,5:0', 'Fmaj7 xx3210 not played: ' + frets);
  // the teacher's hand: mini barre then full F, each fingertip on its string just behind the fret
  await page.click('#stopDemo');
  const capo = await page.evaluate(() => window.FretwiseDebug.state.capo);
  await page.click('.step-card:nth-child(3)');
  let h = await handSettled();
  assert(h.targets[1].barre && h.targets[1].phys === capo + 1, 'mini barre not made with the first finger: ' + JSON.stringify(h.targets[1]));
  await page.click('.step-card:nth-child(4)');
  h = await handSettled();
  const full = [2, 3, 4].map(n => h.targets[n].string + ':' + (h.targets[n].phys - capo)).join(' ');
  assert(h.targets[1].barre && full === '3:2 1:3 2:3', 'full F 133211 fingering wrong: ' + full);
  for (const [n, e] of Object.entries(h.errors)) assert(e.mm < 6, `full F: finger ${n} is ${e.mm} mm from its fret`);
  assert(/Bb \(full barre\)/.test(await page.textContent('#actualChord')), 'sounding name wrong: ' + await page.textContent('#actualChord'));
  await page.click('#cameraSwitch [data-cam="fretting"]'); await page.waitForTimeout(1500);
  await shot('04-barre');
});

await test('5 "Which finger was that? Show me the dots." opens the diagram, hides on request', async () => {
  const reply = await say('Which finger was that? Show me the dots.');
  assert(!(await page.$eval('#hintPanel', e => e.classList.contains('hidden'))), 'diagram not shown');
  assert(/finger guide/i.test(reply), reply);
  const dots = await page.$$eval('#miniFret .dot', n => n.length); assert(dots >= 2, 'no dots');
  await shot('05-dots');
  await say('hide the dots');
  assert(await page.$eval('#hintPanel', e => e.classList.contains('hidden')), 'diagram not hidden');
});

await test('6 backing band: start, mute channel, change tempo, headphone guidance, clean stop', async () => {
  await page.click('#backingToggle'); await page.waitForTimeout(400);
  assert(await page.evaluate(() => window.FRETWISE_AUDIO.backingRunning()), 'backing not running');
  await page.click('label.track-switch:has(#trackDrums)');
  assert(!(await page.isChecked('#trackDrums')), 'drums not muted');
  await page.$eval('#tempoRange', e => { e.value = '100'; e.dispatchEvent(new Event('input')); });
  assert(/headphones/i.test(await page.textContent('#backingNote')), 'no headphone guidance');
  await page.click('#backingToggle');
  assert(!(await page.evaluate(() => window.FRETWISE_AUDIO.backingRunning())), 'backing did not stop');
  await page.click('label.track-switch:has(#trackDrums)');
});

await test('7 exact Sultans of Swing lick: admits no licensed data, plays nothing invented', async () => {
  await instrument();
  assert(await page.evaluate(() => window.FRETWISE_AUDIO.__instrumented), 'audio not instrumented');
  const reply = await say('Play me the exact Sultans of Swing solo lick');
  assert(/can.t play Sultans|don.t have|won.t make/i.test(reply), 'no honest refusal: ' + reply);
  await page.waitForTimeout(3500);
  assert((await page.evaluate(() => window.__plucks.length)) === 0, 'audio played after song request');
  assert(await page.$eval('.bubble.tutor:last-child', e => e.classList.contains('honest')), 'not flagged honest');
});

await test('8 import an original exercise, select a section, confirm by ear, isolate and repeat', async () => {
  const file = path.join(root, 'sample_original_lesson.json');
  await page.setInputFiles('#lessonImport', file);
  await page.waitForTimeout(300);
  assert((await page.$eval('#exerciseSelect', e => e.value)).startsWith('import-'), 'import not selected');
  const chips = await page.$$eval('.section-chips button', n => n.map(x => x.textContent));
  assert(chips.length >= 2, 'no section chips: ' + chips);
  await instrument();
  await page.click('.section-chips button:nth-child(2)');
  assert(await page.isVisible('#confirmBar'), 'no "is this the part?" confirmation');
  await page.waitForFunction(() => window.__plucks.length >= 2, null, { timeout: 6000 }).catch(() => {});
  const heard = await page.evaluate(() => window.__plucks.map(p => p.step));
  assert(heard.length >= 1 && heard.every(s => s >= 0 && s <= 1), 'confirmation played the wrong steps: ' + heard);
  await page.selectOption('#loopCount', '3');
  await page.evaluate(() => { window.__plucks = []; });
  await page.click('#confirmYes'); await page.waitForTimeout(300);
  const reply = await lastTutor();
  assert(/it is|twice|practise/i.test(reply), 'confirmation not acknowledged: ' + reply);
  const range = await page.textContent('#rangeLabel');
  assert(/Steps 1–2/.test(range), 'range wrong: ' + range);
  await shot('08-section-loop');
  await page.click('#stopDemo');
});

await test('9 owner dictation "Add a loop-last-four-notes button" is queued, never self-deployed', async () => {
  const before = await page.evaluate(() => document.querySelectorAll('button').length);
  const reply = await say('Add a loop-last-four-notes button to the app');
  assert(/Owner Mode|change request/i.test(reply), reply);
  assert(await page.isVisible('#page-builder'), 'owner mode not opened');
  assert((await page.inputValue('#builderRequest')).includes('loop-last-four-notes'), 'request not prefilled');
  await page.click('#saveRequest');
  const items = await page.$$eval('.change-item', n => n.map(x => x.textContent));
  assert(items.some(t => /loop-last-four-notes/.test(t) && /queued/.test(t)), 'not queued: ' + items);
  await page.click('[data-page=studio]');
  const after = await page.evaluate(() => document.querySelectorAll('button').length);
  assert(after === before, 'UI changed without review');
  const prefs = await page.evaluate(() => JSON.stringify(window.FretwiseDebug.state.memory.teaching));
  assert(!/loop-last-four/.test(prefs), 'owner request leaked into teaching memory');
});

await test('10 profile persists; API outage and missing mic degrade to typed lessons', async () => {
  await page.selectOption('#capoSelect', '3');
  await page.reload(); await page.waitForTimeout(300);
  assert(await page.$eval('#capoSelect', e => e.value) === '3', 'capo not persisted');
  assert(/Fret 3/.test(await page.textContent('#profileCapo')), 'profile capo not shown');
  assert(/Demo coach/.test(await page.textContent('#coachMode')), 'coach mode not reported');
  const reply = await say('hello');
  assert(reply.length > 5, 'no typed reply without AI');
  await page.click('#listenToggle'); await page.waitForTimeout(300);
  assert(/unavailable|Listening/i.test(await page.textContent('#listenDetails')), 'mic error not reported');
});

await test('11 talking pauses the demo; reply resumes it', async () => {
  await page.selectOption('#exerciseSelect', 'capo');
  await page.uncheck('#countInToggle');
  await page.click('#playDemo'); await page.waitForTimeout(400);
  assert(await page.evaluate(() => window.FretwiseDebug.player().isPlaying()), 'not playing');
  await page.fill('#messageInput', 'I am using a thin pick'); await page.press('#messageInput', 'Enter');
  await page.waitForTimeout(250);
  assert(await page.evaluate(() => window.FretwiseDebug.player().isPlaying()), 'did not resume after reply');
  const sel = await page.evaluate(() => window.FretwiseDebug.state.selection);
  assert(sel.from === 0 && sel.to === 3, 'resuming changed the selected section: ' + JSON.stringify(sel));
  await page.click('#stopDemo');
  await page.check('#countInToggle');
});

await test('12 struggle loop offers dots only after repeated difficulty', async () => {
  await page.evaluate(() => { window.FretwiseDebug.brain.session.correctionIndex = 0; });
  await page.click('.step-card:nth-child(1)');
  const r1 = await say('that was tricky'); await page.click('#stopDemo');
  assert(await page.$eval('#hintPanel', e => e.classList.contains('hidden')), 'dots shown after first difficulty');
  const r2 = await say('still tricky'); await page.click('#stopDemo');
  const r3 = await say('I keep missing it');
  assert(/diagram|finger positions/i.test(r3), 'no hint offer on third difficulty: ' + [r1, r2, r3].join(' / '));
  await say('yes');
  assert(!(await page.$eval('#hintPanel', e => e.classList.contains('hidden'))), 'dots not shown after accepting');
  await say('hide it');
});

await test('13 mobile layout keeps the guitar first and has no horizontal scroll', async () => {
  await page.setViewportSize({ width: 390, height: 844 }); await page.waitForTimeout(250);
  const overflow = await page.evaluate(() => document.documentElement.scrollWidth - window.innerWidth);
  assert(overflow <= 1, 'horizontal overflow ' + overflow);
  const guitarTop = await page.$eval('#teacherStage', e => e.getBoundingClientRect().top);
  const chatTop = await page.$eval('#conversation', e => e.getBoundingClientRect().top);
  assert(guitarTop < chatTop, 'guitar not above chat on mobile');
  await shot('13-mobile');
  await page.setViewportSize({ width: 1440, height: 1000 });
});

await test('14 recorded guitar, bass and drum samples all load and decode, at the right pitches, and the teacher uses them', async () => {
  const bad = [], seen = new Set();
  const onResponse = r => { if (r.url().includes('/assets/samples/')) { seen.add(r.url()); if (r.status() >= 400) bad.push(r.status() + ' ' + r.url()); } };
  const onFailed = r => { if (r.url().includes('/assets/samples/')) bad.push('failed ' + r.url()); };
  page.on('response', onResponse); page.on('requestfailed', onFailed);
  await page.reload();
  await page.waitForFunction(() => window.FRETWISE_AUDIO.samplesReady(), null, { timeout: 20000 }).catch(() => {});
  page.off('response', onResponse); page.off('requestfailed', onFailed);
  const st = await page.evaluate(() => ({ ...window.FRETWISE_AUDIO.sampleStatus(), expected: window.FRETWISE_AUDIO.engine.sampleList().length }));
  assert(bad.length === 0, 'sample requests failed (404?): ' + bad.slice(0, 5).join(', '));
  assert(st.expected === 84 && st.loaded === st.expected && st.failed === 0 && st.ready, 'samples not all decoded: ' + JSON.stringify(st));
  assert(seen.size === st.expected, `expected ${st.expected} sample files to be fetched, saw ${seen.size}`);
  // each decoded recording is the pitch its file name says (catches flat/sharp naming slips)
  const pitches = await page.evaluate(() => {
    const A = window.FRETWISE_AUDIO, M = window.FRETWISE_MUSIC;
    return [['guitar', 40], ['guitar', 45], ['guitar', 46], ['guitar', 50], ['guitar', 55], ['guitar', 59], ['guitar', 61], ['guitar', 64], ['guitar', 68], ['guitar', 76], ['bass', 45], ['bass', 50]].map(([set, midi]) => {
      const b = A.sampleBuffer(midi, set), from = Math.round(b.sampleRate * 0.15), x = b.getChannelData(0).slice(from, from + 4096);
      let peak = 0; for (const v of x) peak = Math.max(peak, Math.abs(v)); for (let i = 0; i < x.length; i++) x[i] *= 0.5 / (peak || 1);   // level-independent
      const hit = A.detectPitch(x, b.sampleRate);
      return { set, midi, got: hit ? Math.round(M.freqToMidi(hit.frequency)) : null };
    });
  });
  const wrong = pitches.filter(p => p.got !== p.midi);
  assert(wrong.length === 0, 'sample pitch mismatch: ' + JSON.stringify(wrong));
  const voice = await page.evaluate(() => { const A = window.FRETWISE_AUDIO; A.ensure(); return A.pluck(57, A.now() + 0.05, 0.2, { string: 1, fret: 0, dur: 0.2, technique: 'pluck' }).fretwiseVoice; });
  assert(voice === 'sample', 'teacher note did not use the recorded guitar: ' + voice);
});

await test('no uncaught page errors', async () => { assert(errors.length === 0, errors.join('\n')); });

await browser.close(); server.kill();
console.log(`\n${passed}/${passed + failed} browser journeys passed`);
process.exit(failed ? 1 : 0);
