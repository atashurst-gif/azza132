# Fretwise — AI Guitar Teacher, Prototype 0.2

A local-first browser guitar tutor built for one learner (Yamaha F310, capo 3/5, working towards Sultans of Swing), continued from Prototype 0.1. The teacher's guitar is the main experience: it **plays** each exercise with synchronised audio, fretting hand and picking hand, then talks you through it one step at a time.

## Start

```bash
cd fretwise
node server.mjs          # http://localhost:8000 — rules-based coach, no install needed
```

With the optional AI coach:

```bash
npm install
cp .env.example .env     # add ANTHROPIC_API_KEY
node server.mjs          # coach status shows "AI coach connected"
```

`python3 -m http.server 8000` still works (static, demo coach only). On a Mac, double-click `START_ON_MAC.command`. Use Chrome on desktop for speech recognition; use headphones when the microphone is listening.

## Tests

```bash
node test.cjs                # 37 unit checks: chord voicings, capo, timeline, tutor brain, migration, import
node tests/browser.mjs       # 15 browser journeys (Playwright/Chromium), incl. the 10 acceptance tests in the build prompt
node tests/coach-mock.mjs    # 13 checks of the AI-coach path against a local mock of the Messages API
```

`SHOTS=some/dir node tests/browser.mjs` also saves screenshots.

## What's real in 0.2 (and what isn't)

**Working, and covered by tests**

- **One canonical performance timeline** (`performance.js`). A lesson or section compiles to timed events: string, fret, finger, MIDI pitch (capo applied), stroke direction and technique. Web Audio, the fretting hand, the picking hand, ringing strings and step highlighting all read from that one list, scheduled on the AudioContext clock. The browser test samples the hand during playback and checks it against the timeline, and checks every scheduled note's pitch and time against the event list.
- **Articulated teacher hand** (`hand.js`). SVG fingers reach from a palm above the treble edge to fingertips placed just behind the fret wire. Finger 1 lies flat for barres. Fingers that move lift and arc into place about 130 ms before the note sounds; idle fingers curl clear of the strings. The picking hand travels across the actual strings for each down or up stroke.
- **Strumming patterns** (down/up on the beat and the "and"), capo bar drawn at the nut, physical fret numbers shown, and the hand window moves for melodies above fret 5.
- **Repeat-by-section practice.** Click a step, shift-click to extend it, or use the section chips (imported lessons get automatic phrase sections). Then: "Is this the part?" playback with Earlier / Later / Yes, repeat ×1–8 or until stopped, optional 4-beat count-in, 🐢 slow motion at 60% speed, "Last 2", Back/Next, pause/resume (Space bar) and a pass counter.
- **Tutor brain** (`tutor.js`). Teaching state machine: goal → passage → demo → your turn → observe → one correction → retry → advance. Replies are capped by your preference; in "one step" mode it sends one instruction plus at most one question. Repeated difficulty rotates through slow it down → isolate the join → offer a diagram → finger-by-finger walkthrough, offering dots only after asking you. Fret dots appear only when you ask or accept them.
- **Two-way voice.** Click the 🎙 to talk, or hold it for push-to-talk. Talking pauses the demonstration and stops the tutor's speech (barge-in), and the demonstration resumes after the reply. Spoken replies use en-GB speech synthesis, with a "Stop talking" button. Typing always works.
- **Personal memory in separate tracks:** playing (instrument, chords, goals, comfort limits, practised sections with demos, loops, cleans and tricky counts) and teaching (preferred length, what works, what confuses). Owner change requests are kept separately again. You can view, delete or add entries on *Your tutor*. 0.1 data is migrated automatically and nothing is lost.
- **Honesty rules in code.** Asking for an exact commercial song or lick (e.g. Sultans of Swing) gets a plain "I don't have a licensed transcription, and I won't invent one", and nothing is played. Pain stops playback and is never coached through. The microphone only compares single notes, and only while the app itself is silent; chords are never graded from audio.
- **Owner Mode:** "change the app" requests are classified away from lessons, saved with a status (queued → in review → approved/rejected) and exported as a brief for Claude Code. Nothing self-deploys.
- **Optional AI coach** (`server.mjs`) using `claude-opus-5-5` through the Anthropic SDK, server-side only. It receives the lesson data and memory, and must answer in the same `{say, actions, remember}` contract. Actions are validated against an allow-list before anything runs. If the API is missing or down, the rules coach takes over automatically.
- **Backing band:** per-channel mute and volume, master level, click track, optional start in sync with the teacher's bar 1, and the band follows the selected section.

**Still illustrative or missing**

- The hand is a 2D illustration driven by correct fret, string and finger data. It is not a rigged 3D or biomechanically simulated hand, and it doesn't show wrist or thumb technique.
- The guitar tone is a synthesised plucked string (Karplus–Strong), not a licensed sample library. Slides, bends and vibrato aren't modelled yet; hammer-ons and pull-offs are only softer, unpicked notes.
- Speech recognition depends on the browser (Chrome works best); there is no server speech-to-text or text-to-speech yet.
- Mic feedback is monophonic pitch only, with no onset or rhythm scoring, and needs field testing on a real F310 in a real room.
- No MusicXML, MIDI or Guitar Pro import yet (JSON only). No accounts, sync, subscriptions or licensed catalogue.
- The AI coach was tested against a mock API in this environment, not the live API (no key available here). Run `node server.mjs` with a key to try it for real.

## Import an exercise

Under the lesson picker, choose **Import your own lesson JSON**. Strings are numbered 0 = low E to 5 = high e, and `fret` is counted from the capo. Optional fields: `tempo`, `pattern` (`simple`, `basic`, `once`), and per step `beats`, `finger` (0–4) and `technique` (`pluck`, `hammer`, `pull`, `slide`, `rest`). You can also add `sections` (`[{ "name", "from", "to" }]`, step indexes).

```json
{
  "name": "My original little lick",
  "kind": "melody",
  "steps": [
    {"string": 5, "fret": 0, "label": "Open high e"},
    {"string": 5, "fret": 3, "label": "Fret 3"},
    {"string": 5, "fret": 5, "label": "Fret 5"},
    {"string": 5, "fret": 3, "label": "Back to 3"}
  ],
  "sections": [{"name": "Up", "from": 0, "to": 2}]
}
```

For chords, use `"kind": "chord"` with steps like `{ "chord": "Am" }`. Known IDs: Am, A, E, Em, D, Dm, C, G, Fmaj7, Fsmall, Fmini, F.

Importing doesn't grant any rights. Only import arrangements you wrote or have permission to use.

## Privacy, data and licensing

- Everything is stored in this browser's localStorage (key `fretwise-v02`; the 0.1 key is read once and migrated). Use **Export learning profile** before clearing browser data.
- No microphone audio is stored or sent anywhere. With the AI coach on, your typed or spoken text, the current lesson data and your memory are sent from the local server to the Anthropic API. The API key stays on the server and is never served to the browser.
- The three photos in `assets/` are the owner's own guitar photos for this private build. Replace them before distributing.
- All exercises and backing grooves are original. The guitar, bass and drum sounds are synthesised in code; no third-party samples are used.

## Files

| File | What it does |
| --- | --- |
| `index.html`, `styles.css` | App shell and layout |
| `music.js` | Chord shapes, lessons, sections, capo maths, import validation |
| `performance.js` | Canonical timeline compiler and AudioContext-clocked player |
| `hand.js` | Articulated SVG fretting hand |
| `audio.js` | Plucked-string synth, mixer, backing band, click, pitch detector |
| `tutor.js` | Tutor brain: intents, teaching state machine, memory, migration, action allow-list |
| `app.js` | UI wiring, voice, microphone, memory views, owner mode |
| `server.mjs` | Static server plus optional AI coach (`/api/status`, `/api/chat`) |
| `test.cjs`, `tests/` | Unit, browser and coach tests |
| `CLAUDE_BUILD_PROMPT.md` | The full product specification this build follows |
