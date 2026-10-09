# Fretwise — AI Guitar Teacher, Prototype 0.3

A local-first browser guitar tutor built for one learner (Yamaha F310, capo 3/5, working towards Sultans of Swing), continued from Prototypes 0.1–0.3. The main experience is the **teacher's guitar**, an F310-style instrument in 3D, played with real recorded guitar sound. A close-up of the fretting hand shows exactly where each finger presses: every finger carries its number, and the string it presses lights up in that finger's colour. Strumming is shown as just a pick crossing the strings, in a small inset. The camera buttons switch between the fretting-hand close-up, the whole guitar and the strumming close-up.

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
node test.cjs                # 45 unit checks: chord voicings, capo, timeline, tutor brain, migration, import, audio engine
node tests/browser.mjs       # 16 browser journeys (Playwright/Chromium + WebGL), incl. the 10 acceptance tests in the build prompt
node tests/coach-mock.mjs    # 13 checks of the AI-coach path against a local mock of the Messages API
```

`SHOTS=some/dir node tests/browser.mjs` also saves screenshots.

## What's real in 0.3 (and what isn't)

**Working, and covered by tests**

- **One canonical performance timeline** (`performance.js`). A lesson or section compiles to timed events: string, fret, finger, MIDI pitch (capo applied), stroke direction and technique. Web Audio, the fretting hand, the picking hand, ringing strings and step highlighting all read from that one list, scheduled on the AudioContext clock. The browser test samples the hand during playback and checks it against the timeline, and checks every scheduled note's pitch and time against the event list.
- **Teacher's guitar** (`teacher3d.js`, Three.js). The guitar is built in code to F310 measurements: 634 mm scale, dreadnought body, spruce top, black pickguard, rosewood board with dot inlays, 3+3 tuners and a slim capo when you set one. It sits on a dark studio background.
  - **Fretting hand only.** The rest of the character (Michelle, from Mixamo) is hidden. Her left hand and forearm are cut out of the model and fade towards the elbow. The hand is slightly see-through, so the strings under the fingers stay visible.
  - **Which string, which fret.** Each pressed string glows in its finger's colour from the fret to the bridge (the part that sounds): 1 blue, 2 green, 3 amber, 4 pink. Open strings that are played glow white, and strings you don't play get a × at the nut. A numbered badge sits exactly where each finger presses its string. String names (E A D G B e) sit at the nut, and physical fret numbers run along the edge. The **Colours** button turns all of this off. The on-request finger diagram still only appears when you ask.
  - **Left hand IK.** Solved for every step: each finger spreads at the knuckle and bends in its own plane. For each chord, the hand's placement is optimised so each pressed fingertip lands just behind its fret. Fingers are re-proportioned at load to human hand measurements (tip about 40% of the base segment, middle finger about 78 mm). Idle fingers curl loosely, fingers sharing a fret line up diagonally, and barres lie nearly flat.
  - **Measured accuracy.** Across every chord in the app at capo 0, 3 and 5, plus single notes up to fret 12, the worst fingertip is 5.6 mm from its target and the median is 2.6 mm. The browser test fails beyond 6 mm, or if the string colours or finger numbers are wrong.
  - **Movement.** Fingers that move lift and arc to the next shape 140 ms before it sounds. The close-up keeps a constant 23 cm width and follows the hand along the neck.
  - **Strumming is just a pick.** Its path is built from the same timeline as the audio, so it crosses each string exactly when that note sounds. It shows in an inset during the fretting close-up and full-size in the Strumming view. Plucked strings vibrate.
- **Coach panel and captions.** Fret talks through the coach panel and on-stage subtitles. To give Fret a face (for example artwork made in ChatGPT), put `portrait.webp`, `.png` or `.jpg` in `assets/teacher/`; the app uses it automatically.
- **Adaptive quality:** on slow machines or software renderers, the 3D stage drops resolution and shadows to keep moving smoothly.
- **Repeat-by-section practice.** Click a step, shift-click to extend it, or use the section chips (imported lessons get automatic phrase sections). Then: "Is this the part?" playback with Earlier / Later / Yes, repeat ×1–8 or until stopped, optional 4-beat count-in, 🐢 slow motion at 60% speed, "Last 2", Back/Next, pause/resume (Space bar) and a pass counter.
- **Tutor brain** (`tutor.js`). Teaching state machine: goal → passage → demo → your turn → observe → one correction → retry → advance. Replies are capped by your preference; in "one step" mode it sends one instruction plus at most one question. Repeated difficulty rotates through slow it down → isolate the join → offer a diagram → finger-by-finger walkthrough, offering dots only after asking you. Fret dots appear only when you ask or accept them.
- **Two-way voice.** Click the 🎙 to talk, or hold it for push-to-talk. Talking pauses the demonstration and stops the tutor's speech (barge-in), and the demonstration resumes after the reply. Spoken replies use en-GB speech synthesis, with a "Stop talking" button. Typing always works.
- **Personal memory in separate tracks:** playing (instrument, chords, goals, comfort limits, practised sections with demos, loops, cleans and tricky counts) and teaching (preferred length, what works, what confuses). Owner change requests are kept separately again. You can view, delete or add entries on *Your tutor*. 0.1 data is migrated automatically and nothing is lost.
- **Honesty rules in code.** Asking for an exact commercial song or lick (e.g. Sultans of Swing) gets a plain "I don't have a licensed transcription, and I won't invent one", and nothing is played. Pain stops playback and is never coached through. The microphone only compares single notes, and only while the app itself is silent; chords are never graded from audio.
- **Owner Mode:** "change the app" requests are classified away from lessons, saved with a status (queued → in review → approved/rejected) and exported as a brief for Claude Code. Nothing self-deploys.
- **Optional AI coach** (`server.mjs`) using `claude-opus-5-5` through the Anthropic SDK, server-side only. It receives the lesson data and memory, and must answer in the same `{say, actions, remember}` contract. Actions are validated against an allow-list before anything runs. If the API is missing or down, the rules coach takes over automatically.
- **Backing band:** per-channel mute and volume, master level, click track, optional start in sync with the teacher's bar 1, and the band follows the selected section.
- **Real recorded instruments** (`assets/samples/`, about 1.4 MB, bundled so it works offline). The teacher plays a sampled steel-string acoustic guitar, one recording per semitone from E2 to E6 (FluidR3 GM SoundFont by Frank Wen, CC BY 3.0). Each string sounds one note at a time: a new note on a string fades the previous one out. Notes ring for their written length, then release naturally. Soft upstrokes are quieter and darker. Hammer-ons and pull-offs have no pick attack, and slides glide into pitch. Each note gets a slight random detune and a gentle room reverb. The backing band uses a sampled fingered electric bass from the same soundfont and a real acoustic drum kit (Virtuosity Drums by Versilian Studios, CC0). Samples decode about a second after the page loads; until then, or if a file is missing, a synthesised voice stands in. Sources, licences and edits are in `assets/samples/CREDITS.md`.

**Still illustrative or missing**

- The fretting hand comes from a stylised 3D character, so it is smoother and chunkier than a real hand. There is no on-screen teacher figure; the coach panel shows a mascot until you add teacher artwork.
- Hand poses come from geometry and joint limits, not from motion capture of a real guitarist. Accurate positions don't guarantee perfect technique details such as exact wrist angle or thumb pressure.
- The 3D guitar needs WebGL. Without it, lessons, audio and the finger diagram still work, with a notice instead of the stage.
- The guitar has one sample layer: each note is a single recording, shaped by envelope and filter, not a multi-velocity studio library. Bends and vibrato aren't modelled. Slides are a short pitch glide, and hammer-ons and pull-offs are softer notes without the pick attack.
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
- The 3D character is Mixamo's "Michelle" (via the three.js examples). Mixamo characters are royalty-free to use in projects but may not be redistributed on their own. Replace it with a character you own before a public release; see `assets/models/CREDITS.md`. Three.js is MIT-licensed (`vendor/three`).
- All exercises and backing grooves are original. The guitar and bass sounds are recordings from the FluidR3 GM SoundFont by Frank Wen, licensed [CC BY 3.0](https://creativecommons.org/licenses/by/3.0/us/), which requires attribution. The drums are from Virtuosity Drums by Versilian Studios ([CC0](https://creativecommons.org/publicdomain/zero/1.0/)). Keep `assets/samples/CREDITS.md` and the credit when distributing. The reverb, click and fallback voices are synthesised in code.

## Files

| File | What it does |
| --- | --- |
| `index.html`, `styles.css` | App shell and layout |
| `music.js` | Chord shapes, lessons, sections, capo maths, import validation |
| `performance.js` | Canonical timeline compiler and AudioContext-clocked player |
| `teacher3d.js` | The teacher's guitar: F310 model, fretting hand (IK, human proportions), colour-coded strings and finger numbers, pick synced to the timeline, cameras and strumming inset |
| `assets/teacher/` | Optional teacher artwork for Fret's face (`portrait.webp/png/jpg`) |
| `vendor/three/`, `assets/models/` | Three.js (MIT) and the rigged character |
| `lab/` | Developer test pages for posing the fretting hand (served only when `FRETWISE_LAB=1`) |
| `audio.js` | Sampled guitar, bass and drums (with a synth fallback), room reverb, mixer, backing band, click, pitch detector |
| `assets/samples/` | Bundled instrument recordings, with sources and licences in `CREDITS.md` |
| `tutor.js` | Tutor brain: intents, teaching state machine, memory, migration, action allow-list |
| `app.js` | UI wiring, voice, microphone, memory views, owner mode |
| `server.mjs` | Static server plus optional AI coach (`/api/status`, `/api/chat`) |
| `test.cjs`, `tests/` | Unit, browser and coach tests |
| `CLAUDE_BUILD_PROMPT.md` | The full product specification this build follows |
