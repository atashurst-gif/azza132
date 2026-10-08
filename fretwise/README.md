# Fretwise — AI Guitar Teacher, Prototype 0.1

A self-contained **working browser prototype** for a personalised conversational guitar tutor, seeded with the owner's Yamaha F310 photographs and current learning goals. It includes an extensive [Claude Code implementation prompt](CLAUDE_BUILD_PROMPT.md) to carry the prototype forward.

## Start in one command

From this directory, run:

```bash
python3 -m http.server 8000
```

Visit **http://localhost:8000** in a modern desktop browser, ideally Chrome. No account or package installation is required. Headphones are recommended when using microphone feedback alongside sound playback.

If you already have Node, you can alternatively run `npx serve`, though that may require downloading the package. Do not simply double-click `index.html` if you want microphone permission: browsers usually require HTTPS or localhost.

## Actually implemented

- Yamaha F310 profile seeded from your supplied guitar photos; upload a different reference photo (stored locally).
- Dark, responsive Practice Studio, Tutor, My Guitar, Progress, and owner-only Change the App pages.
- An **illustrative** virtual guitar with animated fretting finger pads and moving picking cue. Guitar plucks generated in Web Audio from chord/note data; note/chord sequence and visual cue advance together.
- Original lessons: capo chord workout, barre-chord stepping stones, G/A chord transitions and simple four-note lick. **These are not reproductions of Sultans of Swing.**
- Capo-aware pitches and displayed sounding chord; manual tempo adjustment; click a step to begin teacher playback from that step.
- On-demand extra fretboard finger dots/diagram, **hidden by default**; optional automatic hints after repeated detected misses in the one-note practice exercise.
- Basic Web Audio accompaniment with individually enabled drums, bass and rhythm guitar.
- Typed conversational commands and a rule-based *demo coach*; browser speech recognition (when supported) and speech synthesis for spoken responses. Teaching preferences and local learning notes persist.
- Microphone **single-note** pitch estimator with basic comparison in the lead-note exercise. It **cannot reliably grade full chord voicings**, finger pressure, bends, or complex polyphonic audio yet.
- Owner Builder Mode queues prompts, supports optional dictation, and exports a change specification for Claude. **It does not autonomously edit source code.**
- User-authorised **JSON exercise import** for original chord or lead-note sequences, plus profile export. Does not include song library or original artist audio.

## Not yet built — important honesty

There is **no real generative AI API connected** yet: the demonstration coach uses a transparent, rules-based command system. The virtual hand is **an illustration, not a rigged human or camera-tracked hand**. Guitar tone and drums are primitive synthesis, not professional recorded stems. Timing/mic analysis requires field testing; note detection may misidentify harmonics. Voice transcription depends on browser support and sometimes network speech services. No real song transcription, real Sultans of Swing playback, licensed catalogue, song stem isolation, live webcam hand analysis, subscriptions, authentication, or automatic code generation/deployment yet.

The prototype does **not** claim that an AI-generated cover makes protected music free of copyright. Read `CLAUDE_BUILD_PROMPT.md` for the music rights and production plan.

## Import an exercise

Under Practice Studio, click **Import your own lesson JSON**. Here is an ORIGINAL four-note practice example (`string`: 0 = low E, 5 = high e; `fret` is relative to the capo):

```json
{
  "name":"My original little lick",
  "kind":"melody",
  "steps":[
    {"string":5,"fret":0,"label":"Open high e"},
    {"string":5,"fret":3,"label":"Fret 3"},
    {"string":5,"fret":5,"label":"Fret 5"},
    {"string":5,"fret":3,"label":"Back to 3"}
  ]
}
```

For chords, `"kind":"chord"` and each `steps` entry is `{ "chord":"Am" }`, `{ "chord":"G" }` etc. Known IDs: Am, A, E, Em, D, Dm, C, G, Fmaj7, Fsmall, Fmini, F.

## Privacy and data

Preferences, teaching notes, song imports and owner change requests are stored via **localStorage** on that browser. There is no backend or cloud sync. The guitar photos included are the three photos provided for this private build; remove or replace them before distributing a commercial build. If localStorage/browser data is cleared, progress is lost; use **Export learning profile** first. No microphone samples are stored by the code.

## Developer notes

- `index.html`: app shells and screens.
- `styles.css`: layout/design.
- `music.js`: lesson/chord data and note/chord semantics.
- `audio.js`: Web Audio plucks/drum/bass synth and mono pitch detection.
- `app.js`: UI, simple demo tutor, saved profile, owner requests.
- `CLAUDE_BUILD_PROMPT.md`: next development specification.

To develop further, open the folder in Claude Code and paste the build prompt, or point Claude Code directly to the prompt document.
