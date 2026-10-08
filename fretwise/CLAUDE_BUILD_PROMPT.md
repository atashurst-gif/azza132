# FRETWISE — MASTER CLAUDE CODE BUILD PROMPT

You are a senior full-stack engineer, audio-DSP engineer, music-education product designer, and pragmatic technical lead. **Build a real, working, testable, iterative AI guitar tutor called Fretwise.** Do not return only concept screens or a high-level plan. Start from the accompanying runnable vanilla-JS prototype in this folder. Inspect what is present, run it, and make working improvements in small increments. Prioritise accurate demonstrations, responsive interaction, and honest feedback over glossy but non-functional features. Explain implementation in ordinary British English.

## 0. Non-negotiable product definition

Fretwise is an interactive AI guitar teacher designed first for **one actual learner**, then for eventual commercial subscriptions. The learner plays a six-string steel-string **Yamaha F310 acoustic**, mostly in standard tuning. Their guitar photos are under `assets/`. Its fretboard has ordinary dot markers, including double dots at fret 12. The app should recognise/save a learner's exact instrument and capo, and teach according to physical fret positions as well as sounding pitches.

The learner describes themselves as **advanced beginner**. They know Am, E, Em, D, Dm, Fmaj7, C; are less consistent at G and A; cannot comfortably play full barre chords; and get overwhelmed when explanations are too long. Their favourite current challenge is **Dire Straits — “Sultans of Swing”**, often practising an easier personal arrangement with capo 3 or 5. Treat this as a *learning goal*, not as a licence to copy the song or invent an inaccurate transcription.

The main experience should feel like learning beside a patient, skilful guitar tutor. The learner talks or types freely, the tutor answers by voice, visually **PLAYS its own virtual guitar** with synchronised guitar audio, can isolate any supported passage and repeat it, then listens to the learner and adjusts. It remembers what teaching approaches work, not just which chords they know.

### CRUCIAL INTERACTION RULE: tutor guitar FIRST, hint dots SECOND

By default show the tutor's demonstrative virtual guitar and plausibly articulated fretting/picking **hands**. The user should HEAR the correctly played part and SEE HOW it is played; don't replace this with dots on a static neck. Keep extra glowing fretboard dots / close-up labelled fingering hidden by default. Show them **only** when the learner explicitly asks (“show me finger placement”, “which fret?”, “show dots”) or after repeated, reliably detected difficulty if the user has enabled auto-help. Let the learner dismiss them. Avoid unsolicited dense theory and constant instructional overlays.

The current prototype has an illustrative hand animation and algorithmically synthesised plucks, not a photorealistic or human-accurate 3D performer. Evolve to a synchronised physically credible virtual instrument and hand rig; never claim the existing graphic is already biomechanically accurate. Correct **left-hand and right-hand articulation, fret, string, audio pitch, capo transposition and note timing** must all come from ONE canonical event timeline (do not allow audio and animations to disagree).

## 1. UX and core screens

Preserve and improve these starting pages:

1. **Practice Studio**: large virtual guitar player/close-up hand view; current song/exercise, section/bar locator, play/watch/repeat, tempo, capo, start/pause/stop, A–B loop, teacher playback, backing band and optional hint overlay. Allow phrase-level selection and “Is this the section?” confirmation before coaching it.
2. **Your Tutor**: personable, optional mascot avatar, natural **two-way voice AND typed messages**, with push-to-talk and playback interruption. Coach can stop and explain a note, demonstrate again, and resume. Playback should duck/pause automatically when conversing where appropriate.
3. **My Guitar**: photo/camera-guided instrument setup; save acoustic/electric, number of strings, tuning, capo, body, handedness, unique markers, and measured fret calibration. The included photos establish the Yamaha model; do not pretend automatic 3D hand tracking already exists.
4. **Learning Profile / Progress**: known chords, struggles, lesson history, observable improvements and teaching preferences; learner can inspect and correct stored assumptions.
5. **Owner Builder Mode**: voice or text requests to edit the **software**, with classification of (a) teaching preference, (b) remembered user fact, or (c) feature/code change. Code changes go into a reviewed queue, tests/preview, and explicit approve + rollback; **never let arbitrary spoken phrases self-deploy code or reveal secrets**.
6. **Backing Band Studio**: individually muting drums, bass, rhythm guitar and lead demonstration; optional count-in, volume sliders, realistic sound, tempo control, looping, click track, clear sync with main song timeline.

The design should be premium, uncluttered, warm, accessible, mobile/tablet responsive, suitable for dark evening practice. Current colours and assets in the prototype are starting points, not constraints.

## 2. Teaching intelligence — what matters most

Keep separate memory tracks:

- **Playing state:** capabilities, tunings, actual pitch/rhythm measurements, comfort constraints, active song, difficult chords/sections, progress and confidence.
- **Teaching state:** instruction length, examples that work, confusion triggers, pace, demonstration vs explanation preference, when to offer visual guides, favourite songs and learning motivation.
- **Owner change requests:** instructions about product behaviour or code; never silently treat these as lesson content.

Adapt incrementally as evidence accumulates. Examples:

- “I'm overwhelmed / that's gone in one ear and out of the other” -> pause; **one instruction only**; offer a visual demonstration; wait for the student to try; avoid long paragraphs.
- “I can't make the barre chord ring, my fingers won't stretch” -> show safe easier F chord voicings, such as `xx3210` Fmaj7 then `xx321x` F triad, then a small F shape `xx3211`, before trying full F `133211`; check wrist/thumb, fingertip location close behind the fret, pressure, technique and possible overly-high action on the Yamaha. NEVER encourage playing through pain or claim to assess precise finger pressure from a microphone.
- “No, that's the wrong part” -> let learner move earlier/later, identify a musical section by playback, and confirm before practising. If catalogued reference data is absent, admit that rather than inventing notes.
- “Explain that in plain English” -> replace jargon with practical hand movements, a single instruction and optional audio/visual example.
- “That way of teaching works” -> save preference, but do not stereotype permanently; periodically test effectiveness and let learner override it.

Implement a repeatable teaching state machine: `identify goal → choose tiny passage → play accurate demonstration → wait for attempt → observe reliable metrics → identify one important issue or acknowledge uncertainty → give one concrete corrective action → retry → update memory → advance only when ready`. Provide conversation interruption at any point.

## 3. Song data, virtual performance, rights

**Do not let a general LLM hallucinate notes, chords, fingering or timings.** Every playable song passage needs a validated musical source with ordered timed events: instrument, note pitch, duration, velocity, string/fret/finger choices, capo, technique (bend with target cents, slide, vibrato, hammer/pull, rest), tempo changes, bars/sections, version provenance and permissions. Audio samples, animated hands, fret lighting and scrolling tab must all derive from this exact same event stream.

Support **legally obtained** structured notation / user-provided arrangements in a private practice workflow; build import adapters for sensible formats such as MusicXML, MIDI or Guitar Pro where libraries and file permissions allow. Validate imported data and provide a human review path. Never assume user uploads automatically grant public reproduction or commercial rights.

For accurate well-known songs, plan a rights-cleared catalog with appropriate composition/arrangement permissions, and sound-recording permissions where original recordings are used. An AI cover/re-recording of a copyrighted song is **not** automatically rights-free; do not design a copyright evasion system or train on ripped catalogue recordings without proper authorisation. Explore music-publisher agreements and rights-managed content vendors in the UK. Until licences are in place, ship original exercises, public-domain compositions whose actual status is verified, properly licensed arrangements or explicitly authorised material. A backing track 'in the general style of' a genre must be independently created and not deceptively reproduce a particular protected song.

**Demonstration playback** is essential:
- virtual acoustic guitar sound sufficiently realistic for practice (use licensed sample library or an authorised synthesis engine), with true articulated performances for slides, hammer-ons, bends, vibrato and fingerpicking;
- visible LEFT hand and picking RIGHT hand, with optional close-up, slow motion, adjustable camera angle, and reset/repeat last 2–4 notes;
- user-selectable section with accurate audio matching original arrangement if authorised;
- practice tempo may change without incorrectly shifting pitch; optionally isolate guitar, drum, bass and rhythm stems when legal stems exist; backing generated from owned MIDI events is not the original record;
- capo-aware `played shape` vs `sounding chord`; capo 5: Am → Dm, G → C, C → F, etc.

## 4. Microphone / camera engineering

- Browser audio input over HTTPS/localhost with explicit permission; persistent mic status and safe stop. Headphones encouraged when playback and listening run together.
- For v1 use tested monophonic pitch detection + onset/rhythm estimation on isolated notes, and **confidence reporting**. The current app only estimates monophonic pitch; **do not claim it can identify exact six-string chord voicing or hand position**.
- Add polyphonic chord detection only after ground-truth testing on actual acoustic-guitar recordings. Account for mic quality, dynamics, background sound, tunings, capo, and room noise.
- Finger/wrist camera feedback is a separate later capability requiring careful calibration and uncertainty: don't diagnose physical causes from audio alone. If hands hurt, stop and suggest adjusting technique or consulting a professional / luthier as appropriate.
- Real-time voice conversations must not corrupt pitch classification; either separate input modalities in the pipeline or correctly gate during speech and backing playback. Reduce echo/latency and report unsupported devices honestly.

## 5. Recommended production architecture

You may refactor from the vanilla no-dependency prototype into a React + TypeScript app (Vite/Next depending on architecture) with WebAudio/AudioWorklet for low-latency music playback, a carefully selected notation player, deterministic musical event engine and a modular 2D/3D animated guitar component. For 3D, assess Three.js / React Three Fiber + rigged guitar/hands, but **don't spend the entire MVP budget on photorealism** if finger timing is still inaccurate. Introduce a server-side conversation adapter to an AI model for teaching, grounded in the user's authorised musical data and memory, with voice streaming/STT/TTS where available. Keep API keys exclusively on the server. Do not invent support for a provider API: verify current SDK documentation in the development environment.

Recommended services/models are implementation decisions, not hard dependencies: speech recognition/STT, TTS, LLM, protected account storage (for later subscriptions). For v1, offer a local-demo mode when no API keys are configured. Typed interaction MUST always work if speech services are unavailable.

Use event schemas and typed contracts between lesson engine, note playback, animations and teaching LLM. Prefer local persistent memory initially; migrate to secure authenticated backend only when warranted. Provide GDPR-conscious controls for mic/audio recordings, export and delete. No recording retention without explicit user choice. Separate owner privileges from general learners before public launch. Treat user prompts as untrusted input; never execute arbitrary code from chat.

## 6. Build in phases — ACTUALLY IMPLEMENT

**Phase A — solidify the existing working app**
- Inspect files and run the included smoke checks. Repair defects. Establish a test runner, lint/type safety as appropriate, browser smoke tests, CI and a simple one-command startup.
- Fix sound/animation synchronization, play/stop/pause, tempo/capo edge cases, drag/selection, mobile responsiveness, accessible controls. Original notes/chord exercises must be musically correct.
- Add accurate note timing visualisation and realistic sample playback with clear source licensing. Add controllable per-instrument backing (with counted-in start, reliable loops and metronome).

**Phase B — meaningful tutor conversations**
- Add working configurable server-side LLM/STT/TTS adapters (with fallback to demo mode). Keep voice and keyboard input. Teach with simple short replies first; interruptions, confirmations and memory updates are testable.
- Evaluate user commands for (1) pedagogical correction (2) tutor preferences/memory (3) owner software change. Test no accidental self-edit.

**Phase C — reliable virtual guitar teacher**
- Move from the current illustrative CSS fingertips to believable, coordinated fretting and picking animations driven by a canonical performance event timeline. You can use a licensed rig/animation asset if appropriately sourced. Default hand demonstration; explicit/struggle-triggered fret dots only.
- Implement visible section selection, note/phrase looping, stop/step/back/repeat, and 'is this the part?' playback confirmation.

**Phase D — personal learner and song workflows**
- Build initial skill assessment, session memory, guitar picture/capo setup, note-by-note feedback loop and user-authorised notation imports. Add real tests that distinguish reliable detection from unknown/uncertain.
- Offer initial commercially usable exercises created by us, without copyrighted song transcripts.

**Phase E — beta for others**
- Authentication, learner profiles, privacy management, permissions for recordings, subscription readiness, rights-managed content catalog, usage metering and accessibility review, only after core teaching works.

## 7. Acceptance tests

Demonstrate these journeys in a real browser:

1. Open Fretwise and see the teacher’s playable guitar. Extra fret dots are hidden by default.
2. Select a chord workout at capo 5, click Play, hear pitches transposed exactly five semitones, watch correct fretting and picking in time, stop/repeat/slow it.
3. Say or type “This explanation is overwhelming; just one step please”. Tutor changes behaviour, remembers this after restart, and does not send another long explanation.
4. Ask for an F barre chord with limited stretch. Tutor proposes a safe sequence from Fmaj7 to a smaller F triad/mini-barre, with correct finger positions and a concise demonstration.
5. Say “Which finger was that? Show me the dots.” Extra diagram appears; it can be hidden immediately.
6. Start original drum/bass backing, mute a channel, alter tempo, and play over it. Headphone guidance appears; backing stops cleanly.
7. Ask for an exact Sultans of Swing lick without licensed/accurate note data. Tutor admits unavailable source and offers import/authorised source; **must not hallucinate a note sequence and pass it off as that lick**.
8. Import an original/user-authorised notated exercise, select bar/phrase, confirm by hearing playback, isolate it and repeat.
9. Dictate “Add a loop-last-four-notes button” in Owner Mode; saves a change request, but NO self-modifying code is shipped without an explicit review/approve pipeline.
10. Guitar profile persists; browser permission errors and API outages degrade gracefully to usable typed lessons.

## 8. Deliverables and collaboration protocol

1. Give a short current-state inventory: which prototype features are truly operational and which are illustrations or placeholders.
2. Propose **the very next smallest shippable set of changes**, then implement it. Ask questions only where they block implementation; sensible defaults otherwise.
3. Produce **working source files**, a run command, minimal environment setup (`.env.example` if server providers used), a short README, tests, and any temporary/asset licensing notes.
4. Run lint/tests/browser checks, show results, and name any failing/untested features. Avoid claiming success from unrun tests.
5. Preserve user-generated profile and changes across code upgrades via migrations; don't overwrite the local learning history.
6. Before large architectural changes, write a 5–8 line summary of trade-offs and get owner approval. Otherwise keep building.

**Begin now by opening this folder, running the prototype locally, and improving the most important user journey: tutor guitar demonstration → user interruption/question → precise, one-step explanation → optional hints → repeat.** Do not stop after producing mockups or a plan.
