# Audio sample credits

Fretwise bundles these recordings so the app works offline. `audio.js` loads them; if any file is missing, that note falls back to the built-in synthesised voice.

## Steel-string acoustic guitar: `guitar-steel/`

- **Files:** 49 notes from E2 (MIDI 40) to E6 (MIDI 88), one per semitone. File names use flats (`Bb2.mp3`, `Db3.mp3`, …).
- **Source:** `FluidR3_GM/acoustic_guitar_steel-mp3/` from [gleitz/midi-js-soundfonts](https://github.com/gleitz/midi-js-soundfonts) (gh-pages branch), downloaded from `https://raw.githubusercontent.com/gleitz/midi-js-soundfonts/gh-pages/FluidR3_GM/acoustic_guitar_steel-mp3/<note>.mp3`.
- **Author:** Frank Wen, *Fluid (R3) General MIDI SoundFont* (FluidR3_GM.sf2). Rendered to per-note MP3 files by Benjamin Gleitzman (gleitz/midi-js-soundfonts).
- **Licence:** [Creative Commons Attribution 3.0](https://creativecommons.org/licenses/by/3.0/us/) (CC BY 3.0), as stated by the source repository.
- **Changes:** The files are byte-for-byte copies; only the notes Fretwise can play were taken. When the app runs, `audio.js` trims each file's lead-in to 1 ms before the attack, keeps the first 2.4 s with a 50 ms fade, and uses one channel, because the files are identical in both channels. It then adds its own envelope (let-ring and release), velocity filtering, ±3 cent detune, pitch glides for slides and a synthetic room reverb.

## Bass: `bass/`

- **Files:** 28 notes from E1 (MIDI 28) to G3 (MIDI 55), named as above.
- **Source:** `FluidR3_GM/electric_bass_finger-mp3/` from [gleitz/midi-js-soundfonts](https://github.com/gleitz/midi-js-soundfonts) (gh-pages branch).
- **Author:** Frank Wen (FluidR3_GM.sf2), rendered by Benjamin Gleitzman.
- **Licence:** [CC BY 3.0](https://creativecommons.org/licenses/by/3.0/us/).
- **Changes:** The files are byte-for-byte copies. When the app runs, it trims the lead-in, keeps the first 1.8 s and uses one channel. It also applies an envelope, a low-pass filter and one-note-at-a-time playing.

## Drum kit: `drums/`

- **Files:** `kick-1.mp3`, `kick-2.mp3` (bass drum, two round-robin hits), `snare-1.mp3`, `snare-2.mp3` (snare, centre hits), `hat-1.mp3`, `hat-2.mp3`, `hat-3.mp3` (closed hi-hat; `hat-3` is the accent).
- **Source:** *Virtuosity Drums* by Versilian Studios. Drummer Austin McMahon played the house kit at Virtuosity Musical Instruments, Boston. The SFZ edition is at [sfzinstruments/virtuosity_drums](https://github.com/sfzinstruments/virtuosity_drums) (master branch, `Samples/`).
- **Licence:** [CC0 1.0 Universal](https://creativecommons.org/publicdomain/zero/1.0/) (public-domain dedication; no attribution required, credited anyway).
- **Original files:** `Samples/{kickmic,snaremic,oh,room}/…`:
  - `kick/<mic>_kick_snon_vl3_rr1.flac` and `…_rr2.flac` became `kick-1` and `kick-2`
  - `snare/<mic>_snare_center_vl20.flac` and `…_vl22.flac` became `snare-1` and `snare-2`
  - `hh/<mic>_hh_closed_vl2_rr1.flac`, `…_vl2_rr2.flac` and `…_vl3_rr1.flac` became `hat-1`, `hat-2` and `hat-3`
- **Changes:** For each hit, the kick-drum mic, snare mic and stereo overheads (full level) and the stereo room mics (half level) were mixed into one stereo file, as the library's "basic kit" combines them. Each mix was trimmed to 0.9 s (kick), 1.0 s (snare) or 0.45 s (hi-hat), faded out over its last 40%, resampled from 48 kHz to 44.1 kHz, and peak-normalised per instrument. Hits of the same instrument share one gain, so their dynamics are kept. The result was encoded as 128 kb/s MP3 with ffmpeg/LAME. Command used for each hit (`<hit>` = e.g. `kick_snon_vl3_rr1`):

  ```sh
  ffmpeg -i kickmic_<hit>.flac -i snaremic_<hit>.flac -i oh_<hit>.flac -i room_<hit>.flac -filter_complex \
    "[0]aformat=channel_layouts=stereo[a];[1]aformat=channel_layouts=stereo[b];[2]aformat=channel_layouts=stereo[c];[3]aformat=channel_layouts=stereo[d];\
     [a][b][c][d]amix=inputs=4:weights=1 1 1 0.5:normalize=0,atrim=0:<len>,afade=t=out:st=<0.6*len>:d=<0.4*len>,aresample=44100,volume=<gain>dB" \
    -c:a libmp3lame -b:a 128k <name>.mp3
  ```

  The gains were +7.6 dB (kick), +8.9 dB (snare) and +11.8 dB (hi-hat).

## Attribution text

Use this wherever the app's credits are shown:

> Guitar and bass samples: Fluid R3 GM SoundFont by Frank Wen, rendered by Benjamin Gleitzman (midi-js-soundfonts), licensed CC BY 3.0. Drum samples: Virtuosity Drums by Versilian Studios (CC0).

The room reverb, click track and fallback voices are synthesised in code.
