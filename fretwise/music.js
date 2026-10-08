/* Music data and semantics. Frets are listed low E → high e (string index 0 = low E, 5 = high e); -1 means muted.
   Every exercise here is ORIGINAL practice material. Nothing is a transcription of a commercial recording. */
window.FRETWISE_MUSIC = (() => {
  const CHORDS = {
    Am:{frets:[-1,0,2,2,1,0],fingers:[0,0,2,3,1,0],name:'Am'},
    A:{frets:[-1,0,2,2,2,0],fingers:[0,0,1,2,3,0],name:'A'},
    E:{frets:[0,2,2,1,0,0],fingers:[0,2,3,1,0,0],name:'E'},
    Em:{frets:[0,2,2,0,0,0],fingers:[0,2,3,0,0,0],name:'Em'},
    D:{frets:[-1,-1,0,2,3,2],fingers:[0,0,0,1,3,2],name:'D'},
    Dm:{frets:[-1,-1,0,2,3,1],fingers:[0,0,0,2,3,1],name:'Dm'},
    C:{frets:[-1,3,2,0,1,0],fingers:[0,3,2,0,1,0],name:'C'},
    G:{frets:[3,2,0,0,0,3],fingers:[2,1,0,0,0,3],name:'G'},
    Fmaj7:{frets:[-1,-1,3,2,1,0],fingers:[0,0,3,2,1,0],name:'Fmaj7'},
    Fsmall:{frets:[-1,-1,3,2,1,-1],fingers:[0,0,3,2,1,0],name:'F (3 strings)'},
    Fmini:{frets:[-1,-1,3,2,1,1],fingers:[0,0,3,2,1,1],name:'F (mini barre)'},
    F:{frets:[1,3,3,2,1,1],fingers:[1,3,4,2,1,1],name:'F (full barre)'}
  };
  // Strum patterns are expressed in beats within a 4/4 bar. 'D' = downstroke, 'U' = upstroke.
  const STRUM_PATTERNS = {
    simple:[{b:0,d:'D'},{b:1,d:'D'},{b:2,d:'D'},{b:3,d:'D'}],
    basic:[{b:0,d:'D'},{b:1,d:'D'},{b:1.5,d:'U'},{b:2.5,d:'U'},{b:3,d:'D'},{b:3.5,d:'U'}],
    once:[{b:0,d:'D'}]
  };
  const TECHNIQUES = ['pluck','hammer','pull','slide','rest'];
  const LESSONS = [
    {id:'capo',name:'Capo 5 · chord workout',title:'Capo chord changes',kind:'chord',pattern:'basic',description:'An ORIGINAL chord workout, not a transcription of Sultans of Swing. Practise familiar shapes in the key created by your capo.',
      steps:[{chord:'Am',label:'Am shape'},{chord:'G',label:'G shape'},{chord:'C',label:'C shape'},{chord:'Em',label:'Em shape'}],
      sections:[{name:'Bars 1–2 · Am to G',from:0,to:1},{name:'Bars 3–4 · C to Em',from:2,to:3}]},
    {id:'barre',name:'Your first F barre chord',title:'A gentler route to F',kind:'chord',pattern:'simple',description:'Build slowly, starting with the smaller shapes rather than fighting all six strings.',
      steps:[{chord:'Fmaj7',label:'Fmaj7'},{chord:'Fsmall',label:'Small F'},{chord:'Fmini',label:'Mini barre'},{chord:'F',label:'Full F'}],
      sections:[{name:'Easy shapes · Fmaj7 and small F',from:0,to:1},{name:'Barre shapes · mini then full',from:2,to:3}]},
    {id:'changes',name:'G / A chord changes',title:'Make those changes clean',kind:'chord',pattern:'basic',description:'An original four-bar exercise using chord shapes you already know.',
      steps:[{chord:'G',label:'G'},{chord:'D',label:'D'},{chord:'A',label:'A'},{chord:'E',label:'E'}],
      sections:[{name:'Bars 1–2 · G to D',from:0,to:1},{name:'Bars 3–4 · A to E',from:2,to:3}]},
    {id:'lead',name:'Four-note lead guitar lick',title:'Copy this little lick',kind:'melody',description:'An original four-note demonstration to test note-by-note teaching and microphone feedback.',
      steps:[{string:5,fret:0,label:'Open high e'},{string:5,fret:3,label:'High e · fret 3'},{string:5,fret:5,label:'High e · fret 5'},{string:5,fret:3,label:'High e · fret 3'}],
      sections:[{name:'First two notes',from:0,to:1},{name:'Last two notes',from:2,to:3}]}
  ];
  const NOTES=['C','C♯','D','D♯','E','F','F♯','G','G♯','A','A♯','B'];
  const STRING_MIDI=[40,45,50,55,59,64];
  const STRING_NAMES=['E','A','D','G','B','e'];
  const ROOT={'C':0,'C#':1,'Db':1,'D':2,'D#':3,'Eb':3,'E':4,'F':5,'F#':6,'Gb':6,'G':7,'G#':8,'Ab':8,'A':9,'A#':10,'Bb':10,'B':11};
  const FLAT_NOTES=['C','Db','D','Eb','E','F','Gb','G','Ab','A','Bb','B'];
  function chordSound(name,capo){ const display=CHORDS[name]?CHORDS[name].name:String(name); const m=display.match(/^([A-G](?:#|b)?)(.*)$/); if(!m) return display; const root=ROOT[m[1]]; if(root===undefined)return display;return FLAT_NOTES[(root+Number(capo)+12)%12]+m[2]; }
  function stepFrets(step) { if(step.chord) return CHORDS[step.chord]?.frets||[-1,-1,-1,-1,-1,-1];const a=[-1,-1,-1,-1,-1,-1];if(step.technique==='rest')return a;a[step.string]=step.fret;return a; }
  function stepFingers(step,finger) {if(step.chord)return CHORDS[step.chord]?.fingers||[0,0,0,0,0,0];const a=[0,0,0,0,0,0];if(step.technique==='rest')return a;a[step.string]=step.fret===0?0:(finger||step.finger||1);return a;}
  function stepMidis(step,capo) {return stepFrets(step).map((f,i)=>f<0?null:STRING_MIDI[i]+f+Number(capo)).filter(x=>x!==null);}
  function midiToName(midi){return NOTES[((midi%12)+12)%12]+(Math.floor(midi/12)-1);}
  function freqToMidi(freq){return 69+12*Math.log2(freq/440);}
  function midiToFreq(midi){return 440*Math.pow(2,(midi-69)/12);}
  function getExercise(id){return LESSONS.find(x=>x.id===id)||LESSONS[0];}
  // Lessons without authored sections (e.g. imports) get phrase-sized sections so they can still be isolated and looped.
  function sectionsFor(lesson){
    if(lesson.sections&&lesson.sections.length) return lesson.sections;
    const n=lesson.steps.length; if(n<2) return [];
    const size=n<=8?2:4; const out=[];
    for(let from=0;from<n;from+=size){ const to=Math.min(n-1,from+size-1); out.push({name:from===to?'Step '+(from+1):'Steps '+(from+1)+'–'+(to+1),from,to,auto:true}); }
    return out;
  }
  // A barre is finger 1 pressing two or more strings at the same fret.
  function barreFor(frets,fingers){const idx=[];fingers.forEach((f,i)=>{if(f===1&&frets[i]>0)idx.push(i);});if(idx.length<2)return null;const fret=frets[idx[0]];if(!idx.every(i=>frets[i]===fret))return null;return {fret,from:Math.min(...idx),to:Math.max(...idx)};}
  // Importing JSON validates shape and size. It does not confer permission to reproduce any song.
  function validateImportedExercise(raw){
    if(!raw || typeof raw!=='object' || !Array.isArray(raw.steps) || raw.steps.length<1 || raw.steps.length>128)throw Error('Provide 1–128 exercise steps.');
    const kind=raw.kind==='melody'?'melody':'chord';
    const cleaned={id:'import-'+Date.now(), name:String(raw.name||'My imported arrangement').slice(0,70),title:String(raw.title||raw.name||'Imported arrangement').slice(0,70),kind,description:'User-imported material. Confirm that you have the rights needed to use it.',steps:[],imported:true,source:String(raw.source||'user upload').slice(0,120)};
    if(kind==='chord'&&raw.pattern!==undefined){if(!STRUM_PATTERNS[raw.pattern])throw Error('Unknown strum pattern '+raw.pattern+'. Use one of: '+Object.keys(STRUM_PATTERNS).join(', '));cleaned.pattern=raw.pattern;}
    if(raw.tempo!==undefined){if(!Number.isFinite(raw.tempo)||raw.tempo<40||raw.tempo>200)throw Error('Tempo must be 40–200 BPM.');cleaned.tempo=Math.round(raw.tempo);}
    for(const step of raw.steps){
      if(!step||typeof step!=='object')throw Error('Each step must be an object.');
      const out={label:String(step.label||'').slice(0,30)};
      if(step.beats!==undefined){if(!Number.isFinite(step.beats)||step.beats<0.25||step.beats>8)throw Error('Step beats must be between 0.25 and 8.');out.beats=step.beats;}
      if(kind==='chord'){if(!CHORDS[step.chord])throw Error('Unknown chord '+step.chord);out.chord=step.chord;out.label=out.label||step.chord;}
      else{
        if(step.technique!==undefined){if(!TECHNIQUES.includes(step.technique))throw Error('Unknown technique '+step.technique+'. Use one of: '+TECHNIQUES.join(', '));out.technique=step.technique;}
        if(out.technique!=='rest'){
          if(!Number.isInteger(step.string)||step.string<0||step.string>5||!Number.isInteger(step.fret)||step.fret<0||step.fret>12)throw Error('Melody notes require string 0–5 and fret 0–12.');
          out.string=step.string;out.fret=step.fret;
          if(step.finger!==undefined){if(!Number.isInteger(step.finger)||step.finger<0||step.finger>4)throw Error('Finger must be 0–4.');out.finger=step.finger;}
        }
        out.label=out.label||(out.technique==='rest'?'Rest':'Note');
      }
      cleaned.steps.push(out);
    }
    if(raw.sections!==undefined){
      if(!Array.isArray(raw.sections)||raw.sections.length>32)throw Error('Provide at most 32 sections.');
      cleaned.sections=raw.sections.map(s=>{if(!s||!Number.isInteger(s.from)||!Number.isInteger(s.to)||s.from<0||s.to>=cleaned.steps.length||s.from>s.to)throw Error('Each section needs from/to step indexes inside the lesson.');return {name:String(s.name||('Steps '+(s.from+1)+'–'+(s.to+1))).slice(0,40),from:s.from,to:s.to};});
    }
    return cleaned;
  }
  return {CHORDS,LESSONS,STRUM_PATTERNS,TECHNIQUES,STRING_MIDI,STRING_NAMES,chordSound,stepFrets,stepFingers,stepMidis,midiToName,freqToMidi,midiToFreq,getExercise,barreFor,sectionsFor,validateImportedExercise};
})();
