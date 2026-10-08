/* Music data: frets are low E → high e, -1 indicates muted. All exercises below are original. */
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
  const LESSONS = [
    {id:'capo',name:'Capo 5 · chord workout',title:'Capo chord changes',kind:'chord',description:'An ORIGINAL chord workout, not a transcription of Sultans of Swing. Practise familiar shapes in the key created by your capo.',steps:[{chord:'Am',label:'Am shape'},{chord:'G',label:'G shape'},{chord:'C',label:'C shape'},{chord:'Em',label:'Em shape'}]},
    {id:'barre',name:'Your first F barre chord',title:'A gentler route to F',kind:'chord',description:'Build slowly, starting with the smaller shapes rather than fighting all six strings.',steps:[{chord:'Fmaj7',label:'Fmaj7'},{chord:'Fsmall',label:'Small F'},{chord:'Fmini',label:'Mini barre'},{chord:'F',label:'Full F'}]},
    {id:'changes',name:'G / A chord changes',title:'Make those changes clean',kind:'chord',description:'An original four-bar exercise using chord shapes you already know.',steps:[{chord:'G',label:'G'},{chord:'D',label:'D'},{chord:'A',label:'A'},{chord:'E',label:'E'}]},
    {id:'lead',name:'Four-note lead guitar lick',title:'Copy this little lick',kind:'melody',description:'An original four-note demonstration to test note-by-note teaching and microphone feedback.',steps:[{string:5,fret:0,label:'Open high e'},{string:5,fret:3,label:'High e · fret 3'},{string:5,fret:5,label:'High e · fret 5'},{string:5,fret:3,label:'High e · fret 3'}]}
  ];
  const NOTES=['C','C♯','D','D♯','E','F','F♯','G','G♯','A','A♯','B'];
  const STRING_MIDI=[40,45,50,55,59,64];
  const ROOT={'C':0,'C#':1,'Db':1,'D':2,'D#':3,'Eb':3,'E':4,'F':5,'F#':6,'Gb':6,'G':7,'G#':8,'Ab':8,'A':9,'A#':10,'Bb':10,'B':11};
  const FLAT_NOTES=['C','Db','D','Eb','E','F','Gb','G','Ab','A','Bb','B'];
  function chordSound(name,capo){ const m=String(name).match(/^([A-G](?:#|b)?)(.*)$/); if(!m) return name; const root=ROOT[m[1]]; if(root===undefined)return name;return FLAT_NOTES[(root+Number(capo)+12)%12]+m[2]; }
  function stepFrets(step) { if(step.chord) return CHORDS[step.chord]?.frets||[-1,-1,-1,-1,-1,-1];const a=[-1,-1,-1,-1,-1,-1];a[step.string]=step.fret;return a; }
  function stepFingers(step) {if(step.chord)return CHORDS[step.chord]?.fingers||[0,0,0,0,0,0];const a=[0,0,0,0,0,0];a[step.string]=step.fret===0?0:1;return a;}
  function stepMidis(step,capo) {return stepFrets(step).map((f,i)=>f<0?null:STRING_MIDI[i]+f+Number(capo)).filter(x=>x!==null);}
  function midiToName(midi){return NOTES[((midi%12)+12)%12]+(Math.floor(midi/12)-1);}
  function freqToMidi(freq){return 69+12*Math.log2(freq/440);}
  function midiToFreq(midi){return 440*Math.pow(2,(midi-69)/12);}
  function getExercise(id){return LESSONS.find(x=>x.id===id)||LESSONS[0];}
  // Importing JSON validates shape and size. It does not confer permission to reproduce any song.
  function validateImportedExercise(raw){
    if(!raw || typeof raw!=='object' || !Array.isArray(raw.steps) || raw.steps.length<1 || raw.steps.length>128)throw Error('Provide 1–128 exercise steps.');
    const cleaned={id:'import-'+Date.now(), name:String(raw.name||'My imported arrangement').slice(0,70),title:String(raw.title||raw.name||'Imported arrangement').slice(0,70),kind:raw.kind==='melody'?'melody':'chord',description:'User-imported material. Confirm that you have the rights needed to use it.',steps:[]};
    for(const step of raw.steps){if(cleaned.kind==='chord'){if(!CHORDS[step.chord])throw Error('Unknown chord '+step.chord);cleaned.steps.push({chord:step.chord,label:String(step.label||step.chord).slice(0,30)});}else{if(!Number.isInteger(step.string)||step.string<0||step.string>5||!Number.isInteger(step.fret)||step.fret<0||step.fret>12)throw Error('Melody notes require string 0–5 and fret 0–12.');cleaned.steps.push({string:step.string,fret:step.fret,label:String(step.label||'Note').slice(0,30)});}}return cleaned;
  }
  return {CHORDS,LESSONS,STRING_MIDI,chordSound,stepFrets,stepFingers,stepMidis,midiToName,freqToMidi,midiToFreq,getExercise,validateImportedExercise};
})();
