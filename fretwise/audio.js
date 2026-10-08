/* Native Web Audio: a plucked-string model for the teacher's guitar, a small synthesised backing band, a click,
   a mixer with per-channel levels, and a conservative monophonic pitch estimator for the microphone.
   Every note is scheduled against AudioContext time so sound lines up with the performance timeline. */
window.FRETWISE_AUDIO = (()=>{
  let ctx, backingTimer=null, backingNext=0, backingBeat=0, micStream=null, micAnalyser=null, micSource=null, micInterval=null;
  const sounding=new Set();
  const levels={master:1,guitar:1,drums:.9,bass:.9,rhythm:.8,click:.7};
  const nodes={};
  const ensure=()=>{const AC=window.AudioContext||window.webkitAudioContext;if(!AC)throw Error('Your browser does not support Web Audio.');if(!ctx){ctx=new AC();nodes.master=ctx.createGain();nodes.master.gain.value=levels.master;nodes.master.connect(ctx.destination);for(const k of ['guitar','drums','bass','rhythm','click']){nodes[k]=ctx.createGain();nodes[k].gain.value=levels[k];nodes[k].connect(nodes.master);}}if(ctx.state==='suspended')ctx.resume();return ctx;};
  function setLevel(name,value){levels[name]=Math.max(0,Math.min(1,Number(value)));if(nodes[name]&&ctx)nodes[name].gain.setTargetAtTime(levels[name],ctx.currentTime,.02);}
  // Karplus–Strong style string: short noise burst into a damped delay line. Longer sustain for lower strings.
  function pluck(midi,when,volume=.32,meta={}){ensure();const freq=FRETWISE_MUSIC.midiToFreq(midi),sample=ctx.sampleRate,period=Math.max(2,Math.floor(sample/freq)),seconds=Math.min(1.8,Math.max(.6,(meta.dur||1.2)+.3)),length=Math.floor(sample*seconds),buffer=ctx.createBuffer(1,length,sample),data=buffer.getChannelData(0);const bright=meta.technique==='hammer'||meta.technique==='pull'?.35:.55;const damping=midi<50?.9992:midi<60?.9988:.9984;for(let i=0;i<period;i++)data[i]=(Math.random()*2-1)*(i<period*bright?1:.5);for(let i=period;i<length;i++)data[i]=((data[i-period]+data[i-period+1])*0.5)*damping;const source=ctx.createBufferSource();source.buffer=buffer;const gain=ctx.createGain();const at=Math.max(when,ctx.currentTime);gain.gain.setValueAtTime(.0001,at);gain.gain.exponentialRampToValueAtTime(Math.max(.0002,volume),at+.008);gain.gain.setValueAtTime(Math.max(.0002,volume),at+.02);gain.gain.exponentialRampToValueAtTime(.0001,at+seconds-.02);source.connect(gain);gain.connect(meta.channel==='rhythm'?nodes.rhythm:nodes.guitar);source.onended=()=>sounding.delete(source);sounding.add(source);source.start(at);source.stop(at+seconds);return source;}
  const guitar=(midi,when,volume)=>pluck(midi,when,volume,{});
  function chord(step,capo,when,volume=.18,channel='guitar'){const frets=FRETWISE_MUSIC.stepFrets(step);let k=0;frets.forEach((f,s)=>{if(f<0)return;pluck(FRETWISE_MUSIC.STRING_MIDI[s]+f+Number(capo),when+(k++)*.014,volume,{channel,dur:1.2});});}
  function click(t,accent){ensure();const osc=ctx.createOscillator(),g=ctx.createGain();osc.type='square';osc.frequency.value=accent?1600:1100;g.gain.setValueAtTime(accent?.25:.16,t);g.gain.exponentialRampToValueAtTime(.001,t+.05);osc.connect(g).connect(nodes.click);osc.start(t);osc.stop(t+.06);}
  function kick(t){ensure();const osc=ctx.createOscillator();const g=ctx.createGain();osc.type='sine';osc.frequency.setValueAtTime(130,t);osc.frequency.exponentialRampToValueAtTime(45,t+.11);g.gain.setValueAtTime(.5,t);g.gain.exponentialRampToValueAtTime(.001,t+.15);osc.connect(g).connect(nodes.drums);osc.start(t);osc.stop(t+.16);}
  function noise(t,duration,vol,filterFreq){ensure();const len=Math.floor(ctx.sampleRate*duration),buff=ctx.createBuffer(1,len,ctx.sampleRate),data=buff.getChannelData(0);for(let i=0;i<len;i++)data[i]=(Math.random()*2-1);const src=ctx.createBufferSource(),filter=ctx.createBiquadFilter(),g=ctx.createGain();src.buffer=buff;filter.type='highpass';filter.frequency.value=filterFreq;g.gain.setValueAtTime(vol,t);g.gain.exponentialRampToValueAtTime(.001,t+duration);src.connect(filter).connect(g).connect(nodes.drums);src.start(t);src.stop(t+duration);}
  function bass(note,t){ensure();const osc=ctx.createOscillator(),gain=ctx.createGain();osc.type='triangle';osc.frequency.value=FRETWISE_MUSIC.midiToFreq(Math.max(28,note-12));gain.gain.setValueAtTime(.0001,t);gain.gain.exponentialRampToValueAtTime(.13,t+.018);gain.gain.exponentialRampToValueAtTime(.0001,t+.45);osc.connect(gain).connect(nodes.bass);osc.start(t);osc.stop(t+.48);}
  /* The backing band follows the chord list returned by getState() bar by bar. Pass startAt (AudioContext time) to align
     bar 1 with the teacher's performance; otherwise it starts almost immediately. */
  function startBacking(getState,options={}){ensure();stopBacking();backingNext=options.startAt||(ctx.currentTime+.09);backingBeat=0;const tick=()=>{const state=getState();const beatSeconds=60/state.tempo;while(backingNext<ctx.currentTime+.16){const beat=backingBeat%4,bar=Math.floor(backingBeat/4),chords=state.chords&&state.chords.length?state.chords:[{chord:'Am'}],step=chords[bar%chords.length];if(state.click)click(backingNext,beat===0);if(state.drums){if(beat===0||beat===2)kick(backingNext);if(beat===1||beat===3)noise(backingNext,.12,.2,1050);noise(backingNext,.045,.055,6000);}if(state.bass&&step.chord){const notes=FRETWISE_MUSIC.stepMidis(step,state.capo);if(notes.length)bass(Math.min(...notes),backingNext);}if(state.rhythm&&step.chord&&(beat===0||beat===2))chord(step,state.capo,backingNext,.07,'rhythm');backingBeat++;backingNext+=beatSeconds;}};tick();backingTimer=setInterval(tick,55);}
  function stopAllNotes(){for(const source of sounding){try{source.stop(0);}catch(e){}}sounding.clear();}
  function stopBacking(){if(backingTimer){clearInterval(backingTimer);backingTimer=null;}}
  const backingRunning=()=>!!backingTimer;
  const now=()=>ctx?ctx.currentTime:0;
  // A conservative, monophonic autocorrelation estimator; never used to certify full chord voicings.
  function detectPitch(samples,sampleRate){
    const n=samples.length;let rms=0;for(let i=0;i<n;i++)rms+=samples[i]*samples[i];rms=Math.sqrt(rms/n);if(rms<.018)return null;
    const minLag=Math.floor(sampleRate/900),maxLag=Math.min(Math.floor(sampleRate/75),Math.floor(n/2));
    const scores=new Float32Array(maxLag+1);let highest=0;
    for(let lag=minLag;lag<=maxLag;lag++){
      let corr=0,a=0,b=0;for(let j=0;j<n-lag;j+=2){const v=samples[j],w=samples[j+lag];corr+=v*w;a+=v*v;b+=w*w;}
      scores[lag]=corr/Math.sqrt((a*b)||1);if(scores[lag]>highest)highest=scores[lag];
    }
    if(highest<.9)return null;
    // Select the earliest STRONG autocorrelation peak (the fundamental period),
    // rather than a later peak at 2x/3x/4x its period, which creates octave errors.
    let chosen=-1;const threshold=Math.max(.89,highest-.045);
    for(let lag=minLag+1;lag<maxLag;lag++){
      if(scores[lag]>=threshold&&scores[lag]>=scores[lag-1]&&scores[lag]>=scores[lag+1]){chosen=lag;break;}
    }
    if(chosen<0)return null;
    return {frequency:sampleRate/chosen,confidence:scores[chosen],rms};
  }
  async function startMic(onPitch){ensure();stopMic();micStream=await navigator.mediaDevices.getUserMedia({audio:{echoCancellation:false,noiseSuppression:false,autoGainControl:false}});micSource=ctx.createMediaStreamSource(micStream);micAnalyser=ctx.createAnalyser();micAnalyser.fftSize=4096;micSource.connect(micAnalyser);const sample=new Float32Array(micAnalyser.fftSize);micInterval=setInterval(()=>{if(!micAnalyser)return;micAnalyser.getFloatTimeDomainData(sample);onPitch(detectPitch(sample,ctx.sampleRate));},160);}
  function stopMic(){if(micInterval){clearInterval(micInterval);micInterval=null;}if(micSource){micSource.disconnect();micSource=null;}if(micStream){micStream.getTracks().forEach(t=>t.stop());micStream=null;}micAnalyser=null;}
  return {ensure,pluck,guitar,chord,click,setLevel,levels,startBacking,stopBacking,backingRunning,stopAllNotes,startMic,stopMic,detectPitch,hasMic:()=>!!micStream,now};
})();
