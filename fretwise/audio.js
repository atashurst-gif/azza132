/* Native Web Audio: offline guitar-pluck model, synthesized rhythm backing, and monophonic pitch observation. */
window.FRETWISE_AUDIO = (()=>{
  let ctx, backingTimer=null, backingNext=0, backingBeat=0, micStream=null, micAnalyser=null, micSource=null, micInterval=null;
  const sounding=new Set();
  const ensure=()=>{const AC=window.AudioContext||window.webkitAudioContext;if(!AC)throw Error('Your browser does not support Web Audio.');ctx=ctx||new AC();if(ctx.state==='suspended')ctx.resume();return ctx;};
  const amp=(at,value,dur=0.2)=>{const g=ctx.createGain();g.gain.setValueAtTime(value,at);g.gain.exponentialRampToValueAtTime(.0001,at+dur);g.connect(ctx.destination);return g;};
  function guitar(midi,when,volume=.32){ensure();const freq=FRETWISE_MUSIC.midiToFreq(midi),sample=ctx.sampleRate,period=Math.max(2,Math.floor(sample/freq)),length=Math.floor(sample*1.6),buffer=ctx.createBuffer(1,length,sample),data=buffer.getChannelData(0);const noise=new Float32Array(period);for(let i=0;i<period;i++)noise[i]=Math.random()*2-1;for(let i=0;i<length;i++) {if(i<period)data[i]=noise[i]*Math.exp(-i/period*.12);else data[i]=((data[i-period]+data[i-period+1])*0.5)*.9986;}const source=ctx.createBufferSource();source.buffer=buffer;const gain=ctx.createGain();gain.gain.setValueAtTime(.0001,when);gain.gain.exponentialRampToValueAtTime(Math.max(.0002,volume),when+.012);gain.gain.exponentialRampToValueAtTime(.0001,when+1.5);source.connect(gain);gain.connect(ctx.destination);source.onended=()=>sounding.delete(source);sounding.add(source);source.start(when);source.stop(when+1.54);return source;}
  function chord(step,capo,when,volume=.18){const notes=FRETWISE_MUSIC.stepMidis(step,capo);notes.forEach((note,i)=>guitar(note,when+i*.039,volume));}
  function kick(t){ensure();const osc=ctx.createOscillator();const g=ctx.createGain();osc.type='sine';osc.frequency.setValueAtTime(130,t);osc.frequency.exponentialRampToValueAtTime(45,t+.11);g.gain.setValueAtTime(.5,t);g.gain.exponentialRampToValueAtTime(.001,t+.15);osc.connect(g).connect(ctx.destination);osc.start(t);osc.stop(t+.16);}
  function noise(t,duration,vol,filterFreq){ensure();const len=Math.floor(ctx.sampleRate*duration),buff=ctx.createBuffer(1,len,ctx.sampleRate),data=buff.getChannelData(0);for(let i=0;i<len;i++)data[i]=(Math.random()*2-1);const src=ctx.createBufferSource(),filter=ctx.createBiquadFilter(),g=ctx.createGain();src.buffer=buff;filter.type='highpass';filter.frequency.value=filterFreq;g.gain.setValueAtTime(vol,t);g.gain.exponentialRampToValueAtTime(.001,t+duration);src.connect(filter).connect(g).connect(ctx.destination);src.start(t);src.stop(t+duration);}
  function bass(note,t){ensure();const osc=ctx.createOscillator(),gain=ctx.createGain();osc.type='triangle';osc.frequency.value=FRETWISE_MUSIC.midiToFreq(Math.max(28,note-12));gain.gain.setValueAtTime(.0001,t);gain.gain.exponentialRampToValueAtTime(.13,t+.018);gain.gain.exponentialRampToValueAtTime(.0001,t+.45);osc.connect(gain).connect(ctx.destination);osc.start(t);osc.stop(t+.48);}
  function startBacking(getState){ensure();stopBacking();backingNext=ctx.currentTime+.09;backingBeat=0;backingTimer=setInterval(()=>{const state=getState();const beatSeconds=60/state.tempo;while(backingNext<ctx.currentTime+.16){const beat=backingBeat%4,bar=Math.floor(backingBeat/4),step=state.lesson.steps[bar%state.lesson.steps.length];if(state.drums){if(beat===0||beat===2)kick(backingNext);if(beat===1||beat===3)noise(backingNext,.12,.2,1050);noise(backingNext,.045,.055,6000);}if(state.bass){const notes=FRETWISE_MUSIC.stepMidis(step,state.capo);bass(Math.min(...notes),backingNext);}if(state.rhythm&&beat===0)chord(step,state.capo,backingNext,.07);backingBeat++;backingNext+=beatSeconds; }},55);}
  function stopAllNotes(){for(const source of sounding){try{source.stop(0);}catch(e){}}sounding.clear();}
  function stopBacking(){if(backingTimer){clearInterval(backingTimer);backingTimer=null;}};
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
  return {ensure,guitar,chord,startBacking,stopBacking,stopAllNotes,startMic,stopMic,detectPitch,hasMic:()=>!!micStream};
})();
