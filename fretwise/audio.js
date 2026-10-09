/* Native Web Audio: real recorded samples for the teacher's steel-string guitar and the backing band (FluidR3 GM guitar and
   fingered bass, CC BY 3.0; Virtuosity Drums kit, CC0 — see assets/samples/CREDITS.md), a procedural room reverb, a mixer with
   per-channel levels, a click, and a conservative monophonic pitch estimator for the microphone. Until the samples are decoded
   a Karplus–Strong plucked string (and synthesised drums and bass) stands in, so a note is never silent.
   Every note is scheduled against AudioContext time so sound lines up with the performance timeline. */
window.FRETWISE_AUDIO = (()=>{
  let ctx, backingTimer=null, backingNext=0, backingBeat=0, micStream=null, micAnalyser=null, micSource=null, micInterval=null, noiseBuf=null;
  const sounding=new Set();                 // live plucked-string voices (teacher + rhythm guitar), so stopAllNotes() can silence them
  const strings={};                         // 'channel|string' → the voice currently sounding on that string
  const levels={master:1,guitar:1,drums:.9,bass:.9,rhythm:.8,click:.7};
  const nodes={};
  const GAIN={guitar:3.6,synth:1,bass:.8,kick:.42,snare:.28,hat:.12};   // sample level → mix level (samples peak around −15 dBFS)
  const REL=.22, CHOKE=.03, RING=2;         // release after the let-ring, same-string fade, default let-ring when no duration is given

  /* ---- the sample bank ---- */
  const NOTE_NAMES=['C','Db','D','Eb','E','F','Gb','G','Ab','A','Bb','B'];
  const noteName=m=>NOTE_NAMES[((m%12)+12)%12]+(Math.floor(m/12)-1);        // 40 → E2, 46 → Bb2: the files use flats
  const SETS={guitar:{dir:'assets/samples/guitar-steel/',lo:40,hi:88,keep:2.4},bass:{dir:'assets/samples/bass/',lo:28,hi:55,keep:1.8}};
  const DRUMS={kick:['kick-1','kick-2'],snare:['snare-1','snare-2'],hat:['hat-1','hat-2','hat-3']};
  function sampleList(){const out=[];for(const [set,s] of Object.entries(SETS))for(let m=s.lo;m<=s.hi;m++)out.push({set,key:m,url:s.dir+noteName(m)+'.mp3'});for(const [piece,files] of Object.entries(DRUMS))files.forEach((f,i)=>out.push({set:'drums',key:piece+i,url:'assets/samples/drums/'+f+'.mp3'}));return out;}
  const bank={guitar:{},bass:{},drums:{}}, status={total:sampleList().length,loaded:0,failed:0};
  let loading=null;
  // First sample of the attack (2% of peak), less 1 ms, so a note's sound starts on its scheduled time.
  function onsetIndex(data,sampleRate){let peak=0;for(let i=0;i<data.length;i++){const a=Math.abs(data[i]);if(a>peak)peak=a;}if(!peak)return 0;let i=0;while(i<data.length&&Math.abs(data[i])<peak*.02)i++;return Math.max(0,i-Math.round(sampleRate*.001));}
  // Trim the lead-in, keep only the length a note can use (with a short fade), and fold dual-mono files to one channel.
  function prepare(c,buf,keep,mono){const sr=buf.sampleRate,start=onsetIndex(buf.getChannelData(0),sr),len=Math.max(1,Math.min(buf.length-start,Math.round(sr*keep))),chs=mono?1:buf.numberOfChannels,out=c.createBuffer(chs,len,sr),fade=Math.min(len,Math.round(sr*.05));for(let ch=0;ch<chs;ch++){const to=out.getChannelData(ch);to.set(buf.getChannelData(ch).subarray(start,start+len));for(let i=0;i<fade;i++)to[len-1-i]*=i/fade;}return out;}
  /* Fetches and decodes every bundled sample once (an OfflineAudioContext decodes before the first click, so no user gesture is
     needed). Resolves true when all of them decoded. Missing files are reported and those notes keep the synth voice. */
  function loadSamples(){
    if(loading)return loading;
    const OAC=typeof window!=='undefined'&&(window.OfflineAudioContext||window.webkitOfflineAudioContext);
    if(typeof fetch!=='function'||!(ctx||OAC))return Promise.resolve(false);
    const dec=ctx||new OAC(1,1,44100);
    const decode=ab=>new Promise((res,rej)=>{const p=dec.decodeAudioData(ab,res,rej);if(p&&p.catch)p.catch(rej);});
    loading=Promise.all(sampleList().map(async item=>{
      try{const r=await fetch(item.url);if(!r.ok)throw Error('HTTP '+r.status);const buf=await decode(await r.arrayBuffer());bank[item.set][item.key]=prepare(dec,buf,item.set==='drums'?1.2:SETS[item.set].keep,item.set!=='drums');status.loaded++;}
      catch(e){status.failed++;console.warn('Fretwise: sample '+item.url+' unavailable ('+(e&&e.message||e)+'); using the synth voice.');}
    })).then(()=>status.failed===0);
    return loading;
  }
  const samplesReady=()=>status.loaded===status.total;
  // Nearest recorded note (within 3 semitones of what has loaded); outside the sampled range the closest end is re-pitched.
  function nearestSample(midi,set,have){const s=SETS[set];let key=Math.max(s.lo,Math.min(s.hi,Math.round(midi)));if(have&&!have[key]){let best=null;for(let d=1;d<=3&&best===null;d++){if(have[key-d])best=key-d;else if(have[key+d])best=key+d;}if(best===null)return null;key=best;}return {key,rate:Math.pow(2,(midi-key)/12)};}
  // Velocity → loudness and brightness (soft upstrokes are quieter and darker); hammer-ons, pull-offs and slides have no pick attack.
  function toneFor(vel,technique){const v=Math.max(0,Number(vel)||0),n=Math.min(1,v/.36),legato=technique==='hammer'||technique==='pull'||technique==='slide';return {gain:Math.max(.0002,v),cutoff:Math.round(1900*Math.pow(2,n*2.6)*(legato?.7:1)),attack:legato?.012:0,offset:legato?.02:0,legato};}
  // A string sounds one note at a time: a later note on the same string returns the earlier voice so it can be faded at the new start.
  function claimString(map,key,voice){const prev=map[key];if(prev&&voice.at<prev.at)return null;map[key]=voice;return prev&&voice.at<prev.stopAt?prev:null;}

  /* ---- mixer, room and master ---- */
  // Procedural room: decaying stereo noise that darkens as it fades (−60 dB at the end), after a 6 ms pre-delay.
  function roomImpulse(c,seconds){const sr=c.sampleRate,len=Math.round(sr*seconds),ir=c.createBuffer(2,len,sr),pre=Math.round(sr*.006);for(let ch=0;ch<2;ch++){const d=ir.getChannelData(ch);let lp=0;for(let i=0;i<len;i++){const p=i/len;lp+=(.85-.65*p)*((Math.random()*2-1)-lp);d[i]=i<pre?0:lp*Math.pow(.001,p);}}return ir;}
  function build(){
    const comp=ctx.createDynamicsCompressor();comp.threshold.value=-10;comp.knee.value=8;comp.ratio.value=4;comp.attack.value=.003;comp.release.value=.25;comp.connect(ctx.destination);
    nodes.master=ctx.createGain();nodes.master.gain.value=levels.master;nodes.master.connect(comp);
    const room=ctx.createConvolver();room.buffer=roomImpulse(ctx,1.2);room.connect(nodes.master);
    for(const k of ['guitar','drums','bass','rhythm','click']){nodes[k]=ctx.createGain();nodes[k].gain.value=levels[k];nodes[k].connect(nodes.master);}
    for(const [k,wet] of [['guitar',.12],['rhythm',.1],['drums',.05]]){const send=ctx.createGain();send.gain.value=wet;nodes[k].connect(send);send.connect(room);}
  }
  const ensure=()=>{const AC=window.AudioContext||window.webkitAudioContext;if(!AC)throw Error('Your browser does not support Web Audio.');if(!ctx){ctx=new AC();build();loadSamples();}if(ctx.state==='suspended'){const p=ctx.resume();if(p&&p.catch)p.catch(()=>{});}return ctx;};
  function setLevel(name,value){levels[name]=Math.max(0,Math.min(1,Number(value)));if(nodes[name]&&ctx)nodes[name].gain.setTargetAtTime(levels[name],ctx.currentTime,.02);}

  /* ---- voices ---- */
  // source → low-pass (brightness) → envelope (attack, let-ring, ~220 ms release) → choke (same-string fade / stop) → bus
  function voice(src,o){
    const filter=ctx.createBiquadFilter(),env=ctx.createGain(),choke=ctx.createGain(),at=o.at,end=at+o.ring;
    filter.type='lowpass';filter.frequency.value=o.cutoff;filter.Q.value=0;
    if(o.attack){env.gain.setValueAtTime(0,at);env.gain.linearRampToValueAtTime(o.peak,at+o.attack);}else env.gain.setValueAtTime(o.peak,at);
    env.gain.setValueAtTime(o.peak,end);env.gain.exponentialRampToValueAtTime(o.peak*.001,end+REL);env.gain.setValueAtTime(0,end+REL);
    src.connect(filter);filter.connect(env);env.connect(choke);choke.connect(o.bus);
    const v={src,choke,at,stopAt:end+REL+.01,midi:o.midi};
    src.onended=()=>{sounding.delete(v);try{choke.disconnect();}catch(e){}};
    if(o.track)sounding.add(v);
    src.start(at,o.offset||0);src.stop(v.stopAt);
    return v;
  }
  function fadeOut(v,t){if(v.fadeAt!==undefined&&v.fadeAt<=t)return;v.fadeAt=t;try{const g=v.choke.gain;g.cancelScheduledValues(t);g.setValueAtTime(1,t);g.linearRampToValueAtTime(0,t+CHOKE);v.src.stop(t+CHOKE+.005);v.stopAt=t+CHOKE+.005;}catch(e){}}
  // Karplus–Strong stand-in: a short noise burst into a damped delay line, longer sustain for lower strings.
  function ksSource(midi,meta){const freq=FRETWISE_MUSIC.midiToFreq(midi),sample=ctx.sampleRate,period=Math.max(2,Math.floor(sample/freq)),seconds=Math.min(1.8,Math.max(.6,(meta.dur||1.2)+.3)),length=Math.floor(sample*seconds),buffer=ctx.createBuffer(1,length,sample),data=buffer.getChannelData(0);const bright=meta.technique==='hammer'||meta.technique==='pull'?.35:.55;const damping=midi<50?.9992:midi<60?.9988:.9984;for(let i=0;i<period;i++)data[i]=(Math.random()*2-1)*(i<period*bright?1:.5);for(let i=period;i<length;i++)data[i]=((data[i-period]+data[i-period+1])*0.5)*damping;const s=ctx.createBufferSource();s.buffer=buffer;return s;}
  /* One plucked note. meta (a timeline event) may carry string (one note per string), dur (let-ring), technique and channel.
     Timing is never altered: the note starts exactly at `when` (or now, if that has passed). */
  function pluck(midi,when,volume=.32,meta={}){
    ensure();meta=meta||{};
    const at=Math.max(when,ctx.currentTime),tone=toneFor(volume,meta.technique),channel=meta.channel==='rhythm'?'rhythm':'guitar';
    const key=Number.isInteger(meta.string)?channel+'|'+meta.string:null,before=key&&strings[key];
    const pick=nearestSample(midi,'guitar',bank.guitar),buf=pick&&bank.guitar[pick.key];
    let src,maxRing,peak,attack=tone.attack,offset=tone.offset;
    if(buf){
      src=ctx.createBufferSource();src.buffer=buf;
      const rate=pick.rate*Math.pow(2,(Math.random()*2-1)*3/1200);        // ±3 cents, like a real, slightly imperfect string
      if(meta.technique==='slide'){const from=before&&before.stopAt>at&&before.midi!==undefined?before.midi:midi-2;src.playbackRate.setValueAtTime(rate*Math.pow(2,(from-midi)/12),at);src.playbackRate.exponentialRampToValueAtTime(rate,at+.07);}
      else src.playbackRate.value=rate;
      maxRing=(buf.duration-offset)/rate-REL-.02;peak=tone.gain*GAIN.guitar;src.fretwiseVoice='sample';
    }else{src=ksSource(midi,meta);maxRing=src.buffer.duration-REL-.02;peak=tone.gain*GAIN.synth;attack=Math.max(attack,.008);offset=0;src.fretwiseVoice='synth';}
    const ring=Math.max(attack+.04,Math.min(maxRing,meta.dur>0?meta.dur:RING));
    const v=voice(src,{at,ring,peak,attack,offset,cutoff:tone.cutoff,bus:nodes[channel],midi,track:true});
    if(key){const prev=claimString(strings,key,v);if(prev)fadeOut(prev,at);}
    return src;
  }
  const guitar=(midi,when,volume)=>pluck(midi,when,volume,{});
  // A quick downstroke over the chord's sounding strings (used by the rhythm-guitar track).
  function chord(step,capo,when,volume=.18,channel='guitar',dur=1.2){const frets=FRETWISE_MUSIC.stepFrets(step);let k=0;frets.forEach((f,s)=>{if(f<0)return;pluck(FRETWISE_MUSIC.STRING_MIDI[s]+f+Number(capo),when+(k++)*.014,volume,{channel,dur,string:s});});}
  function click(t,accent){ensure();const osc=ctx.createOscillator(),g=ctx.createGain();osc.type='square';osc.frequency.value=accent?1600:1100;g.gain.setValueAtTime(accent?.25:.16,t);g.gain.exponentialRampToValueAtTime(.001,t+.05);osc.connect(g).connect(nodes.click);osc.start(t);osc.stop(t+.06);}

  /* ---- backing band ---- */
  const rr={};
  // A recorded drum hit (round-robin, with a little level variation like a real drummer); synthesised if not loaded yet.
  function drumHit(piece,t,vel,index){const files=DRUMS[piece],i=index!==undefined?index:(rr[piece]=((rr[piece]||0)+1)%files.length),buf=bank.drums[piece+i];if(!buf)return false;const s=ctx.createBufferSource(),g=ctx.createGain();s.buffer=buf;g.gain.value=GAIN[piece]*vel*(.94+Math.random()*.12);s.connect(g).connect(nodes.drums);s.start(t);return true;}
  function noise(t,duration,vol,type,freq,q){if(!noiseBuf){noiseBuf=ctx.createBuffer(1,ctx.sampleRate,ctx.sampleRate);const d=noiseBuf.getChannelData(0);for(let i=0;i<d.length;i++)d[i]=Math.random()*2-1;}const src=ctx.createBufferSource(),filter=ctx.createBiquadFilter(),g=ctx.createGain();src.buffer=noiseBuf;filter.type=type;filter.frequency.value=freq;if(q)filter.Q.value=q;g.gain.setValueAtTime(vol,t);g.gain.exponentialRampToValueAtTime(.001,t+duration);src.connect(filter).connect(g).connect(nodes.drums);src.start(t,Math.random()*.5);src.stop(t+duration+.01);}
  function drumTone(t,type,f0,f1,sweep,vol,duration){const osc=ctx.createOscillator(),g=ctx.createGain();osc.type=type;osc.frequency.setValueAtTime(f0,t);osc.frequency.exponentialRampToValueAtTime(f1,t+sweep);g.gain.setValueAtTime(vol,t);g.gain.exponentialRampToValueAtTime(.001,t+duration);osc.connect(g).connect(nodes.drums);osc.start(t);osc.stop(t+duration+.01);}
  function kick(t,vel=1){ensure();if(drumHit('kick',t,vel))return;drumTone(t,'sine',150,48,.12,.55*vel,.3);noise(t,.012,.12*vel,'highpass',2500);}
  function snare(t,vel=1){ensure();if(drumHit('snare',t,vel))return;drumTone(t,'triangle',195,165,.06,.2*vel,.09);noise(t,.14,.17*vel,'bandpass',3200,.7);}
  function hat(t,accent){ensure();if(drumHit('hat',t,accent?.9:.75,accent?2:(rr.hatSoft=((rr.hatSoft||0)+1)%2)))return;noise(t,.04,accent?.05:.035,'highpass',7500);}
  // Fingered electric bass, one note at a time, ringing for `dur` seconds.
  function bass(note,t,dur=.45,vel=1){ensure();const midi=Math.max(28,note-12),pick=nearestSample(midi,'bass',bank.bass),buf=pick&&bank.bass[pick.key];
    if(!buf){const osc=ctx.createOscillator(),gain=ctx.createGain();osc.type='triangle';osc.frequency.value=FRETWISE_MUSIC.midiToFreq(midi);gain.gain.setValueAtTime(.0001,t);gain.gain.exponentialRampToValueAtTime(.13*vel,t+.018);gain.gain.exponentialRampToValueAtTime(.0001,t+.45);osc.connect(gain).connect(nodes.bass);osc.start(t);osc.stop(t+.48);return;}
    const src=ctx.createBufferSource();src.buffer=buf;src.playbackRate.value=pick.rate;
    const v=voice(src,{at:t,ring:Math.max(.08,Math.min(dur,buf.duration/pick.rate-REL-.02)),peak:GAIN.bass*vel,attack:.004,cutoff:1600,bus:nodes.bass,midi,track:false});
    const prev=claimString(strings,'bass|0',v);if(prev)fadeOut(prev,t);
  }
  /* The backing band follows the chord list returned by getState() bar by bar. Pass startAt (AudioContext time) to align
     bar 1 with the teacher's performance; otherwise it starts almost immediately. */
  function startBacking(getState,options={}){ensure();stopBacking();backingNext=options.startAt||(ctx.currentTime+.09);backingBeat=0;const tick=()=>{const state=getState();const beatSeconds=60/state.tempo;while(backingNext<ctx.currentTime+.16){const beat=backingBeat%4,bar=Math.floor(backingBeat/4),chords=state.chords&&state.chords.length?state.chords:[{chord:'Am'}],step=chords[bar%chords.length];if(state.click)click(backingNext,beat===0);if(state.drums){if(beat===0||beat===2)kick(backingNext,beat===0?1:.88);if(beat===1||beat===3)snare(backingNext,beat===3?1:.92);hat(backingNext,beat===0);}if(state.bass&&step.chord){const notes=FRETWISE_MUSIC.stepMidis(step,state.capo);if(notes.length)bass(Math.min(...notes),backingNext,beatSeconds*.92,beat===0?1:.85);}if(state.rhythm&&step.chord&&(beat===0||beat===2))chord(step,state.capo,backingNext,.07,'rhythm',beatSeconds*1.9);backingBeat++;backingNext+=beatSeconds;}};tick();backingTimer=setInterval(tick,55);}
  // Silences every guitar voice (a 30 ms fade, so there is no click) and forgets which strings were ringing.
  function stopAllNotes(){const t=ctx?ctx.currentTime:0;for(const v of sounding)fadeOut(v,t);sounding.clear();for(const k of Object.keys(strings))if(k!=='bass|0')delete strings[k];}
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
  // Start fetching and decoding the samples as soon as the page has loaded, so the first demonstration already uses them.
  if(typeof document!=='undefined'&&typeof fetch==='function'&&window.addEventListener){const go=()=>setTimeout(()=>{try{loadSamples();}catch(e){}},0);if(document.readyState==='complete')go();else window.addEventListener('load',go,{once:true});}
  return {ensure,pluck,guitar,chord,click,setLevel,levels,startBacking,stopBacking,backingRunning,stopAllNotes,startMic,stopMic,detectPitch,hasMic:()=>!!micStream,now,
    loadSamples,samplesReady,sampleStatus:()=>({...status,ready:samplesReady()}),sampleBuffer:(midi,set='guitar')=>bank[set]&&bank[set][midi]||null,
    engine:{noteName,sampleList,nearestSample,toneFor,claimString,onsetIndex,SETS,DRUMS}};
})();
