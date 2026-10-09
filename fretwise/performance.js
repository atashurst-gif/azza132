/* Canonical performance timeline.
   compile() turns a lesson (or a section of it) into ONE ordered list of timed events. The audio engine, the fretting-hand
   animation, the picking-hand animation and the step/section highlighting all read from this same list, so they cannot
   disagree about fret, string, finger, pitch, capo or timing.

   Time values are in seconds from the start of the performance (before any count-in). */
window.FRETWISE_PERFORMANCE = (() => {
  const M = window.FRETWISE_MUSIC;
  const PRE_SHIFT = 0.14;           // the fretting hand forms the next shape this long before its first note sounds
  const DOWN_STAGGER = 0.013;       // seconds between strings in a downstroke
  const UP_STAGGER = 0.011;         // seconds between strings in an upstroke
  const UP_STRINGS = 4;             // an upstroke brushes the top (thinnest) strings

  const clamp=(v,lo,hi)=>Math.max(lo,Math.min(hi,v));

  // Suggest a left-hand finger for a single note from the hand's current position window.
  function fingerForNote(fret, windowStart){ if(fret<=0) return 0; return clamp(fret-windowStart+1,1,4); }

  // A five-fret window that the hand sits over; shifts when a note falls outside it (a position shift).
  function windowFor(frets, previous){
    const pressed=frets.filter(f=>f>0);
    if(!pressed.length) return previous||1;
    const lo=Math.min(...pressed), hi=Math.max(...pressed);
    const start=previous||1;
    if(lo>=start && hi<=start+4) return start;
    return clamp(Math.min(lo, Math.max(1,hi-4)),1,9);
  }

  function shapeFor(step, windowStart){
    const frets=M.stepFrets(step);
    let fingers;
    if(step.chord) fingers=M.stepFingers(step);
    else { const f=step.technique==='rest'?0:(step.finger!==undefined?step.finger:fingerForNote(step.fret, windowStart)); fingers=M.stepFingers(step,f); }
    const barre=step.chord?M.barreFor(frets,fingers):null;
    return {frets,fingers,barre,chord:step.chord||null,label:step.label||(step.chord?(M.CHORDS[step.chord]?.name||step.chord):'Note'),technique:step.technique||'pluck'};
  }

  /* compile(lesson, {capo, tempo, from, to}) */
  function compile(lesson, opts){
    const capo=Number(opts.capo||0), tempo=Number(opts.tempo||lesson.tempo||86);
    const spb=60/tempo;
    const from=clamp(Number.isInteger(opts.from)?opts.from:0,0,lesson.steps.length-1);
    const to=clamp(Number.isInteger(opts.to)?opts.to:lesson.steps.length-1,from,lesson.steps.length-1);
    const pattern=M.STRUM_PATTERNS[opts.pattern||lesson.pattern]||M.STRUM_PATTERNS.basic;
    const steps=[], events=[], gestures=[];
    let t=0, windowStart=1;
    // Establish the hand window from the whole lesson so fingerings stay consistent across sections. A melody whose
    // pressed notes all fit under one hand is played in that position (e.g. frets 3 and 5 → first and third fingers).
    if(lesson.kind==='melody'){
      const pressed=lesson.steps.filter(s=>s.technique!=='rest'&&s.fret>0).map(s=>s.fret);
      if(pressed.length){ const lo=Math.min(...pressed), hi=Math.max(...pressed); if(hi-lo<=4) windowStart=clamp(lo,1,9); }
    }
    for(let i=0;i<from;i++) windowStart=windowFor(M.stepFrets(lesson.steps[i]),windowStart);
    for(let i=from;i<=to;i++){
      const raw=lesson.steps[i];
      const frets=M.stepFrets(raw);
      windowStart=windowFor(frets,windowStart);
      const shape=shapeFor(raw,windowStart);
      const beats=raw.beats!==undefined?raw.beats:(lesson.kind==='melody'?1:4);
      const dur=beats*spb;
      const step={index:i,t,dur,beats,shape,window:windowStart,sounding:shape.chord?M.chordSound(shape.chord,capo):null};
      steps.push(step);
      if(lesson.kind==='melody'){
        if(shape.technique!=='rest'){
          const midi=M.STRING_MIDI[raw.string]+raw.fret+capo;
          const stroke=(shape.technique==='hammer'||shape.technique==='pull')?null:(events.filter(e=>e.stroke).length%2===0?'D':'U');
          const vel=shape.technique==='hammer'||shape.technique==='pull'?0.22:0.34;
          const ev={t,dur:Math.min(dur,1.5),string:raw.string,fret:raw.fret,finger:shape.fingers[raw.string],midi,vel,step:i,stroke,technique:shape.technique,gesture:gestures.length};
          events.push(ev);
          gestures.push({t,dur:0.06,kind:stroke?'pick':'legato',stroke,from:raw.string,to:raw.string,step:i});
        }
      } else {
        const sounding=frets.map((f,s)=>f>=0?s:-1).filter(s=>s>=0);
        for(const hit of pattern){
          if(hit.b>=beats) continue;
          const at=t+hit.b*spb;
          const order=hit.d==='D'?sounding:sounding.slice(-UP_STRINGS).reverse();
          const stagger=hit.d==='D'?DOWN_STAGGER:UP_STAGGER;
          const g={t:at,dur:stagger*Math.max(1,order.length-1)+0.03,kind:'strum',stroke:hit.d,from:order[0],to:order[order.length-1],step:i};
          const gi=gestures.push(g)-1;
          order.forEach((s,k)=>{
            const vel=(hit.d==='D'?(hit.b===0?0.26:0.2):0.15);
            events.push({t:at+k*stagger,dur:Math.min(1.5,(beats-hit.b)*spb),string:s,fret:frets[s],finger:shape.fingers[s],midi:M.STRING_MIDI[s]+frets[s]+capo,vel,step:i,stroke:hit.d,technique:'pluck',gesture:gi});
          });
        }
      }
      t+=dur;
    }
    events.sort((a,b)=>a.t-b.t);
    return {lessonId:lesson.id,kind:lesson.kind,capo,tempo,secondsPerBeat:spb,from,to,steps,events,gestures,duration:t,
      stepAt(time){ if(time<0) return steps[0]; for(let i=steps.length-1;i>=0;i--) if(time>=steps[i].t) return steps[i]; return steps[0]; },
      preShift:PRE_SHIFT};
  }

  /* createPlayer({audio, onTick, onStep, onEnd, onCountIn}) schedules audio ahead of the clock and reports the
     performance clock to the UI every animation frame. Everything is referenced to AudioContext time. */
  function createPlayer(h){
    const audio=h.audio; let ctx=null, perf=null, timer=null, raf=null, startAt=0, countIn=0, loopCount=1, pass=0, next=0, playing=false, lastStep=-1, lastPass=-1, lastCount=-1, endReason=null;
    const LOOKAHEAD=0.18, INTERVAL=30;
    const nowFn=()=>typeof requestAnimationFrame==='function'?requestAnimationFrame:(fn)=>setTimeout(()=>fn(),16);
    const cancelFn=()=>typeof cancelAnimationFrame==='function'?cancelAnimationFrame:clearTimeout;
    function trackStep(){ // step changes are reported from the audio clock, independent of the screen's frame rate
      const abs=ctx.currentTime-startAt; if(abs<countIn) return; const rel=abs-countIn; if(rel>=perf.duration*loopCount){ finish('complete'); return; }
      const p=Math.min(loopCount-1,Math.floor(rel/perf.duration)); const stepObj=perf.stepAt(rel-p*perf.duration+PRE_SHIFT);
      if(stepObj.index!==lastStep||p!==lastPass){ lastStep=stepObj.index; lastPass=p; h.onStep&&h.onStep(stepObj,p); }
    }
    function schedule(){
      if(!playing) return;
      trackStep(); if(!playing) return;
      const horizon=ctx.currentTime+LOOKAHEAD;
      for(;;){
        if(next>=perf.events.length){
          if(pass+1<loopCount){ pass++; next=0; continue; }
          break;
        }
        const ev=perf.events[next];
        const when=startAt+countIn+pass*perf.duration+ev.t;
        if(when>horizon) break;
        audio.pluck(ev.midi, when, ev.vel, ev);
        next++;
      }
    }
    function frame(){
      if(!playing) return;
      const abs=ctx.currentTime-startAt;
      if(abs<countIn){
        const beat=Math.floor(abs/perf.secondsPerBeat);
        if(beat!==lastCount){ lastCount=beat; h.onCountIn&&h.onCountIn(Math.round(countIn/perf.secondsPerBeat)-beat); }
        h.onTick&&h.onTick({time:-1,abs,pass:0,countingIn:true});
        raf=nowFn()(frame); return;
      }
      const rel=abs-countIn;
      const p=Math.min(loopCount-1,Math.floor(rel/perf.duration));
      const time=rel-p*perf.duration;
      const stepObj=perf.stepAt(time+PRE_SHIFT);
      if(rel>=perf.duration*loopCount){ finish('complete'); return; }
      if(stepObj.index!==lastStep||p!==lastPass){ lastStep=stepObj.index; lastPass=p; h.onStep&&h.onStep(stepObj,p); }
      h.onTick&&h.onTick({time,abs,pass:p,countingIn:false});
      raf=nowFn()(frame);
    }
    function finish(reason){ if(!playing) return; playing=false; clearInterval(timer); timer=null; if(raf!==null){cancelFn()(raf);raf=null;} endReason=reason; h.onEnd&&h.onEnd(reason,{pass,perf}); }
    return {
      start(performance, options={}){
        this.stop('restart');
        ctx=audio.ensure(); perf=performance; loopCount=options.loop&&options.loop>1?options.loop:1; if(options.loop===Infinity) loopCount=Infinity;
        countIn=options.countIn?4*perf.secondsPerBeat:0;
        startAt=ctx.currentTime+0.08; pass=0; next=0; lastStep=-1; lastPass=-1; lastCount=-1; playing=true; endReason=null;
        if(countIn) for(let i=0;i<4;i++) audio.click(startAt+i*perf.secondsPerBeat, i===0);
        schedule(); timer=setInterval(schedule,INTERVAL); raf=nowFn()(frame);
      },
      stop(reason='stopped'){ if(!playing) return; finish(reason); audio.stopAllNotes(); },
      isPlaying:()=>playing,
      current:()=>({perf,pass,playing,endReason,loopCount,startAt,countIn}),
      // Position in seconds inside the current pass, or null while counting in / stopped (drives the 3D teacher).
      clock(){ if(!playing||!ctx) return null; const rel=ctx.currentTime-startAt-countIn; if(rel<0) return null; return rel-Math.floor(rel/perf.duration)*perf.duration; },
      // Position in seconds inside the current pass (used for pause/resume).
      position(){ if(!playing||!ctx) return 0; const rel=ctx.currentTime-startAt-countIn; return rel<0?0:rel-Math.floor(rel/perf.duration)*perf.duration; }
    };
  }

  return {compile,createPlayer,fingerForNote,windowFor,shapeFor,PRE_SHIFT};
})();
