/* Fretwise app: wires the UI to the performance timeline (audio + hands), the tutor brain, local memory and voice.
   Nothing here executes text from the chat; tutor replies are {say, actions, remember} and actions are allow-listed. */
(() => {
  'use strict';
  const M=window.FRETWISE_MUSIC, A=window.FRETWISE_AUDIO, P=window.FRETWISE_PERFORMANCE, T=window.FRETWISE_TUTOR;
  const el=(id)=>document.getElementById(id);
  const PAGES=['studio','teacher','guitar','progress','builder'];
  const KEY_V1='fretwise-prototype-v01', KEY='fretwise-v02';
  const DEFAULT={capo:5,tempo:86,baseTempo:86,lesson:'capo',stats:{sessions:0,demos:0,questions:0},imageData:null,importedLessons:[],messages:[],selection:{from:0,to:3},cursor:0,loopCount:1,countIn:true,slow:false,
    backing:{drums:true,bass:true,rhythm:false,click:false,withDemo:false,volumes:{drums:.9,bass:.9,rhythm:.8,master:1}}};

  /* ---------- state + migration (never discard the learner's history) ---------- */
  let stored=null;
  try{ stored=JSON.parse(localStorage.getItem(KEY)||'null'); }catch(e){ stored=null; }
  if(!stored){ let v1={}; try{ v1=JSON.parse(localStorage.getItem(KEY_V1)||'{}'); }catch(e){} stored=T.migrate(v1); }
  if(!stored.memory||stored.memory.version!==T.MEMORY_VERSION) stored=T.migrate(stored);
  const state={...DEFAULT,...stored,stats:{...DEFAULT.stats,...(stored.stats||{})},selection:{...DEFAULT.selection,...(stored.selection||{})},
    backing:{...DEFAULT.backing,...(stored.backing||{}),volumes:{...DEFAULT.backing.volumes,...((stored.backing||{}).volumes||{})}}};
  const base=T.defaultMemory();
  state.memory.playing={...base.playing,...state.memory.playing,instrument:{...base.playing.instrument,...(state.memory.playing.instrument||{})},chords:{...base.playing.chords,...(state.memory.playing.chords||{})}};
  state.memory.teaching={...base.teaching,...state.memory.teaching};
  state.memory.owner={...base.owner,...state.memory.owner};
  delete state.prefs; delete state.knowledge; delete state.changes; // folded into memory
  state.stats.sessions++;
  const prefs=()=>state.memory.teaching;
  function save(){ try{ localStorage.setItem(KEY,JSON.stringify(state)); }catch(e){ console.warn('Local storage full',e); } }

  /* ---------- lessons ---------- */
  const scratchLessons=[];
  const lessons=()=>[...M.LESSONS,...state.importedLessons,...scratchLessons];
  const lesson=()=>lessons().find(x=>x.id===state.lesson)||M.LESSONS[0];
  const lastIndex=()=>lesson().steps.length-1;
  function clampSelection(){ const last=lastIndex(); let {from,to}=state.selection; if(!Number.isInteger(from)||from<0||from>last) from=0; if(!Number.isInteger(to)||to<from||to>last) to=last; state.selection={from,to}; state.cursor=Math.min(Math.max(state.cursor,0),last); }

  /* ---------- runtime ---------- */
  let player=null, perfNow=null, evCursor=0, gCursor=0, lastPass=-1, resumeStep=null, resumeAfterTalk=false, speaking=false, recognition=null, builderRecognition=null, holdTimer=null, holdMode=false;
  let missed=0, lastPitchAt=0, lastSuccess='', llm={on:false,model:null};
  let attempt=null; // {expected:[{midi,label,step}], heard:[], startedAt, lastMidi}
  const brain=T.create({music:M,getContext:()=>({lesson:lesson(),lessons:lessons(),memory:state.memory,cursor:state.cursor,selection:state.selection,capo:state.capo,tempo:state.tempo,baseTempo:state.baseTempo,micOn:A.hasMic(),hintsVisible:!el('hintPanel').classList.contains('hidden'),playing:!!(player&&player.isPlaying())})});

  /* ---------- stage: neck, strings, hands ---------- */
  let shownWindow=1, hand=null;
  function initWood(){
    const wood=el('teacherFrets'); wood.innerHTML='';
    for(let i=1;i<=5;i++){ const fret=document.createElement('div'); fret.className='fret-segment'; fret.dataset.fret=String(i); wood.append(fret); }
    for(let i=0;i<6;i++){ const line=document.createElement('span'); line.className='string-line'; line.dataset.string=String(5-i); line.style.top=(((i+.5)/6)*100)+'%'; el('teacherNeck').append(line); }
    const body=el('bodyStrings'); body.innerHTML='';
    for(let i=0;i<6;i++){ const line=document.createElement('span'); line.className='body-string'; line.dataset.string=String(5-i); line.style.top=(((i+.5)/6)*100)+'%'; body.append(line); }
    hand=window.FRETWISE_HAND.create(el('teacherNeck'));
    pickTo(5,null,0);
  }
  function refreshInlayMarkers(windowStart){
    const nums=el('fretNumbers'); nums.innerHTML='';
    document.querySelectorAll('.fret-segment').forEach((segment,i)=>{
      segment.querySelectorAll('.fret-marker').forEach(marker=>marker.remove());
      const relative=windowStart+i, physical=relative+state.capo;
      if([3,5,7,9,12,15,17].includes(physical)){ const marker=document.createElement('span'); marker.className='fret-marker'; segment.append(marker); if(physical===12){ const second=marker.cloneNode(); second.style.top='34%'; marker.style.top='66%'; segment.append(second); } }
      const n=document.createElement('span'); n.textContent=(i===0&&relative===1&&state.capo?'capo '+state.capo+' · ':'')+'fret '+physical; if(i===0&&relative===1&&state.capo) n.className='capo'; nums.append(n);
    });
    el('teacherNeck').classList.toggle('has-capo',state.capo>0&&windowStart===1);
  }
  function renderHand(shape,windowStart,immediate){
    if(windowStart!==shownWindow){ shownWindow=windowStart; refreshInlayMarkers(windowStart); }
    hand.setShape(shape,windowStart,!!immediate);
  }
  const stringTop=(s)=>(((5-s)+.5)/6*100)+'%';
  function ringString(s){ document.querySelectorAll('[data-string="'+s+'"]').forEach(n=>{ n.classList.remove('ringing'); void n.offsetWidth; n.classList.add('ringing'); }); }
  function pickTo(s,stroke,dur){ const pick=el('pickHand'); pick.style.setProperty('--pick-dur',Math.max(.04,dur||.08)+'s'); pick.style.top='calc('+stringTop(s)+' - 18px)'; pick.classList.toggle('down',stroke==='D'); pick.classList.toggle('up',stroke==='U'); pick.classList.toggle('legato',!stroke); }
  function gesture(g){ if(g.kind==='strum'){ pickTo(g.from,g.stroke,0); requestAnimationFrame(()=>pickTo(g.to,g.stroke,g.dur)); } else pickTo(g.from,g.stroke,.05); }

  /* ---------- stage: static render ---------- */
  function stepShape(i){ const l=lesson(); const perf=P.compile(l,{capo:state.capo,tempo:state.tempo,from:i,to:i}); return perf.steps[0]; }
  function humanShape(step){ return step.chord?(M.CHORDS[step.chord]?.name||step.chord):(step.label||'Single note'); }
  function stepInfo(step){ if(step.chord) return 'Sounds as '+M.chordSound(step.chord,state.capo)+(state.capo?' with capo '+state.capo:' without capo'); if(step.technique==='rest') return 'Rest'; const notes=M.stepMidis(step,state.capo); return notes.length?M.midiToName(notes[0])+' sounding · string '+(6-step.string)+' · fret '+step.fret+(state.capo?' from the capo':''):'Practice note'; }
  function renderStage(){
    clampSelection(); const l=lesson(); const i=state.cursor; const step=l.steps[i];
    el('exerciseTitle').textContent=l.title; el('nowChord').textContent=humanShape(step); el('actualChord').textContent=stepInfo(step);
    const s=stepShape(i); shownWindow=-1; renderHand(s.shape,s.window);
    renderSteps(); renderSections(); renderRange();
    if(!el('hintPanel').classList.contains('hidden')) renderHint();
    el('profileCapo').textContent=state.capo?'Fret '+state.capo:'None';
  }
  function renderSteps(){
    const box=el('stepsTrack'); box.innerHTML=''; const l=lesson(); const key=(i)=>l.id+':'+i+'-'+i;
    l.steps.forEach((step,i)=>{ const b=document.createElement('button'); b.className='step-card'+(i===state.cursor?' active':'')+(i>=state.selection.from&&i<=state.selection.to?' in-range':''); b.innerHTML='<strong>'+(i+1)+'</strong><small></small>'; b.querySelector('small').textContent=step.label||humanShape(step); b.title='Click to select step '+(i+1)+'; shift-click to extend the section'; b.dataset.index=String(i);
      const st=state.memory.playing.sections[key(i)]; if(st&&st.ok){ const ok=document.createElement('span'); ok.className='step-ok'; ok.textContent='✓ '+st.ok; b.append(ok); }
      b.addEventListener('click',(e)=>{ stopPlayback('select'); if(e.shiftKey){ const from=Math.min(state.selection.from,i), to=Math.max(state.selection.to,i); state.selection={from,to}; state.cursor=i; } else { state.cursor=i; state.selection={from:i,to:i}; } brain.event('section_selected'); el('confirmBar').hidden=true; save(); renderStage(); });
      box.append(b); });
  }
  function renderSections(){
    const box=el('sectionChips'); box.innerHTML=''; const l=lesson(); const last=lastIndex();
    const chips=[{name:'Whole lesson',from:0,to:last},...M.sectionsFor(l)];
    chips.forEach(sec=>{ const b=document.createElement('button'); b.textContent=sec.name; b.classList.toggle('active',state.selection.from===sec.from&&state.selection.to===sec.to); b.addEventListener('click',()=>chooseSection(sec)); box.append(b); });
  }
  function renderRange(){ const {from,to}=state.selection; el('rangeLabel').textContent=from===to?'Step '+(from+1):'Steps '+(from+1)+'–'+(to+1); el('loopCount').value=state.loopCount===Infinity?'inf':String(state.loopCount); el('countInToggle').checked=!!state.countIn; el('slowToggle').classList.toggle('active',!!state.slow); }
  function chooseSection(sec){ stopPlayback('select'); state.selection={from:sec.from,to:sec.to}; state.cursor=sec.from; brain.event('section_selected'); save(); renderStage(); el('confirmText').textContent='Playing “'+sec.name+'” once — is this the part you want to practise?'; el('confirmBar').hidden=false; brain.session.awaiting='confirm_section'; brain.session.phase='passage'; play({from:sec.from,to:sec.to,loop:1,countIn:false,silent:true}); }

  /* ---------- hints (hidden by default; explicit ask or struggle only) ---------- */
  function renderHint(){
    const s=stepShape(state.cursor); const {frets,fingers}=s.shape; const w=s.window; const step=lesson().steps[state.cursor];
    el('hintTitle').textContent=humanShape(step)+' · finger-by-finger';
    el('hintText').textContent=step.chord?'Green circles show where fingers go. Read the top row as the thin, high e string. Numbers are your fingers (1=index, 4=little finger). Press just behind each fret, not on the metal.':'The green circle is your target note. Try that one string slowly before playing the full phrase.';
    const box=el('miniFret'); box.innerHTML=''; const names=['e','B','G','D','A','E'];
    for(let row=0;row<6;row++){ const label=document.createElement('span'); label.className='mini-cell label'; label.textContent=names[row]; box.append(label); for(let c=0;c<5;c++){ const fret=w+c; const cell=document.createElement('span'); cell.className='mini-cell'; const idx=5-row; if(frets[idx]===fret){ const dot=document.createElement('span'); dot.className='dot'; dot.textContent=fingers[idx]||'●'; cell.append(dot); } if(row===5){ cell.title='fret '+(fret+state.capo); } box.append(cell); } }
  }
  function showHints(trigger){ el('hintPanel').classList.remove('hidden'); el('showHints').textContent='✓ Hide fret help'; renderHint(); if(trigger==='auto') tutorSay('We’ve hit a tricky spot a few times, so I’ve opened the finger-position guide. One note at a time.'); }
  function hideHints(){ el('hintPanel').classList.add('hidden'); el('showHints').textContent='◎ Show fret help'; }

  /* ---------- playback (everything comes from the compiled timeline) ---------- */
  function ensurePlayer(){
    if(player) return player;
    player=P.createPlayer({audio:A,
      onCountIn:(n)=>{ const c=el('countIn'); c.hidden=false; c.textContent=String(n); c.style.animation='none'; void c.offsetWidth; c.style.animation=''; },
      onStep:(step,pass)=>{ el('countIn').hidden=true; state.cursor=step.index; renderHand(step.shape,step.window); el('nowChord').textContent=step.shape.label; el('actualChord').textContent=step.sounding?'Sounds as '+step.sounding+(state.capo?' with capo '+state.capo:''):stepInfo(lesson().steps[step.index]); document.querySelectorAll('.step-card').forEach(c=>{ c.classList.toggle('playing',Number(c.dataset.index)===step.index); c.classList.toggle('active',Number(c.dataset.index)===step.index); });
        if(pass!==lastPass){ if(lastPass>=0) brain.event('loop_done'); lastPass=pass; evCursor=0; gCursor=0; const cur=player.current(); if(cur.loopCount>1) setChip(cur.loopCount===Infinity?'LOOPING · PASS '+(pass+1):'PASS '+(pass+1)+' OF '+cur.loopCount); } },
      onTick:(t)=>{ if(t.countingIn||!perfNow) return; const time=t.time; while(gCursor<perfNow.gestures.length&&perfNow.gestures[gCursor].t<=time){ gesture(perfNow.gestures[gCursor]); gCursor++; } while(evCursor<perfNow.events.length&&perfNow.events[evCursor].t<=time){ ringString(perfNow.events[evCursor].string); evCursor++; } },
      onEnd:(reason)=>{ el('countIn').hidden=true; el('guitarDemo').classList.remove('is-playing'); el('playDemo').innerHTML=reason==='pause'?'▶ <span>Resume</span>':'▶ <span>Hear & watch</span>'; document.querySelectorAll('.step-card').forEach(c=>c.classList.remove('playing')); if(state.backing.withDemo&&A.backingRunning()&&reason!=='restart') toggleBacking(false); if(reason==='complete'){ setChip(null); state.cursor=state.selection.from; renderStage(); const r=brain.event('demo_end',{reason}); if(r) applyReply(r,{source:'rules'}); } else if(reason==='pause'){ setChip('PAUSED'); } else if(reason!=='restart'){ setChip(null); } }
    });
    return player;
  }
  function setChip(text,cls){ const c=el('stageChip'); if(!text){ c.hidden=true; c.className='stage-chip'; return; } c.hidden=false; c.textContent=text; c.className='stage-chip'+(cls?' '+cls:''); }
  function play(opts={}){
    let audio; try{ audio=A.ensure(); }catch(e){ tutorSay(e.message); return false; }
    const p=ensurePlayer(); p.stop('restart');
    // opts.resume continues part-way through the selected section without changing what the learner selected.
    let from=Number.isInteger(opts.from)?opts.from:state.selection.from, to=Number.isInteger(opts.to)?opts.to:state.selection.to;
    if(!opts.resume){ state.selection={from,to}; clampSelection(); from=state.selection.from; to=state.selection.to; }
    else { from=Math.max(0,Math.min(from,lastIndex())); to=Math.max(from,Math.min(to,lastIndex())); }
    const loop=opts.loop!==undefined?opts.loop:state.loopCount; const countIn=opts.countIn!==undefined?opts.countIn:state.countIn;
    perfNow=P.compile(lesson(),{capo:state.capo,tempo:state.tempo,from,to}); evCursor=0; gCursor=0; lastPass=-1; resumeStep=null;
    state.stats.demos++; save(); updateStats();
    el('playDemo').innerHTML='⏸ <span>Pause</span>'; el('guitarDemo').classList.add('is-playing'); el('guitarDemo').classList.remove('your-turn'); el('attemptBar').hidden=true; if(!opts.silent) el('confirmBar').hidden=true;
    setChip(state.slow?'SLOW MOTION · '+state.tempo+' BPM':(loop>1?(loop===Infinity?'LOOPING':'PASS 1 OF '+loop):null));
    p.start(perfNow,{loop,countIn});
    if(state.backing.withDemo){ const cur=p.current(); startBacking({startAt:cur.startAt+cur.countIn}); }
    renderRange(); renderSections(); return true;
  }
  function stopPlayback(reason='stopped'){ if(player&&player.isPlaying()){ if(reason==='pause'){ resumeStep=state.cursor; } player.stop(reason); } }
  function togglePlay(){ if(player&&player.isPlaying()){ stopPlayback('pause'); el('playDemo').innerHTML='▶ <span>Resume</span>'; return; } if(resumeStep!==null&&resumeStep>state.selection.from&&resumeStep<=state.selection.to){ const from=resumeStep; resumeStep=null; play({from,to:state.selection.to,resume:true,countIn:false,loop:1}); return; } play(); }

  /* ---------- backing band ---------- */
  function backingState(){ const l=lesson(); const {from,to}=state.selection; return {tempo:state.tempo,capo:state.capo,chords:l.kind==='chord'?l.steps.slice(from,to+1):[],drums:state.backing.drums,bass:state.backing.bass,rhythm:state.backing.rhythm&&l.kind==='chord',click:state.backing.click}; }
  function startBacking(options={}){ try{ A.startBacking(backingState,options); el('backingToggle').textContent='■ Stop backing band'; el('backingNote').textContent='Playing an original practice groove over '+(state.selection.from===state.selection.to?'step '+(state.selection.from+1):'steps '+(state.selection.from+1)+'–'+(state.selection.to+1))+'. Headphones recommended if the microphone is listening.'; }catch(e){ tutorSay(e.message); } }
  function toggleBacking(force){ const shouldStart=typeof force==='boolean'?force:!A.backingRunning(); if(shouldStart){ try{ A.ensure(); }catch(e){ tutorSay(e.message); return; } startBacking({}); } else { A.stopBacking(); el('backingToggle').textContent='▶ Start backing band'; } }
  function applyVolumes(){ const v=state.backing.volumes; A.setLevel('drums',v.drums); A.setLevel('bass',v.bass); A.setLevel('rhythm',v.rhythm); A.setLevel('master',v.master); el('volDrums').value=Math.round(v.drums*100); el('volBass').value=Math.round(v.bass*100); el('volRhythm').value=Math.round(v.rhythm*100); el('volMaster').value=Math.round(v.master*100); el('trackDrums').checked=state.backing.drums; el('trackBass').checked=state.backing.bass; el('trackRhythm').checked=state.backing.rhythm; el('trackClick').checked=state.backing.click; el('backingWithDemo').checked=state.backing.withDemo; }

  /* ---------- speech out ---------- */
  function speak(text){
    if(!prefs().speak||!window.speechSynthesis) return;
    try{ speechSynthesis.cancel(); const u=new SpeechSynthesisUtterance(text.replace(/\*+/g,'').slice(0,650)); u.rate=.98; u.pitch=1.06; u.lang='en-GB';
      u.onstart=()=>{ speaking=true; el('speakingBar').hidden=false; el('avatar').classList.add('speaking'); }; u.onend=u.onerror=()=>{ speaking=false; el('speakingBar').hidden=true; el('avatar').classList.remove('speaking'); };
      speechSynthesis.speak(u); }catch(e){ console.warn(e); }
  }
  function stopSpeaking(){ if(window.speechSynthesis) speechSynthesis.cancel(); speaking=false; el('speakingBar').hidden=true; el('avatar').classList.remove('speaking'); }

  /* ---------- conversation ---------- */
  function addBubble(kind,text,record=true,cls=''){ const container=el('conversation'); const wrapper=document.createElement('div'); wrapper.className='bubble '+kind+(cls?' '+cls:''); const meta=document.createElement('div'); meta.className='bubble-meta'; meta.textContent=kind==='tutor'?'FRET · YOUR GUITAR COACH':'YOU'; wrapper.append(meta); const content=document.createElement('div'); content.textContent=text; wrapper.append(content); container.append(wrapper); container.scrollTop=container.scrollHeight; if(record){ state.messages.push({kind,text,time:new Date().toISOString()}); state.messages=state.messages.slice(-40); save(); } }
  function tutorSay(text,cls){ addBubble('tutor',text,true,cls); speak(text); }
  function remember(items){
    for(const r of T.sanitiseRemember(items)){ const track=state.memory[r.track]; if(r.key){ const existing=track.notes.findIndex(n=>typeof n==='object'&&n.key===r.key); const entry={text:r.text,key:r.key,at:new Date().toISOString()}; if(existing>=0) track.notes[existing]=entry; else track.notes.push(entry); } else if(!track.notes.some(n=>(typeof n==='string'?n:n.text)===r.text)) track.notes.push({text:r.text,at:new Date().toISOString()}); track.notes=track.notes.slice(-30); }
    save(); renderMemory();
  }
  function runAction(a){
    switch(a.type){
      case 'play': play({from:a.from,to:a.to,loop:a.loop,countIn:a.countIn}); return true;
      case 'stop': stopPlayback('stopped'); toggleBacking(false); return true;
      case 'select': stopPlayback('select'); state.selection={from:a.from,to:Number.isInteger(a.to)?a.to:a.from}; state.cursor=a.from; clampSelection(); renderStage(); if(a.confirm){ el('confirmText').textContent='Is this the part you want to practise? ('+el('rangeLabel').textContent+')'; el('confirmBar').hidden=false; } return false;
      case 'tempo': setTempo(a.bpm,{user:false}); return false;
      case 'slow': setSlow(!!a.on); return false;
      case 'hints': a.show?showHints():hideHints(); return false;
      case 'lesson': { if(a.lesson){ try{ const cleaned={...a.lesson,id:String(a.lesson.id||'scratch-'+Date.now()).slice(0,60)}; if(!scratchLessons.some(l=>l.id===cleaned.id)) scratchLessons.push(cleaned); selectLesson(cleaned.id); }catch(e){ console.warn(e); } } else selectLesson(a.id); return false; }
      case 'page': goPage(a.id); return false;
      case 'owner_request': el('builderRequest').value=a.text||''; el('builderFlag').hidden=false; goPage('builder'); return false;
      case 'prefs': for(const k of ['explanation','pace','autoHints','speak']) if(k in a) prefs()[k]=a[k]; renderPrefs(); save(); return false;
      case 'prompt_attempt': el('attemptBar').hidden=false; el('guitarDemo').classList.add('your-turn'); setChip('YOUR TURN','turn'); return false;
      case 'listen': if(a.on) beginAttemptListening(); return false;
      case 'capo': setCapo(a.fret); return false;
    }
    return false;
  }
  function applyReply(reply,meta={}){
    if(!reply) return;
    const actions=T.sanitiseActions(reply.actions,{lesson:lesson()});
    tutorSay(reply.say,reply.honest?'honest':'');
    let playedOrStopped=false; for(const a of actions){ try{ if(runAction(a)) playedOrStopped=true; }catch(e){ console.warn('action failed',a,e); } }
    remember(reply.remember);
    if(resumeAfterTalk&&!playedOrStopped&&resumeStep!==null){ const from=resumeStep; resumeStep=null; play({from,to:state.selection.to,resume:true,countIn:false,loop:1}); }
    resumeAfterTalk=false; save();
  }
  async function askCoach(text){
    if(!llm.on) return brain.handle(text);
    const ctx={lesson:{id:lesson().id,name:lesson().name,kind:lesson().kind,pattern:lesson().pattern,steps:lesson().steps.map((s,i)=>({index:i,label:s.label||s.chord,chord:s.chord,string:s.string,fret:s.fret,shape:s.chord?M.CHORDS[s.chord]:null,sounding:s.chord?M.chordSound(s.chord,state.capo):null})),sections:M.sectionsFor(lesson())},
      capo:state.capo,tempo:state.tempo,baseTempo:state.baseTempo,cursor:state.cursor,selection:state.selection,hintsVisible:!el('hintPanel').classList.contains('hidden'),micOn:A.hasMic(),session:{phase:brain.session.phase,awaiting:brain.session.awaiting},
      memory:{playing:state.memory.playing,teaching:state.memory.teaching},history:state.messages.slice(-12)};
    const controller=new AbortController(); const timeout=setTimeout(()=>controller.abort(),20000);
    try{ const res=await fetch('/api/chat',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({message:text,context:ctx}),signal:controller.signal}); clearTimeout(timeout); if(!res.ok) throw Error('coach '+res.status); const data=await res.json(); if(!data||typeof data.say!=='string') throw Error('bad reply'); if(data.session&&data.session.awaiting!==undefined) brain.session.awaiting=data.session.awaiting; return {say:data.say,actions:data.actions||[],remember:data.remember||[],honest:!!data.honest,source:'ai'}; }
    catch(e){ clearTimeout(timeout); console.warn('AI coach unavailable, using the demo coach',e); setCoachMode(false,'AI coach unreachable — using the rules-based demo coach'); return brain.handle(text); }
  }
  async function message(text){
    text=String(text||'').trim(); if(!text) return;
    addBubble('user',text); state.stats.questions++; updateStats(); el('messageInput').value='';
    if(player&&player.isPlaying()){ resumeStep=state.cursor; resumeAfterTalk=true; player.stop('pause'); }
    stopSpeaking();
    const reply=await askCoach(text); applyReply(reply||{say:'Tell me a little more about where you get stuck.',actions:[],remember:[]});
  }

  /* ---------- preferences, memory, progress ---------- */
  function renderPrefs(){ const p=prefs(); el('explanationLength').value=p.explanation; el('teachingPace').value=p.pace; el('autoHints').checked=!!p.autoHints; el('speakReplies').checked=!!p.speak; el('voiceToggle').textContent=p.speak?'◖))':'◖×'; }
  const noteText=(n)=>typeof n==='string'?n:n.text;
  function renderMemory(){
    const pl=state.memory.playing, te=state.memory.teaching;
    const item=(text,onRemove,fixed)=>{ const div=document.createElement('div'); div.className='memory-item'+(fixed?' fixed':''); const span=document.createElement('span'); span.textContent=text; div.append(span); if(onRemove){ const b=document.createElement('button'); b.textContent='×'; b.title='Forget this'; b.setAttribute('aria-label','Forget: '+text); b.addEventListener('click',()=>{ onRemove(); save(); renderMemory(); }); div.append(b); } return div; };
    const a=el('memoryPlaying'); a.innerHTML='';
    a.append(item(`${pl.instrument.model} · ${pl.instrument.type} · tuning ${pl.instrument.tuning} · capo ${state.capo?'fret '+state.capo:'off'}`,null,true));
    a.append(item(`Knows ${pl.chords.known.join(', ')}. Developing ${pl.chords.developing.join(', ')||'—'}.`,null,true));
    pl.goals.forEach((g,i)=>a.append(item('Goal: '+g,()=>pl.goals.splice(i,1))));
    pl.constraints.forEach((g,i)=>a.append(item('Comfort: '+g,()=>pl.constraints.splice(i,1))));
    pl.notes.forEach((n,i)=>a.append(item(noteText(n),()=>pl.notes.splice(i,1))));
    const b=el('memoryTeaching'); b.innerHTML='';
    b.append(item(`Explanations: ${{short:'one small instruction at a time',medium:'a little more detail',detailed:'detailed'}[te.explanation]}. When struggling: ${te.pace==='gentle'?'slow down and simplify':'offer tips then retry'}. Visual guides: ${te.autoHints?'offered after repeated difficulty':'only when asked'}.`,null,true));
    te.works.forEach((n,i)=>b.append(item('Works: '+n,()=>te.works.splice(i,1))));
    te.confusers.forEach((n,i)=>b.append(item('Confusing: '+n,()=>te.confusers.splice(i,1))));
    te.notes.forEach((n,i)=>b.append(item(noteText(n),()=>te.notes.splice(i,1))));
  }
  function renderProgress(){ const box=el('sectionStats'); box.innerHTML=''; const entries=Object.entries(state.memory.playing.sections).sort((x,y)=>String(y[1].last).localeCompare(String(x[1].last))); if(!entries.length){ box.innerHTML='<p class="note">Nothing yet — pick a section in the studio and press Hear & watch.</p>'; return; } for(const [key,s] of entries.slice(0,12)){ const div=document.createElement('div'); div.className='section-stat'; div.innerHTML='<div><span></span><small></small></div><div><span class="ok"></span> <span class="tricky"></span></div>'; div.querySelector('div>span').textContent=s.label||key; div.querySelector('small').textContent=`${s.demos||0} demo${s.demos===1?'':'s'} · ${s.loops||0} loop${s.loops===1?'':'s'} · last ${s.last?new Date(s.last).toLocaleDateString('en-GB'):'—'}`; div.querySelector('.ok').textContent=s.ok?'✓ '+s.ok:''; div.querySelector('.tricky').textContent=s.tricky?'tricky '+s.tricky:''; box.append(div); } }
  function updateStats(){ el('statSessions').textContent=state.stats.sessions; el('statDemos').textContent=state.stats.demos; el('statQuestions').textContent=state.stats.questions; }
  function renderTempo(){ el('tempoRange').value=state.tempo; el('tempoLabel').textContent=state.tempo+' BPM'+(state.slow?' · slow':''); }
  function setTempo(bpm,{user}={user:true}){ bpm=Math.max(50,Math.min(160,Math.round(bpm))); state.tempo=bpm; if(user){ state.baseTempo=bpm; state.slow=false; } renderTempo(); renderRange(); save(); if(player&&player.isPlaying()){ play({from:state.cursor,to:state.selection.to,loop:1,countIn:false,resume:true}); } }
  function setSlow(on){ state.slow=on; state.tempo=on?Math.max(50,Math.round(state.baseTempo*.6)):state.baseTempo; renderTempo(); renderRange(); save(); if(player&&player.isPlaying()){ play({from:state.cursor,to:state.selection.to,loop:1,countIn:false,resume:true}); } }
  function setCapo(fret){ state.capo=fret; el('capoSelect').value=String(fret); save(); renderStage(); if(player&&player.isPlaying()) play({from:state.cursor,to:state.selection.to,loop:1,countIn:false,resume:true}); }
  function selectLesson(id){ if(!lessons().some(l=>l.id===id)) return; stopPlayback('select'); state.lesson=id; state.cursor=0; state.selection={from:0,to:lastIndex()}; state.memory.playing.lastLesson=id; selectOptions(); save(); renderStage(); }
  function selectOptions(){ const sel=el('exerciseSelect'); sel.innerHTML=''; for(const item of lessons()){ const opt=document.createElement('option'); opt.value=item.id; opt.textContent=item.name; sel.append(opt); } sel.value=state.lesson; }
  function goPage(id){ document.querySelectorAll('.page').forEach(node=>node.classList.toggle('active',node.id==='page-'+id)); document.querySelectorAll('.nav-link').forEach(node=>node.classList.toggle('active',node.dataset.page===id)); const names={studio:'Practice studio',teacher:'Your tutor',guitar:'My guitar',progress:'My progress',builder:'Change the app'}; el('breadcrumb').textContent=names[id]||id; if(id==='progress') renderProgress(); if(location.hash!=='#'+id) history.replaceState(null,'','#'+id); }
  function exportJSON(name,data){ const blob=new Blob([JSON.stringify(data,null,2)],{type:'application/json'}); const url=URL.createObjectURL(blob); const a=document.createElement('a'); a.href=url; a.download=name; a.click(); setTimeout(()=>URL.revokeObjectURL(url),1000); }

  /* ---------- owner change requests (specifications only; never self-modifying) ---------- */
  const STATUSES=['queued','in review','approved','rejected'];
  function renderChanges(){ const box=el('changeHistory'); box.innerHTML=''; const list=state.memory.owner.changes; if(!list.length){ box.textContent='No change requests saved yet.'; box.className='change-history note'; return; } box.className='change-history'; list.slice().reverse().forEach((request)=>{ const div=document.createElement('div'); div.className='change-item'; div.textContent=request.text; const meta=document.createElement('small'); meta.textContent='Saved '+new Date(request.date).toLocaleString('en-GB')+' · '; const sel=document.createElement('select'); sel.className='change-status'; STATUSES.forEach(s=>{ const o=document.createElement('option'); o.value=s; o.textContent=s; sel.append(o); }); sel.value=request.status||'queued'; sel.addEventListener('change',()=>{ request.status=sel.value; save(); }); meta.append(sel); div.append(meta); box.append(div); }); }
  function changeBrief(){ return `You are the coding agent for Fretwise, a local-first prototype AI guitar tutor. Implement the following owner-requested changes safely and incrementally. Preserve current working features and tests. Never insert API secrets into client code. Avoid copyrighted song reproductions unless valid rights are confirmed. The virtual guitar hand demonstration is the DEFAULT; extra fretboard dots appear only on learner request or after repeated difficulty. Never claim pitch-only mic analysis proves a chord was played correctly. New changes should be previewable, tested, and reversible.\n\nOWNER REQUESTS:\n${state.memory.owner.changes.map((c,i)=>`${i+1}. [${c.status||'queued'}] ${c.text}`).join('\n')||'(None saved.)'}\n\nUSER LEARNING PREFERENCES:\n${JSON.stringify(prefs(),null,2)}\n\nDeliver: scope, file changes, runnable tests, accessibility check, regression-risk assessment, deployment/rollback instructions.`; }

  /* ---------- voice in (browser speech recognition; push-to-talk or toggle) ---------- */
  function startRecognition(target){
    const SR=window.SpeechRecognition||window.webkitSpeechRecognition;
    if(!SR){ if(target==='teacher') el('voiceStatus').textContent='Speech recognition is unavailable in this browser (Chrome desktop supports it). Typing works just the same.'; else alert('Voice dictation is unavailable in this browser. Please type the request.'); return null; }
    const r=new SR(); r.lang='en-GB'; r.interimResults=target==='teacher'; r.maxAlternatives=1;
    const button=target==='teacher'?el('voiceInput'):el('builderVoice'); button.classList.add('recording');
    if(target==='teacher'){ el('voiceStatus').textContent='Listening to you… (the tutor pauses while you talk)'; stopSpeaking(); if(player&&player.isPlaying()){ resumeStep=state.cursor; resumeAfterTalk=true; player.stop('pause'); } }
    let finalText='';
    r.onresult=(event)=>{ let interim=''; for(let i=event.resultIndex;i<event.results.length;i++){ const t=event.results[i][0].transcript; if(event.results[i].isFinal) finalText+=t; else interim+=t; } if(target==='teacher') el('messageInput').value=(finalText+' '+interim).trim(); };
    r.onerror=(event)=>{ if(target==='teacher') el('voiceStatus').textContent=event.error==='not-allowed'?'Microphone permission was refused. You can type instead.':'Could not understand your voice ('+event.error+'). You can type instead.'; else alert('Dictation failed: '+event.error); };
    r.onend=()=>{ button.classList.remove('recording'); if(target==='teacher'){ recognition=null; const spoken=(finalText||el('messageInput').value).trim(); if(spoken){ el('voiceStatus').textContent='Heard you — replying.'; message(spoken); } else { if(!/refused|understand/.test(el('voiceStatus').textContent)) el('voiceStatus').textContent='Click the microphone to talk, or hold it to push-to-talk.'; if(resumeAfterTalk&&resumeStep!==null){ const from=resumeStep; resumeStep=null; resumeAfterTalk=false; play({from,to:state.selection.to,resume:true,countIn:false,loop:1}); } } } else builderRecognition=null; };
    try{ r.start(); }catch(e){ button.classList.remove('recording'); return null; }
    return r;
  }
  function toggleVoice(){ if(recognition){ recognition.stop(); return; } if(speaking){ stopSpeaking(); } recognition=startRecognition('teacher'); }

  /* ---------- microphone pitch: honest, monophonic ---------- */
  function beginAttemptListening(){ if(lesson().kind!=='melody'||!A.hasMic()) return; const {from,to}=state.selection; const expected=lesson().steps.slice(from,to+1).map((s,k)=>({midi:M.stepMidis(s,state.capo)[0],label:(s.label||'note')+' ('+M.midiToName(M.stepMidis(s,state.capo)[0])+')',step:from+k})).filter(e=>Number.isFinite(e.midi)); attempt={expected,heard:[],startedAt:Date.now(),lastMidi:null,lastAt:0}; el('listenDetails').textContent='Listening for your attempt: '+expected.length+' note'+(expected.length>1?'s':'')+', one at a time.'; }
  function finishAttempt(){ if(!attempt) return; const payload={expected:attempt.expected,heard:attempt.heard}; attempt=null; const r=brain.event('attempt',payload); if(r) applyReply(r); }
  function pitchDetected(p){
    if(!p){ el('pitchLine').textContent='Listening... play one string'; if(attempt&&Date.now()-attempt.startedAt>9000) finishAttempt(); return; }
    const exact=M.freqToMidi(p.frequency), midi=Math.round(exact); const name=M.midiToName(midi); el('pitchLine').textContent='Detected: '+name+' · approx. '+Math.round(p.frequency)+' Hz · confidence '+Math.round(p.confidence*100)+'%';
    if((player&&player.isPlaying())||A.backingRunning()||speaking) return;   // never grade while the app itself is making sound
    const now=Date.now();
    if(attempt){ if(attempt.lastMidi===null||Math.abs(exact-attempt.lastMidi)>=.5||now-attempt.lastAt>700){ attempt.heard.push(exact); attempt.lastMidi=exact; attempt.lastAt=now; el('listenDetails').textContent='Heard '+attempt.heard.length+' of '+attempt.expected.length+' notes…'; if(attempt.heard.length>=attempt.expected.length) setTimeout(finishAttempt,250); } else attempt.lastAt=now; return; }
    if(lesson().kind!=='melody') return;
    const target=M.stepMidis(lesson().steps[state.cursor],state.capo)[0]; if(!Number.isFinite(target)) return; const diff=Math.abs(exact-target);
    if(now-lastPitchAt<900) return; lastPitchAt=now;
    if(diff<.55){ missed=0; if(lastSuccess!==''+state.cursor){ lastSuccess=''+state.cursor; el('listenDetails').textContent='That note sounds close! Nice work. Select the next note when you’re ready.'; } }
    else { missed++; if(missed>=3&&prefs().autoHints&&el('hintPanel').classList.contains('hidden')){ showHints('auto'); missed=0; } }
  }
  async function toggleMic(){ if(A.hasMic()){ A.stopMic(); attempt=null; el('listenToggle').textContent='Start listening'; el('listenHeading').textContent='Ready when you are'; el('pitchLine').textContent='No note detected'; return; } try{ await A.startMic(pitchDetected); el('listenToggle').textContent='■ Stop listening'; el('listenHeading').textContent='Listening to your guitar'; el('listenDetails').textContent='This version identifies individual notes, not full chord accuracy. For best results play one note at a time, and wear headphones if the teacher or band is playing.'; }catch(e){ el('listenDetails').textContent='Microphone unavailable: '+e.message+' (use localhost/HTTPS and grant permission).'; } }

  /* ---------- AI coach availability ---------- */
  function setCoachMode(on,text){ llm.on=on; el('coachMode').textContent=text||(on?'AI coach connected · '+(llm.model||'server'):'Demo coach · rules-based, works offline'); el('coachStatusText').textContent=on?'AI coach connected through the local server ('+(llm.model||'model')+'). Keys stay on the server. If it becomes unreachable, the rules-based coach takes over automatically.':'Rules-based demo coach. Start node server.mjs with an API key to enable the AI coach; typed lessons always work without it.'; }
  async function detectCoach(){ try{ const res=await fetch('/api/status',{cache:'no-store'}); if(!res.ok) throw 0; const s=await res.json(); llm.model=s.model||null; setCoachMode(!!s.llm,s.llm?undefined:'Demo coach · server running without an API key'); }catch(e){ setCoachMode(false); } }

  /* ---------- events ---------- */
  document.querySelectorAll('[data-page]').forEach(button=>button.addEventListener('click',()=>goPage(button.dataset.page)));
  el('playDemo').addEventListener('click',togglePlay);
  el('stopDemo').addEventListener('click',()=>{ stopPlayback('stopped'); resumeStep=null; el('playDemo').innerHTML='▶ <span>Hear & watch</span>'; setChip(null); if(state.backing.withDemo) toggleBacking(false); });
  el('repeatDemo').addEventListener('click',()=>{ resumeStep=null; play(); });
  el('stepBack').addEventListener('click',()=>{ stopPlayback('select'); const i=Math.max(0,state.cursor-1); state.cursor=i; state.selection={from:i,to:i}; save(); renderStage(); });
  el('stepNext').addEventListener('click',()=>{ stopPlayback('select'); const i=Math.min(lastIndex(),state.cursor+1); state.cursor=i; state.selection={from:i,to:i}; save(); renderStage(); });
  el('lastTwo').addEventListener('click',()=>{ const to=Math.max(state.cursor,state.selection.from); const from=Math.max(0,to-1); state.selection={from,to}; state.cursor=from; renderStage(); play({from,to,loop:3,countIn:true}); });
  el('showHints').addEventListener('click',()=>el('hintPanel').classList.contains('hidden')?showHints():hideHints());
  el('hideHints').addEventListener('click',hideHints);
  el('loopCount').addEventListener('change',e=>{ state.loopCount=e.target.value==='inf'?Infinity:Number(e.target.value); save(); });
  el('countInToggle').addEventListener('change',e=>{ state.countIn=e.target.checked; save(); });
  el('slowToggle').addEventListener('click',()=>setSlow(!state.slow));
  el('confirmYes').addEventListener('click',()=>{ el('confirmBar').hidden=true; brain.session.awaiting='confirm_section'; message('Yes, that’s the part.'); });
  el('confirmEarlier').addEventListener('click',()=>{ el('confirmBar').hidden=true; message('Earlier'); });
  el('confirmLater').addEventListener('click',()=>{ el('confirmBar').hidden=true; message('Later'); });
  el('confirmNo').addEventListener('click',()=>{ el('confirmBar').hidden=true; brain.session.awaiting='confirm_section'; message('No, that’s not the part.'); });
  el('attemptOk').addEventListener('click',()=>{ el('attemptBar').hidden=true; el('guitarDemo').classList.remove('your-turn'); setChip(null); brain.session.awaiting='attempt_report'; message('Got it.'); });
  el('attemptTricky').addEventListener('click',()=>{ el('attemptBar').hidden=true; el('guitarDemo').classList.remove('your-turn'); setChip(null); brain.session.awaiting='attempt_report'; message('That was tricky.'); });
  el('attemptAgain').addEventListener('click',()=>{ el('attemptBar').hidden=true; el('guitarDemo').classList.remove('your-turn'); setChip(null); message('Show me again.'); });
  el('backingToggle').addEventListener('click',()=>toggleBacking());
  for(const [id,key] of [['trackDrums','drums'],['trackBass','bass'],['trackRhythm','rhythm'],['trackClick','click'],['backingWithDemo','withDemo']]) el(id).addEventListener('change',e=>{ state.backing[key]=e.target.checked; save(); });
  for(const [id,key] of [['volDrums','drums'],['volBass','bass'],['volRhythm','rhythm'],['volMaster','master']]) el(id).addEventListener('input',e=>{ state.backing.volumes[key]=Number(e.target.value)/100; try{ A.setLevel(key,state.backing.volumes[key]); }catch(err){} save(); });
  el('exerciseSelect').addEventListener('change',e=>{ selectLesson(e.target.value); tutorSay(lesson().description); });
  el('capoSelect').addEventListener('change',e=>setCapo(Number(e.target.value)));
  el('tempoRange').addEventListener('input',e=>setTempo(Number(e.target.value),{user:true}));
  el('sendMessage').addEventListener('click',()=>message(el('messageInput').value));
  el('messageInput').addEventListener('keydown',e=>{ if(e.key==='Enter') message(el('messageInput').value); });
  document.querySelectorAll('[data-quick]').forEach(b=>b.addEventListener('click',()=>message(b.dataset.quick)));
  el('voiceInput').addEventListener('click',()=>{ if(holdMode){ holdMode=false; return; } toggleVoice(); });
  el('voiceInput').addEventListener('pointerdown',()=>{ holdTimer=setTimeout(()=>{ holdMode=true; if(!recognition) recognition=startRecognition('teacher'); },350); });
  const endHold=()=>{ clearTimeout(holdTimer); if(holdMode&&recognition){ recognition.stop(); setTimeout(()=>{ holdMode=false; },50); } };
  el('voiceInput').addEventListener('pointerup',endHold); el('voiceInput').addEventListener('pointerleave',endHold);
  el('stopSpeaking').addEventListener('click',stopSpeaking);
  el('voiceToggle').addEventListener('click',()=>{ prefs().speak=!prefs().speak; renderPrefs(); save(); if(!prefs().speak) stopSpeaking(); });
  el('listenToggle').addEventListener('click',toggleMic);
  el('explanationLength').addEventListener('change',e=>{ prefs().explanation=e.target.value; save(); renderMemory(); });
  el('teachingPace').addEventListener('change',e=>{ prefs().pace=e.target.value; save(); renderMemory(); });
  el('autoHints').addEventListener('change',e=>{ prefs().autoHints=e.target.checked; save(); renderMemory(); });
  el('speakReplies').addEventListener('change',e=>{ prefs().speak=e.target.checked; renderPrefs(); save(); });
  el('resetPrefs').addEventListener('click',()=>{ Object.assign(prefs(),T.defaultMemory().teaching); renderPrefs(); renderMemory(); save(); });
  el('memoryAdd').addEventListener('click',()=>{ const text=el('memoryAddText').value.trim(); if(!text) return; remember([{track:el('memoryAddTrack').value,text}]); el('memoryAddText').value=''; });
  el('memoryAddText').addEventListener('keydown',e=>{ if(e.key==='Enter') el('memoryAdd').click(); });
  el('exportProfile').addEventListener('click',()=>exportJSON('fretwise-learning-profile.json',{exported:new Date().toISOString(),version:T.MEMORY_VERSION,capo:state.capo,tempo:state.baseTempo,lesson:state.lesson,memory:state.memory,statistics:state.stats}));
  el('builderVoice').addEventListener('click',()=>{ if(builderRecognition){ builderRecognition.stop(); return; } builderRecognition=startRecognition('builder'); if(builderRecognition){ builderRecognition.onresult=(ev)=>{ const spoken=ev.results?.[0]?.[0]?.transcript||''; el('builderRequest').value=(el('builderRequest').value+' '+spoken).trim(); }; } });
  el('saveRequest').addEventListener('click',()=>{ const t=el('builderRequest').value.trim(); if(!t){ alert('Please describe a change first.'); return; } state.memory.owner.changes.push({text:t,date:new Date().toISOString(),status:'queued'}); el('builderRequest').value=''; el('builderFlag').hidden=true; save(); renderChanges(); });
  el('copyChangeBrief').addEventListener('click',async()=>{ try{ await navigator.clipboard.writeText(changeBrief()); el('copyChangeBrief').textContent='✓ Copied for Claude'; setTimeout(()=>el('copyChangeBrief').textContent='Copy Claude change brief',2400); }catch(e){ alert('Copy unavailable in this browser. Use Download changes instead.'); } });
  el('downloadChanges').addEventListener('click',()=>exportJSON('fretwise-change-requests.json',{requests:state.memory.owner.changes,brief:changeBrief()}));
  el('guitarUpload').addEventListener('change',e=>{ const file=e.target.files?.[0]; if(!file) return; if(!file.type.startsWith('image/')) return alert('Select an image file.'); const image=new Image(); const url=URL.createObjectURL(file); image.onload=()=>{ const canvas=document.createElement('canvas'),max=900,scale=Math.min(1,max/Math.max(image.width,image.height)); canvas.width=Math.round(image.width*scale); canvas.height=Math.round(image.height*scale); canvas.getContext('2d').drawImage(image,0,0,canvas.width,canvas.height); state.imageData=canvas.toDataURL('image/jpeg',.68); el('guitarReference').src=state.imageData; URL.revokeObjectURL(url); save(); }; image.src=url; });
  el('lessonImport').addEventListener('change',async e=>{ const file=e.target.files?.[0]; if(!file) return; try{ if(file.size>200000) throw Error('Limit imports to 200 KB.'); const imported=M.validateImportedExercise(JSON.parse(await file.text())); state.importedLessons.push(imported); selectLesson(imported.id); tutorSay('Your arrangement is loaded with '+imported.steps.length+' steps'+(imported.sections?' and '+imported.sections.length+' sections':'')+'. Pick a section and I’ll play it back so you can confirm it before we practise it. Please make sure you have permission to use this arrangement.'); }catch(err){ alert('Could not import lesson: '+err.message); } e.target.value=''; });
  window.addEventListener('hashchange',()=>{ const v=location.hash.slice(1); if(PAGES.includes(v)) goPage(v); });
  window.addEventListener('pagehide',()=>{ A.stopBacking(); A.stopMic(); stopSpeaking(); });
  document.addEventListener('keydown',e=>{ if(e.code==='Space'&&!/input|textarea|select|button/i.test(document.activeElement?.tagName||'')){ e.preventDefault(); togglePlay(); } });

  /* ---------- boot ---------- */
  if(!lessons().some(l=>l.id===state.lesson)) state.lesson='capo';
  clampSelection();
  state.messages.forEach(m=>addBubble(m.kind,m.text,false));
  if(!state.messages.length) addBubble('tutor','Hey! I’m Fret, your guitar coach. I know you’re using a Yamaha F310 and working on chord changes and Sultans of Swing. I’ll demonstrate on my guitar first; if something’s confusing, just tell me and I’ll slow down and change how I explain it.',false);
  else if(state.migratedFrom){ addBubble('tutor','Welcome back — I’ve kept everything you taught me and sorted it into “how you play” and “how you like to be taught”. Check it on the Your tutor page.',false); delete state.migratedFrom; }
  const flag=document.createElement('div'); flag.className='builder-flag'; flag.id='builderFlag'; flag.hidden=true; flag.textContent='Classified as a software change — saved here, not as a lesson preference.'; el('builderRequest').parentNode.insertBefore(flag,el('builderRequest'));
  initWood(); selectOptions(); el('capoSelect').value=String(state.capo); renderTempo(); renderStage(); renderPrefs(); renderMemory(); renderChanges(); updateStats(); applyVolumes(); if(state.imageData) el('guitarReference').src=state.imageData;
  const initial=location.hash.slice(1); goPage(PAGES.includes(initial)?initial:'studio'); save(); detectCoach();
  window.FretwiseDebug={state,lesson,play,stopPlayback,brain,player:()=>player,perf:()=>perfNow,hand:()=>hand.state(),detectPitch:A.detectPitch,message};
})();
