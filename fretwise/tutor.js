/* The tutor brain. Pure logic, no DOM: it reads a context snapshot, returns {say, actions, remember} and keeps a small
   teaching state machine:  goal → passage → demo → attempt → observe → correct → retry → advance.
   The app executes actions (play, loop, select, hints…) from a fixed allow-list; chat text is never executed as code.
   When a server-side AI coach is configured it returns the SAME contract, so the app treats both identically. */
window.FRETWISE_TUTOR = (() => {
  const MEMORY_VERSION = 2;
  const ACTION_TYPES = ['play','stop','select','tempo','slow','hints','lesson','page','owner_request','prefs','prompt_attempt','listen','capo'];

  function defaultMemory(){
    return {
      version:MEMORY_VERSION,
      playing:{
        instrument:{model:'Yamaha F310',type:'6-string steel-string acoustic',tuning:'E A D G B e',markers:'Dot inlays, double dots at fret 12',handedness:'right'},
        chords:{known:['Am','E','Em','D','Dm','Fmaj7','C'],developing:['G','A'],avoid:[]},
        constraints:['Full barre chords are uncomfortable; limited finger stretch.'],
        goals:['Sultans of Swing — a personal or licensed arrangement, capo 3 or 5','Barre chords, built up from smaller F shapes','Clean, comfortable G and A changes'],
        sections:{},            // "lessonId:from-to" → {demos, loops, ok, tricky, last}
        notes:['Advanced beginner. Practises a personal Sultans of Swing arrangement with a capo on fret 3 or 5.']
      },
      teaching:{
        explanation:'short',pace:'gentle',autoHints:true,speak:true,demoFirst:true,
        works:['One short instruction, then a demonstration, then time to try.'],
        confusers:['Long explanations and several instructions at once.'],
        notes:[],lastReviewed:null
      },
      owner:{changes:[]}
    };
  }

  /* Upgrade a stored state object from the v0.1 prototype without losing anything the learner built up. */
  function migrate(stored){
    const s=stored||{};
    if(s.memory&&s.memory.version===MEMORY_VERSION) return s;
    const memory=defaultMemory();
    const old=s.prefs||{};
    for(const k of ['explanation','pace','autoHints','speak']) if(old[k]!==undefined) memory.teaching[k]=old[k];
    const defaultsV1=['Advanced beginner: knows Am, E, Em, D, Dm, Fmaj7, C; working on G and A.','Barre chords and finger stretching are challenging. Offer manageable steps and a guitar setup check.','Working towards Sultans of Swing with a capo, often fret 3 or 5.','Can feel overwhelmed by complex teaching. Start with one short instruction and a demonstration.'];
    for(const line of (s.knowledge||[])){
      if(defaultsV1.includes(line)) continue;          // already represented in the structured defaults
      if(/teach|instruction|demonstrat|overwhelm|explain|jargon|plain english|feedback on/i.test(line)) memory.teaching.notes.push(line); else memory.playing.notes.push(line);
    }
    memory.owner.changes=(s.changes||[]).map(c=>({...c,status:c.status||'queued'}));
    if(Number.isInteger(s.capo)) memory.playing.instrument.capo=s.capo;
    return {...s,memory,migratedFrom:s.memory?s.memory.version:'v01'};
  }

  /* ---------- language helpers ---------- */
  const FINGER_NAMES={1:'first finger',2:'second finger',3:'third finger',4:'little finger'};
  const STRING_WORDS=['low E string','A string','D string','G string','B string','high e string'];
  function fingerSteps(music,chordId){
    const c=music.CHORDS[chordId]; if(!c) return [];
    const barre=music.barreFor(c.frets,c.fingers); const out=[];
    if(barre) out.push(`Lay your first finger flat across fret ${barre.fret}, from the ${STRING_WORDS[barre.from]} to the ${STRING_WORDS[barre.to]}, with the bony edge of the finger doing the pressing.`);
    const seen=new Set();
    c.fingers.forEach((f,s)=>{ if(f<1||c.frets[s]<1) return; if(barre&&f===1) return; const key=f+':'+c.frets[s]; if(seen.has(key)) return; seen.add(key); const others=c.fingers.map((ff,ss)=>ff===f&&ss!==s&&c.frets[ss]===c.frets[s]?ss:-1).filter(x=>x>=0); out.push(`${cap(FINGER_NAMES[f])} on the ${STRING_WORDS[s]}${others.length?' (it also covers the '+others.map(o=>STRING_WORDS[o]).join(' and ')+')':''}, just behind fret ${c.frets[s]}.`); });
    const lowest=c.frets.findIndex(f=>f>=0);
    out.push(lowest>0?`Strum from the ${STRING_WORDS[lowest]} down — leave the ${c.frets.slice(0,lowest).map((_,i)=>STRING_WORDS[i]).join(' and ')} out.`:'Strum all six strings.');
    return out;
  }
  const cap=s=>s.charAt(0).toUpperCase()+s.slice(1);
  function sentences(text){ return String(text).match(/[^.!?]+[.!?]+["”']?|[^.!?]+$/g)||[String(text)]; }
  /* Enforce the learner's preferred instruction length. parts = {instruction, why, ask} */
  function compose(parts,level){
    const p=parts||{}; const bits=[];
    if(level==='short'){
      // One instruction, one optional question. Never a paragraph.
      if(p.instruction) bits.push(sentences(p.instruction.trim())[0].trim());
      if(p.ask) bits.push(sentences(p.ask.trim())[0].trim());
      return bits.join(' ');
    }
    if(p.instruction) bits.push(p.instruction.trim());
    if(p.why) bits.push(p.why.trim());
    if(p.ask) bits.push(p.ask.trim());
    let text=bits.join(' ');
    if(level==='medium'){ const s=sentences(text); if(s.length>4) text=s.slice(0,4).join('').trim(); }
    return text;
  }

  /* ---------- intent classification (rules; the AI coach does its own, but these keep typed lessons working offline) ---------- */
  const RX={
    owner:/\b(add|change|edit|build|make|remove|rename|move)\b.*\b(app|software|button|screen|feature|page|menu|setting|interface|ui)\b|\b(the app|owner mode|change request)\b/i,
    pain:/\b(hurt|hurts|ache|aching|pain|painful|sore|cramp|numb)\b/i,
    overwhelmed:/too much|overwhelm|confus|one step|one thing at a time|simpl|shorter|one ear|too fast|slow(er)? explanation|too long|keep it brief|less detail/i,
    moreDetail:/more detail|explain more|tell me more|why does|in depth|longer explanation|more information/i,
    plain:/plain english|no jargon|what does that mean|jargon|in simple words|simple terms/i,
    works:/(that|this) (way|works|helps|helped|is better|was better|was great|clicks)|keep (doing|teaching) (that|this)|works for me|that's helpful|that is helpful/i,
    notWorking:/(doesn't|does not|isn't|is not|not) (work|help)(ing)?( for me)?|that didn't help|stop doing that/i,
    showVisual:/\b(show|see|need|give|open|display|turn on)\b.*\b(dot|dots|fret help|finger (position|placement|guide|diagram)|diagram|fingering|chart)\b|which (fret|finger|string)|where (do|does|should).*(finger|go)|show me (the )?(fingers|dots)/i,
    hideVisual:/\b(hide|close|remove|turn off|get rid of|clear)\b.*\b(dot|dots|guide|diagram|hint|help|chart|fingers)\b|\bhide (it|them|that)\b/i,
    barre:/\bbarre?\b|bar chord|full f\b|\bf chord\b|stretch|can't (reach|press|hold)|cannot (reach|press|hold)|won't ring|buzz/i,
    song:/sultans|dire straits|knopfler|\b(solo|riff|intro|lick|song) (of|from)\b|play (me )?the song|the real (song|version|recording)/i,
    stop:/\b(stop|pause|halt|enough|quiet|shut up)\b/i,
    slower:/\bslow(er| down| it down)?\b|too fast|half speed|slow motion/i,
    faster:/\bfaster\b|speed (it )?up|quicker|full speed|normal speed/i,
    again:/\b(again|repeat|once more|one more time|replay|play it|play that|show me|demonstrat|hear it|watch)\b/i,
    lastNotes:/last (two|2|three|3|four|4) (notes|chords|steps)|repeat the last|those last/i,
    next:/\b(next|move on|carry on|continue|onwards|what's next)\b/i,
    back:/\b(back|previous|go back|earlier|before that)\b/i,
    later:/\b(later|after that|further on|the next bit)\b/i,
    top:/from the (top|start|beginning)|whole (thing|lesson|exercise)|all of it|start again/i,
    section:/\b(section|bar|bars|phrase|part|half)\b/i,
    loop:/\bloop\b|\bon repeat\b|keep (playing|going)|over and over|(\d+|two|three|four|five|ten) times/i,
    yes:/^(yes|yeah|yep|yup|ok|okay|sure|go on|go ahead|please|do it|that's it|that's the one|correct|right|exactly|y)\b/i,
    no:/^(no|nope|nah|wrong|not that|that's not it|not quite|n)\b/i,
    gotIt:/\b(got it|nailed it|that worked|did it|clean|sounds (good|right|fine)|easy|i can do (that|it))\b/i,
    tricky:/\b(tricky|hard|difficult|struggl|can't|cannot|couldn't|missed|messy|muddy|buzz|muted|dead|wrong note|not clean|keep (missing|getting))\b/i,
    greeting:/^(hi|hello|hey|hiya|good (morning|evening|afternoon)|yo)\b/i,
    capoFact:/capo (on|at|is on|is at) (fret )?(\d+)|(\d+)(st|nd|rd|th) fret capo|no capo/i,
    fact:/\b(my (guitar|action|strings|hands?|fingers?|wrist|thumb)|i am|i'm|i have|i've got|i play|i'm left|left[- ]handed|right[- ]handed|i prefer|i like|i practise|i practice|i usually)\b/i,
    chordQ:/\b(how|what|which|where|show|teach|play|form|finger)\b.*\b(am|a|e|em|d|dm|c|g|f|fmaj7|f major 7|a minor|e minor|d minor)\b( chord| shape)?/i,
    capoQ:/capo/i,
    strumQ:/strum|pattern|rhythm|up ?stroke|down ?stroke/i,
    changeQ:/chord change|changing chords|switch(ing)? (between|chords)|transition/i,
    teachMeta:/\b(teach|explain|instruct|your (method|style|approach))\b/i
  };
  const NUMBER_WORDS={one:1,two:2,three:3,four:4,five:5,six:6,seven:7,eight:8,nine:9,ten:10,first:1,second:2,third:3,fourth:4,fifth:5};
  function wordNumber(w){ if(!w) return null; const n=Number(w); if(Number.isFinite(n)) return n; return NUMBER_WORDS[String(w).toLowerCase()]||null; }
  function chordInText(music,text){ const t=' '+text.toLowerCase().replace(/[?!.,;:'"”“’]/g,' ').replace(/\s+/g,' ')+' '; const names={'a minor':'Am','e minor':'Em','d minor':'Dm','f major 7':'Fmaj7','f major seven':'Fmaj7','fmaj7':'Fmaj7','full f':'F','mini barre':'Fmini','small f':'Fsmall'}; for(const k in names) if(t.includes(' '+k+' ')||t.includes(' '+k+' chord')) return names[k]; const m=t.match(/\s([a-g]m?)(?:\s+(chord|shape))?\s/); if(m){ const id=m[1].charAt(0).toUpperCase()+m[1].slice(1); if(music.CHORDS[id]) return id; } return null; }

  function classify(text,ctx){
    const q=String(text||'').trim(); const l=q.toLowerCase(); const awaiting=ctx.session.awaiting;
    if(!q) return {intent:'empty'};
    if(RX.owner.test(l)&&!RX.showVisual.test(l)) return {intent:'owner_change'};
    if(RX.pain.test(l)) return {intent:'pain'};
    if(awaiting&&RX.yes.test(l)) return {intent:'yes'};
    if(awaiting&&RX.no.test(l)) return {intent:'no'};
    if(RX.hideVisual.test(l)) return {intent:'visual_hide'};
    if(RX.showVisual.test(l)) return {intent:'visual_show'};
    if(RX.plain.test(l)) return {intent:'plain_english'};
    if(RX.overwhelmed.test(l)) return {intent:'pref_short'};
    if(RX.moreDetail.test(l)) return {intent:'pref_detail'};
    if(RX.notWorking.test(l)) return {intent:'pref_not_working'};
    if(RX.works.test(l)) return {intent:'pref_works'};
    if(RX.song.test(l)) return {intent:'song'};
    if(RX.barre.test(l)) return {intent:'barre'};
    if(RX.stop.test(l)&&!RX.again.test(l)) return {intent:'stop'};
    if(RX.lastNotes.test(l)){ const m=l.match(/last (two|2|three|3|four|4)/); return {intent:'last_notes',count:wordNumber(m&&m[1])||2}; }
    if(RX.slower.test(l)) return {intent:'slower',slowMotion:/slow motion|half speed/.test(l)};
    if(RX.faster.test(l)) return {intent:'faster'};
    if(RX.top.test(l)) return {intent:'from_top'};
    if(RX.loop.test(l)){ const m=l.match(/(\d+|two|three|four|five|ten) times/); return {intent:'loop',count:m?wordNumber(m[1]):Infinity}; }
    if(RX.section.test(l)){ const m=l.match(/(?:section|bar|phrase|part)s?\s+(\d+|one|two|three|four|five|six|first|second|third|fourth)/); const half=l.includes('first half')?'first':l.includes('second half')?'second':null; if(m||half) return {intent:'section',number:m?wordNumber(m[1]):null,half}; if(RX.again.test(l)) return {intent:'again'}; }
    if(RX.next.test(l)) return {intent:'next'};
    if(RX.back.test(l)||RX.later.test(l)) return {intent:RX.back.test(l)?'earlier':'later'};
    if(RX.gotIt.test(l)) return {intent:'got_it'};
    if(RX.tricky.test(l)) return {intent:'tricky'};
    if(RX.again.test(l)) return {intent:'again'};
    if(RX.capoFact.test(l)){ const m=l.match(/capo (?:on|at|is on|is at) (?:fret )?(\d+)/)||l.match(/(\d+)(?:st|nd|rd|th) fret capo/); return {intent:'capo_fact',capo:l.includes('no capo')?0:(m?Number(m[1]):null)}; }
    if(RX.changeQ.test(l)) return {intent:'change_question'};
    if(RX.strumQ.test(l)) return {intent:'strum_question'};
    const chord=chordInText(ctx.music,l);
    if(RX.capoQ.test(l)&&!/^(how|show|teach)/.test(l)) return {intent:'capo_question',chord};
    if(chord&&RX.chordQ.test(l)) return {intent:'chord_question',chord};
    if(RX.teachMeta.test(l)) return {intent:'teach_meta'};
    if(RX.fact.test(l)) return {intent:'fact'};
    if(RX.greeting.test(l)) return {intent:'greeting'};
    return {intent:'unknown'};
  }

  /* ---------- the brain ---------- */
  function create(opts){
    const music=opts.music; const getContext=opts.getContext;
    const session={phase:'idle',awaiting:null,passage:null,attempts:0,tricky:0,correctionIndex:0,fingerWalk:null,loopsSince:0,lastSay:''};
    const sectionKey=(lessonId,from,to)=>`${lessonId}:${from}-${to}`;
    function stats(ctx,from,to){ const key=sectionKey(ctx.lesson.id,from,to); const mem=ctx.memory.playing.sections; mem[key]=mem[key]||{label:sectionLabel(ctx,from,to),demos:0,loops:0,ok:0,tricky:0,last:null}; mem[key].last=new Date().toISOString(); return mem[key]; }
    function sectionLabel(ctx,from,to){ const sec=music.sectionsFor(ctx.lesson).find(s=>s.from===from&&s.to===to); if(sec) return ctx.lesson.name+' · '+sec.name; const names=ctx.lesson.steps.slice(from,to+1).map(s=>s.label||s.chord||'note'); return ctx.lesson.name+' · steps '+(from+1)+'–'+(to+1)+' ('+names.join(', ')+')'; }
    function level(ctx){ return ctx.memory.teaching.explanation||'short'; }
    let currentIntent='unknown';
    function reply(ctx,parts,actions=[],remember=[],extra={}){ const say=compose(parts,level(ctx)); session.lastSay=say; return {intent:currentIntent,say,actions,remember,...extra}; }
    function stepLabel(ctx,i){ const s=ctx.lesson.steps[i]; return s?(s.label||(s.chord?(music.CHORDS[s.chord]?.name||s.chord):'note')):'that step'; }
    function currentChord(ctx){ const s=ctx.lesson.steps[ctx.cursor]; return s&&s.chord?s.chord:null; }
    function selection(ctx){ return ctx.selection&&Number.isInteger(ctx.selection.from)?ctx.selection:{from:ctx.cursor,to:ctx.cursor}; }
    function playSelection(ctx,extra={}){ const sel=selection(ctx); const st=stats(ctx,sel.from,sel.to); st.demos++; session.phase='demo'; session.passage={lessonId:ctx.lesson.id,...sel}; return {type:'play',from:sel.from,to:sel.to,loop:extra.loop||1,countIn:!!extra.countIn}; }
    function describeSelection(ctx){ const sel=selection(ctx); return sel.from===sel.to?stepLabel(ctx,sel.from):`steps ${sel.from+1} to ${sel.to+1} (${stepLabel(ctx,sel.from)} to ${stepLabel(ctx,sel.to)})`; }
    function yourTurn(ctx){ session.phase='attempt'; session.awaiting='attempt_report'; const melody=ctx.lesson.kind==='melody'; const ask=melody?'Your turn — play those notes once, slowly, then tell me how it went.':'Your turn — strum it slowly, one shape per bar, then tell me how it went.'; const actions=[{type:'prompt_attempt'}]; if(melody&&ctx.micOn) actions.push({type:'listen',on:true}); return reply(ctx,{instruction:ask},actions); }

    /* One corrective action at a time, rotating so the learner never gets a wall of tips. */
    function correct(ctx,issue){
      session.phase='correct'; session.tricky++;
      const sel=selection(ctx); const st=stats(ctx,sel.from,sel.to); st.tricky++;
      const remember=[{track:'playing',text:`Found ${sectionLabel(ctx,sel.from,sel.to)} tricky (${st.tricky}×).`,key:'tricky:'+sectionKey(ctx.lesson.id,sel.from,sel.to)}];
      const order=['slow','isolate','hints','walk'];
      let choice=order[session.correctionIndex%order.length]; session.correctionIndex++;
      if(choice==='hints'&&(!ctx.memory.teaching.autoHints||ctx.hintsVisible)) { choice=order[session.correctionIndex%order.length]; session.correctionIndex++; }
      if(choice==='walk'&&!currentChord(ctx)) choice='slow';
      if(issue&&issue.instruction){ session.phase='retry'; session.awaiting='attempt_report'; return reply(ctx,{instruction:issue.instruction,why:issue.why,ask:issue.ask||'Try just that bit, then tell me.'},issue.actions||[{type:'prompt_attempt'}],remember); }
      if(choice==='slow'){ const bpm=Math.max(50,Math.round(ctx.tempo*0.8)); session.phase='retry'; session.awaiting='attempt_report'; return reply(ctx,{instruction:`No problem — let's slow it right down to ${bpm} and I'll play it twice.`,why:'Slow and clean beats fast and messy; speed comes back on its own.',ask:'Watch, then try it at this speed.'},[{type:'tempo',bpm},playSelection(ctx,{loop:2,countIn:true}),{type:'prompt_attempt'}],remember); }
      if(choice==='isolate'){ const to=Math.min(ctx.lesson.steps.length-1,Math.max(sel.from,ctx.cursor)); const from=Math.max(sel.from,to-1); session.phase='retry'; session.awaiting='attempt_report'; return reply(ctx,{instruction:`Let's isolate just ${from===to?stepLabel(ctx,to):stepLabel(ctx,from)+' into '+stepLabel(ctx,to)} and loop it three times.`,why:'Practising the exact join is what makes the change clean.',ask:'Play along with the loop if you can.'},[{type:'select',from,to},{type:'play',from,to,loop:3,countIn:true},{type:'prompt_attempt'}],remember); }
      if(choice==='hints'){ session.awaiting='hint_offer'; return reply(ctx,{instruction:'Would it help to see the finger positions on a diagram for this one?',why:'I keep it hidden unless you want it, so you learn from watching the hand.',ask:'Say yes and I’ll put it on screen.'},[],remember); }
      // finger-by-finger walkthrough
      const chord=currentChord(ctx); const steps=fingerSteps(music,chord); session.fingerWalk={chord,index:0,steps}; session.awaiting='finger_walk';
      return reply(ctx,{instruction:`One finger at a time for ${music.CHORDS[chord].name}: ${steps[0]}`,ask:'Say "next" when that finger is in place.'},[],remember);
    }

    function handle(text){
      const ctx=getContext(); const c=classify(text,{session,music});
      const lvl=level(ctx); const intent=c.intent; currentIntent=intent; const prefsRemember=(t)=>({track:'teaching',text:t});
      if(['owner_change','song','greeting','unknown','fact','teach_meta'].includes(intent)) session.awaiting=null;
      switch(intent){
        case 'empty': return null;
        case 'owner_change': return reply(ctx,{instruction:'That sounds like a change to the app rather than a lesson, so I’ve opened Owner Mode with your words ready to save as a change request.',why:'Lesson preferences and software changes are kept separate on purpose; nothing in the code changes without review.',ask:''},[{type:'owner_request',text}],[],{intent});
        case 'pain': session.phase='idle'; session.awaiting='comfort'; return reply(ctx,{instruction:'Stop and shake your hand out — never play through pain.',why:'Pain usually means too much pressure, a bent wrist or high string action; a microphone can’t tell which, so we check by feel.',ask:'When it eases, shall we try the lighter Fmaj7 shape, or rest for today?'},[{type:'stop'}],[{track:'playing',text:'Reported hand discomfort while practising; keep pressure light, sessions short, and consider a setup check on the Yamaha.'}],{intent});
        case 'yes': return onYes(ctx);
        case 'no': return onNo(ctx);
        case 'visual_show': return reply(ctx,{instruction:`Here’s the finger guide for ${describeShape(ctx)} — say "hide it" when you’re done.`,ask:''},[{type:'hints',show:true}],[],{intent});
        case 'visual_hide': return reply(ctx,{instruction:'Done — back to watching the hands on the guitar.',ask:''},[{type:'hints',show:false}],[],{intent});
        case 'pref_short': return reply(ctx,{instruction:'Got it — one small step at a time from now on, and I’ll show before I tell.',ask:'Want me to play the current shape so you can watch it first?'},[{type:'prefs',explanation:'short',pace:'gentle'}],[prefsRemember('Prefers one short instruction at a time, followed by a demonstration and time to try.')],{intent,awaiting:(session.awaiting='demo_offer')});
        case 'pref_detail': return reply(ctx,{instruction:'Okay, I’ll add a little more of the why behind each step.',why:'You can switch back to one-step mode any time by saying "simpler".',ask:''},[{type:'prefs',explanation:lvl==='short'?'medium':'detailed'}],[prefsRemember('Happy with a bit more detail and the reasons behind an instruction.')],{intent});
        case 'plain_english': { const chord=currentChord(ctx); if(chord){ const steps=fingerSteps(music,chord); session.fingerWalk={chord,index:0,steps}; session.awaiting='finger_walk'; return reply(ctx,{instruction:`In plain terms, for ${music.CHORDS[chord].name}: ${steps[0]}`,ask:'Say "next" for the next finger.'},[],[prefsRemember('Avoid jargon; describe hand movements in plain English, one finger at a time.')],{intent}); } return reply(ctx,{instruction:'Plain and simple: put the finger just behind the metal fret, press with the fingertip, and pick that one string.',ask:'Shall I play it so you can hear what it should sound like?'},[],[prefsRemember('Avoid jargon; describe hand movements in plain English.')],{intent,awaiting:(session.awaiting='demo_offer')}); }
        case 'pref_works': return reply(ctx,{instruction:'Noted — I’ll keep teaching this way, and I’ll check now and then that it still works for you.',ask:''},[],[prefsRemember(`What worked (${new Date().toLocaleDateString('en-GB')}): ${session.lastSay?'"'+session.lastSay.slice(0,70)+'…"':'the last approach'}.`)],{intent});
        case 'pref_not_working': return reply(ctx,{instruction:'Thanks for telling me — I’ll change tack.',why:'I can demonstrate instead of explaining, go slower, or break it into smaller pieces.',ask:'Which would you like: show, slow, or smaller?'},[],[prefsRemember(`Did not work: ${session.lastSay?'"'+session.lastSay.slice(0,70)+'…"':'the last approach'}.`)],{intent});
        case 'song': return reply(ctx,{instruction:'I can’t play Sultans of Swing for you: I don’t have a licensed or user-supplied transcription, and I won’t make the notes up.',why:'A guessed lick would teach you the wrong thing, and the original recording isn’t mine to reproduce.',ask:'Import your own arrangement as a lesson JSON, or shall we build the skills it needs with the capo chord workout?'},[],[],{intent,honest:true});
        case 'barre': { session.phase='demo'; session.awaiting=null; const sel={from:0,to:0}; session.passage={lessonId:'barre',...sel}; return reply(ctx,{instruction:'Let’s skip the full barre for now and start with Fmaj7, which you already know — I’ll play it twice.',why:'We add one string at a time: Fmaj7, then the small three-string F, then a mini barre on two strings, and only then the full F, moving on when each one rings clean.',ask:'Watch my hand, then try it and tell me how it felt — and stop if anything hurts.'},[{type:'lesson',id:'barre'},{type:'select',from:0,to:0},{type:'play',from:0,to:0,loop:2,countIn:true},{type:'prompt_attempt'}],[{track:'playing',text:'Working through the F route: Fmaj7 → small F → mini barre → full F. Check thumb, wrist and string action before adding pressure.'}],{intent}); }
        case 'stop': session.awaiting=null; if(session.phase==='demo') session.phase='passage'; return reply(ctx,{instruction:'Stopped.',ask:'Tell me exactly which bit you want help with.'},[{type:'stop'}],[],{intent});
        case 'slower': { const bpm=c.slowMotion?Math.max(50,Math.round(ctx.tempo*0.6)):Math.max(50,ctx.tempo-12); return reply(ctx,{instruction:`${c.slowMotion?'Slow motion':'Slower'}: ${bpm} beats per minute. Here it is again.`,ask:''},[{type:'tempo',bpm},playSelection(ctx,{countIn:true})],[],{intent}); }
        case 'faster': { const bpm=Math.min(160,ctx.tempo+10); return reply(ctx,{instruction:`Up to ${bpm} beats per minute.`,ask:'Go at your own pace — say "slower" any time.'},[{type:'tempo',bpm},playSelection(ctx)],[],{intent}); }
        case 'again': return reply(ctx,{instruction:`Here’s ${describeSelection(ctx)} again — watch the fretting hand, listen for each string.`,ask:''},[playSelection(ctx,{countIn:true})],[],{intent});
        case 'last_notes': { const to=ctx.cursor; const from=Math.max(0,to-(c.count-1)); return reply(ctx,{instruction:`Just the last ${to-from+1}: ${from===to?stepLabel(ctx,to):stepLabel(ctx,from)+' to '+stepLabel(ctx,to)}, three times.`,ask:''},[{type:'select',from,to},{type:'play',from,to,loop:3,countIn:true}],[],{intent}); }
        case 'from_top': { const to=ctx.lesson.steps.length-1; return reply(ctx,{instruction:'From the top — the whole exercise.',ask:''},[{type:'select',from:0,to},{type:'play',from:0,to,loop:1,countIn:true}],[],{intent}); }
        case 'loop': { const sel=selection(ctx); const n=c.count===Infinity?Infinity:c.count; return reply(ctx,{instruction:`Looping ${describeSelection(ctx)} ${n===Infinity?'until you say stop':n+' times'}.`,ask:''},[{type:'play',from:sel.from,to:sel.to,loop:n,countIn:true}],[],{intent}); }
        case 'section': return pickSection(ctx,c);
        case 'next': return onNext(ctx);
        case 'earlier': case 'later': return shiftSelection(ctx,intent==='earlier'?-1:1);
        case 'got_it': return onGotIt(ctx);
        case 'tricky': return correct(ctx,null);
        case 'capo_fact': { if(c.capo===null) return reply(ctx,{instruction:'Which fret is the capo on?',ask:''},[],[],{intent}); return reply(ctx,{instruction:`Capo ${c.capo?'on fret '+c.capo:'off'} — I’ll show shapes and sounding chords for that.`,ask:''},[{type:'capo',fret:c.capo}],[{track:'playing',text:`Capo usually ${c.capo?'on fret '+c.capo:'off'} (said on ${new Date().toLocaleDateString('en-GB')}).`}],{intent}); }
        case 'change_question': { const sel=selection(ctx); return reply(ctx,{instruction:'Keep any finger that stays on the same string and fret planted, and move the others together on the last "and" of the bar.',why:'Anchor fingers give the hand a reference point, and moving early keeps the beat steady.',ask:'I’ll play the change slowly so you can see which finger stays.'},[{type:'tempo',bpm:Math.max(50,Math.round(ctx.tempo*.8))},{type:'play',from:sel.from,to:sel.to,loop:2,countIn:true}],[],{intent}); }
        case 'strum_question': { const pat=music.STRUM_PATTERNS[ctx.lesson.pattern]||music.STRUM_PATTERNS.basic; const words=pat.map(h=>h.d==='D'?'down':'up').join(', '); return reply(ctx,{instruction:`The pattern is ${words} — ${pat.map(h=>h.d).join(' ')} — and I’ll play it slowly now.`,why:'Downstrokes land on the beat; upstrokes fall on the "and" in between.',ask:'Watch the picking hand and count along.'},[{type:'tempo',bpm:Math.max(50,Math.round(ctx.tempo*.8))},playSelection(ctx,{countIn:true})],[],{intent}); }
        case 'chord_question': { const chord=c.chord; const lessonWith=ctx.lessons.find(l=>l.kind==='chord'&&l.steps.some(s=>s.chord===chord)); const steps=fingerSteps(music,chord); const actions=lessonWith?[{type:'lesson',id:lessonWith.id},{type:'select',from:lessonWith.steps.findIndex(s=>s.chord===chord),to:lessonWith.steps.findIndex(s=>s.chord===chord)}]:[{type:'lesson',lesson:{id:'chord-'+chord,name:'Chord · '+music.CHORDS[chord].name,title:music.CHORDS[chord].name+' shape',kind:'chord',pattern:'simple',description:'A single chord to study.',steps:[{chord,label:music.CHORDS[chord].name}]}},{type:'select',from:0,to:0}]; const idx=lessonWith?lessonWith.steps.findIndex(s=>s.chord===chord):0; actions.push({type:'play',from:idx,to:idx,loop:2,countIn:false}); session.fingerWalk={chord,index:0,steps}; session.awaiting='finger_walk'; session.phase='demo'; return reply(ctx,{instruction:`For ${music.CHORDS[chord].name}: ${steps[0]}`,why:`${ctx.capo?'With your capo on '+ctx.capo+' it sounds as '+music.chordSound(chord,ctx.capo)+'. ':''}Fingertips just behind the fret need far less pressure than fingers in the middle of the space.`,ask:'Watch it on my guitar, then say "next" for the next finger.'},actions,[],{intent}); }
        case 'capo_question': { const chord=c.chord||currentChord(ctx)||'Am'; return reply(ctx,{instruction:`With the capo on fret ${ctx.capo}, your ${music.CHORDS[chord]?.name||chord} shape sounds as ${music.chordSound(chord,ctx.capo)}; the shape you make doesn’t change, only the pitch.`,why:'Each capo fret raises every string by one semitone, so capo 5 lifts everything by five.',ask:''},[],[],{intent}); }
        case 'teach_meta': return reply(ctx,{instruction:'Tell me what isn’t landing and I’ll change it: shorter, slower, show first, or break it down further.',ask:'I remember what helps you.'},[],[],{intent});
        case 'fact': return reply(ctx,{instruction:'Noted — I’ll keep that in mind when we practise.',ask:''},[],[{track:'playing',text:text.trim().slice(0,160)}],{intent});
        case 'greeting': session.phase='goal'; return reply(ctx,{instruction:'Hi — shall we warm up with your chord changes, ease into that F shape, or try the little lick?',ask:''},[],[],{intent});
        default: session.phase='goal'; return reply(ctx,{instruction:'Let’s tackle one thing: which chord, note or change is hardest right now?',why:'I’ll demonstrate it first, then we isolate it and repeat.',ask:'You can also click a step card and press "Hear & watch".'},[],[],{intent:'unknown'});
      }
    }
    function describeShape(ctx){ const chord=currentChord(ctx); return chord?music.CHORDS[chord].name:stepLabel(ctx,ctx.cursor); }
    function onYes(ctx){
      const a=session.awaiting; session.awaiting=null;
      if(a==='demo_offer'||a==='confirm_section'){ if(a==='confirm_section'){ const sel=selection(ctx); return reply(ctx,{instruction:`Great — ${describeSelection(ctx)} it is, twice with a count-in, then it’s your turn.`,ask:''},[playSelection(ctx,{loop:2,countIn:true})],[]); } return reply(ctx,{instruction:`Watch the fretting hand — here’s ${describeSelection(ctx)}.`,ask:''},[playSelection(ctx,{countIn:true})],[]); }
      if(a==='hint_offer') return reply(ctx,{instruction:`Here’s the finger guide for ${describeShape(ctx)}; say "hide it" when you’re done.`,ask:''},[{type:'hints',show:true}],[{track:'teaching',text:'Accepted a finger diagram after repeated difficulty.'}]);
      if(a==='attempt_report') return onGotIt(ctx);
      if(a==='finger_walk') return onNext(ctx);
      if(a==='comfort') return reply(ctx,{instruction:'Gently then — Fmaj7, light pressure, and we stop the moment anything hurts.',ask:''},[{type:'lesson',id:'barre'},{type:'select',from:0,to:0},{type:'play',from:0,to:0,loop:2,countIn:true},{type:'prompt_attempt'}],[]);
      return reply(ctx,{instruction:'Okay.',ask:'What shall we do next: demonstrate, loop, or move on?'},[],[]);
    }
    function onNo(ctx){
      const a=session.awaiting; session.awaiting=null;
      if(a==='confirm_section') { session.awaiting='confirm_section'; return reply(ctx,{instruction:'No problem — say "earlier" or "later", or click the step cards to mark the part you mean.',ask:'I’ll play it back before we practise it.'},[],[]); }
      if(a==='hint_offer') return reply(ctx,{instruction:'Fair enough — we’ll keep learning from the hands.',ask:'Shall I slow it down and play it again?'},[],[{track:'teaching',text:'Declined a finger diagram; prefers to learn from the hand demonstration.'}],{awaiting:(session.awaiting='demo_offer')});
      if(a==='attempt_report') return correct(ctx,null);
      if(a==='comfort') return reply(ctx,{instruction:'Good call — rest today, and we’ll pick it up next time.',ask:''},[{type:'stop'}],[]);
      return reply(ctx,{instruction:'Okay — tell me what you’d like instead.',ask:''},[],[]);
    }
    function onNext(ctx){
      if(session.awaiting==='finger_walk'&&session.fingerWalk){ const w=session.fingerWalk; w.index++; if(w.index<w.steps.length){ const last=w.index===w.steps.length-1; if(last){ session.awaiting='demo_offer'; } return reply(ctx,{instruction:w.steps[w.index],ask:last?'Want to hear it and watch my hand do the whole shape?':'Say "next" when it’s in place.'},[],[]); } session.fingerWalk=null; session.awaiting='demo_offer'; return reply(ctx,{instruction:'That’s the whole shape.',ask:'Shall I play it so you can check the sound?'},[],[]); }
      const sel=selection(ctx); const len=sel.to-sel.from+1; const last=ctx.lesson.steps.length-1;
      if(sel.to>=last){ session.phase='advance'; return reply(ctx,{instruction:'That was the end of this exercise — nice work.',ask:'Run the whole thing from the top, or pick another lesson?'},[],[]); }
      const from=sel.to+1, to=Math.min(last,sel.to+len); session.phase='passage'; session.awaiting='confirm_section';
      return reply(ctx,{instruction:`Next is ${from===to?stepLabel(ctx,from):stepLabel(ctx,from)+' to '+stepLabel(ctx,to)} — here it is once.`,ask:'Is this the part you want?'},[{type:'select',from,to,confirm:true},{type:'play',from,to,loop:1,countIn:false}],[]);
    }
    function onGotIt(ctx){
      const sel=selection(ctx); const st=stats(ctx,sel.from,sel.to); st.ok++; session.tricky=0; session.phase='advance'; session.awaiting=null;
      const remember=[{track:'playing',text:`Played ${sectionLabel(ctx,sel.from,sel.to)} cleanly (${st.ok}×).`,key:'ok:'+sectionKey(ctx.lesson.id,sel.from,sel.to)}];
      if(ctx.tempo<ctx.baseTempo){ return reply(ctx,{instruction:'Lovely. Let’s nudge the tempo up a little and go again.',ask:''},[{type:'tempo',bpm:Math.min(ctx.baseTempo,ctx.tempo+8)},playSelection(ctx,{countIn:true}),{type:'prompt_attempt'}],remember); }
      const r=onNext(ctx); r.remember=remember.concat(r.remember||[]); r.say='Lovely — '+r.say.charAt(0).toLowerCase()+r.say.slice(1); return r;
    }
    function pickSection(ctx,c){
      const secs=music.sectionsFor(ctx.lesson); let from,to;
      if(c.half){ const last=ctx.lesson.steps.length-1; const mid=Math.floor(last/2); if(c.half==='first'){from=0;to=mid;} else {from=mid+1;to=last;} }
      else if(c.number&&secs[c.number-1]){ from=secs[c.number-1].from; to=secs[c.number-1].to; }
      else if(c.number&&ctx.lesson.steps[c.number-1]){ from=to=c.number-1; }
      else return reply(ctx,{instruction:`This lesson has ${secs.length?secs.length+' sections: '+secs.map((s,i)=>(i+1)+' – '+s.name).join('; '):ctx.lesson.steps.length+' steps'}.`,ask:'Which one?'},[],[]);
      session.phase='passage'; session.awaiting='confirm_section';
      return reply(ctx,{instruction:`${stepLabel(ctx,from)}${from!==to?' to '+stepLabel(ctx,to):''} — here it is once.`,ask:'Is this the part?'},[{type:'select',from,to,confirm:true},{type:'play',from,to,loop:1}],[]);
    }
    function shiftSelection(ctx,dir){
      const sel=selection(ctx); const len=sel.to-sel.from+1; const last=ctx.lesson.steps.length-1; let from=sel.from+dir*len, to=sel.to+dir*len;
      if(from<0){from=0;to=Math.min(last,len-1);} if(to>last){to=last;from=Math.max(0,last-len+1);}
      if(from===sel.from&&to===sel.to) return reply(ctx,{instruction:dir<0?'We’re already at the start of this exercise.':'That’s the end of this exercise.',ask:''},[],[]);
      session.phase='passage'; session.awaiting='confirm_section';
      return reply(ctx,{instruction:`${dir<0?'Earlier':'Later'} — ${stepLabel(ctx,from)}${from!==to?' to '+stepLabel(ctx,to):''}, played once.`,ask:'Is this the part?'},[{type:'select',from,to,confirm:true},{type:'play',from,to,loop:1}],[]);
    }
    /* Events from the app: demo_end, loop_done, attempt (mic observation), hint_auto */
    function event(name,payload={}){
      const ctx=getContext(); currentIntent='event:'+name;
      if(name==='demo_end'){ if(session.phase==='demo'&&payload.reason==='complete') return yourTurn(ctx); return null; }
      if(name==='loop_done'){ const sel=selection(ctx); const st=stats(ctx,sel.from,sel.to); st.loops++; session.loopsSince++; if(session.loopsSince>=3&&ctx.memory.teaching.autoHints&&!ctx.hintsVisible&&session.awaiting!=='hint_offer'){ session.loopsSince=0; session.awaiting='hint_offer'; return reply(ctx,{instruction:'We’ve been round this a few times — would a finger diagram help?',ask:'Say yes to show it, or no to keep watching the hands.'},[],[]); } return null; }
      if(name==='attempt'){ return observe(ctx,payload); }
      if(name==='section_selected'){ session.phase='passage'; session.awaiting=null; session.loopsSince=0; return null; }
      return null;
    }
    /* Honest observation: only monophonic notes are compared; chords are never graded from the mic. */
    function observe(ctx,p){
      const expected=p.expected||[], heard=p.heard||[]; session.phase='observe';
      if(ctx.lesson.kind!=='melody') return reply(ctx,{instruction:'I can’t judge a full chord from the microphone yet, so tell me: did every string ring?',ask:''},[{type:'prompt_attempt'}],[]);
      if(!heard.length) return reply(ctx,{instruction:'I didn’t catch any notes — play a little louder or closer to the microphone, one note at a time.',ask:''},[{type:'prompt_attempt'}],[]);
      const n=Math.min(expected.length,heard.length); let firstMiss=-1, octave=false;
      for(let i=0;i<n;i++){ const d=heard[i]-expected[i].midi; if(Math.abs(d)<=0.6) continue; if(Math.abs(Math.abs(d)-12)<=0.6){octave=true;continue;} firstMiss=i; break; }
      if(firstMiss<0&&heard.length>=expected.length){ session.tricky=0; return onGotIt(Object.assign({},ctx,{})); }
      if(firstMiss<0) return reply(ctx,{instruction:`The first ${heard.length} note${heard.length>1?'s':''} matched${octave?' (one sounded an octave off, which can be the mic)':''} — I only heard ${heard.length} of ${expected.length}.`,ask:'Play the whole phrase once more.'},[{type:'prompt_attempt'},{type:'listen',on:true}],[]);
      const e=expected[firstMiss]; const d=Math.round(heard[firstMiss]-e.midi); const dir=d<0?`${Math.abs(d)} fret${Math.abs(d)>1?'s':''} low`:`${d} fret${d>1?'s':''} high`;
      return correct(ctx,{instruction:`Note ${firstMiss+1} should be ${e.label} — I heard it ${dir}.`,why:'Check the fingertip is just behind the fret and on the right string before you pick.',ask:'Try just that note, then the phrase.',actions:[{type:'select',from:e.step,to:e.step},{type:'play',from:e.step,to:e.step,loop:2},{type:'prompt_attempt'}]});
    }
    return {handle,event,session,classify:(t)=>classify(t,{session,music}),fingerSteps:(id)=>fingerSteps(music,id),compose};
  }

  /* Validate an action list coming from any coach (rules or AI). Unknown types or bad values are dropped. */
  function sanitiseActions(actions,ctx){
    if(!Array.isArray(actions)) return [];
    const max=ctx&&ctx.lesson?ctx.lesson.steps.length-1:1000; const out=[];
    for(const a of actions.slice(0,8)){
      if(!a||typeof a!=='object'||!ACTION_TYPES.includes(a.type)) continue;
      const b={type:a.type};
      if('from' in a){ if(!Number.isInteger(a.from)||a.from<0||a.from>max) continue; b.from=a.from; }
      if('to' in a){ if(!Number.isInteger(a.to)||a.to<(b.from||0)||a.to>max) continue; b.to=a.to; }
      if('loop' in a){ b.loop=a.loop===Infinity||a.loop==='infinite'?Infinity:(Number.isInteger(a.loop)&&a.loop>=1&&a.loop<=16?a.loop:1); }
      if('countIn' in a) b.countIn=!!a.countIn; if('confirm' in a) b.confirm=!!a.confirm;
      if(a.type==='tempo'){ if(!Number.isFinite(a.bpm)||a.bpm<40||a.bpm>200) continue; b.bpm=Math.round(a.bpm); }
      if(a.type==='slow'||a.type==='listen') b.on=!!a.on; if(a.type==='hints') b.show=!!a.show;
      if(a.type==='capo'){ if(![0,1,2,3,4,5,6,7].includes(a.fret)) continue; b.fret=a.fret; }
      if(a.type==='lesson'){ if(typeof a.id==='string'&&a.id.length<60) b.id=a.id; else if(a.lesson&&typeof a.lesson==='object') b.lesson=a.lesson; else continue; }
      if(a.type==='page'){ if(!['studio','teacher','guitar','progress','builder'].includes(a.id)) continue; b.id=a.id; }
      if(a.type==='owner_request'){ b.text=String(a.text||'').slice(0,600); }
      if(a.type==='prefs'){ for(const k of ['explanation','pace','autoHints','speak']) if(k in a) b[k]=a[k]; if(b.explanation&&!['short','medium','detailed'].includes(b.explanation)) delete b.explanation; if(b.pace&&!['gentle','normal'].includes(b.pace)) delete b.pace; }
      out.push(b);
    }
    return out;
  }
  function sanitiseRemember(items){ if(!Array.isArray(items)) return []; return items.filter(r=>r&&['playing','teaching'].includes(r.track)&&typeof r.text==='string'&&r.text.trim()).slice(0,4).map(r=>({track:r.track,text:r.text.trim().slice(0,220),key:typeof r.key==='string'?r.key.slice(0,80):undefined})); }

  return {MEMORY_VERSION,ACTION_TYPES,defaultMemory,migrate,create,compose,sentences,sanitiseActions,sanitiseRemember,classifyRules:RX};
})();
