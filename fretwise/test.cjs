const assert=require('node:assert/strict');
const fs=require('node:fs');
const vm=require('node:vm');
const context={window:{},console,Math,Float32Array,Date,setInterval,clearInterval};
vm.createContext(context);
for(const f of ['music.js','audio.js'])vm.runInContext(fs.readFileSync(__dirname+'/'+f,'utf8'),context,{filename:f});
const M=context.window.FRETWISE_MUSIC,A=context.window.FRETWISE_AUDIO;
let passed=0;function test(name,fn){try{fn();passed++;console.log('PASS '+name);}catch(e){console.error('FAIL '+name+': '+e.message);process.exitCode=1;}}
function midiName(n){return M.midiToName(n)}
test('known chord sound for capo 5',()=>{assert.equal(M.chordSound('Am',5),'Dm');assert.equal(M.chordSound('G',5),'C');assert.equal(M.chordSound('C',5),'F');assert.equal(M.chordSound('Fmaj7',5),'Bbmaj7');});
test('capo moves every sounding note by 5 semitones',()=>{const a=M.stepMidis({chord:'Am'},0),b=M.stepMidis({chord:'Am'},5);assert.equal(a.length,b.length);a.forEach((n,i)=>assert.equal(b[i]-n,5));});
test('A minor open chord pitch set is A E A C E',()=>{assert.deepEqual([...M.stepMidis({chord:'Am'},0)].map(n=>midiName(n)),['A2','E3','A3','C4','E4']);});
test('F shape progression is defined and musically consistent',()=>{for(const key of ['Fmaj7','Fsmall','Fmini','F'])assert(M.CHORDS[key]);assert.equal(M.stepMidis({chord:'Fsmall'},0).length,3);assert.equal(M.stepMidis({chord:'Fmini'},0).length,4);});
test('step frets and solo notes use high e string index 5',()=>{assert.deepEqual([...M.stepFrets({string:5,fret:3})],[-1,-1,-1,-1,-1,3]);assert.equal(M.midiToName(M.stepMidis({string:5,fret:3},0)[0]),'G4');});
test('pitch detector estimates clean 440Hz single note',()=>{const r=44100,n=4096,x=new Float32Array(n);for(let i=0;i<n;i++)x[i]=.24*Math.sin(2*Math.PI*440*i/r);const hit=A.detectPitch(x,r);assert(hit,'pitch estimator returned no pitch');const rounded=Math.round(M.freqToMidi(hit.frequency));assert.equal(rounded,69,'expected A4, got '+hit.frequency);});
test('pitch detector returns no note on silence',()=>{assert.equal(A.detectPitch(new Float32Array(4096),44100),null);});
test('import rejects invalid strings, oversized lessons and unknown chords',()=>{assert.throws(()=>M.validateImportedExercise({kind:'melody',steps:[{string:8,fret:2}]}));assert.throws(()=>M.validateImportedExercise({kind:'chord',steps:[{chord:'Nonsense'}]}));assert.throws(()=>M.validateImportedExercise({kind:'chord',steps:Array.from({length:129},()=>({chord:'Am'}))}));});
test('original user-authorised exercise import succeeds',()=>{const json=JSON.parse(fs.readFileSync(__dirname+'/sample_original_lesson.json','utf8'));const exercise=M.validateImportedExercise(json);assert.equal(exercise.kind,'melody');assert.equal(exercise.steps.length,4);});
test('all built-in lessons are structured and original',()=>{assert.equal(M.LESSONS.length,4);for(const l of M.LESSONS)assert(l.steps.length>=4);});
console.log(`\n${passed}/10 checks passed`);
