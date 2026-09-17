const {test} = require('node:test');
const assert = require('node:assert/strict');
const {gate} = require('./gate.cjs');

function fixture(eventName = 'release') {
  const context = {eventName:'release',sha:'abc',runId:100,repo:{owner:'owner',repo:'repo'},payload:{repository:{id:1,full_name:'owner/repo',default_branch:'main'}}};
  const state = {runs:[],jobs:['lint / lint','tests / tests','build / runtime'].map(name=>({name,status:'completed',conclusion:'success'})),artifacts:[{name:'runtime-binary',expired:false}],logs:[],calls:[],slept:0};
  const run = {id:99,head_sha:'abc',head_branch:'main',head_repository:{id:1},event:'push',status:'completed',conclusion:'success'};
  context.eventName = eventName;
  const github = {rest:{actions:{listWorkflowRuns:'runs',listJobsForWorkflowRun:'jobs',listWorkflowRunArtifacts:'artifacts'}},paginate:async (method,params)=>{state.calls.push({method,params});return state[method];}};
  const core = {info:msg=>state.logs.push(msg)};
  const options = {now:()=>state.slept,sleep:async ms=>{state.slept+=ms;run.status='completed';},timeout:30000};
  return {context,state,run,github,core,options};
}
test('reuse only exact successful source with lint, tests, runtime and the retained artifact',async()=>{
 const f=fixture();f.state.runs=[f.run];
 assert.deepEqual(await gate(f.github,f.context,f.core,f.options),{checks:false,run_id:99,reused:true});
});
test('missing or untrusted runs require full checks',async()=>{
 for(const change of [{head_sha:'other'},{head_branch:'feature'},{event:'pull_request'},{head_repository:{id:2}},{id:100}]){
  const f=fixture();f.state.runs=[{...f.run,...change}];
  assert.deepEqual(await gate(f.github,f.context,f.core,f.options),{checks:true,run_id:100,reused:false});
 }
});
test('failed run or a skipped required job never authorizes publication',async()=>{
 const f=fixture();f.state.runs=[{...f.run,conclusion:'failure'}];
 await assert.rejects(gate(f.github,f.context,f.core,f.options),/failed/);
 f.state.runs=[f.run];f.state.jobs[2].conclusion='skipped';
 await assert.rejects(gate(f.github,f.context,f.core,f.options),/runtime/);
});
test('old CI without retained binary falls back to full verification',async()=>{
 const f=fixture();f.state.runs=[f.run];f.state.artifacts[0].expired=true;
 assert.equal((await gate(f.github,f.context,f.core,f.options)).checks,true);
});
test('wait for pending matching CI without launching another suite',async()=>{
 const f=fixture();f.run.status='in_progress';f.state.runs=[f.run];
 assert.equal((await gate(f.github,f.context,f.core,f.options)).reused,true);
 assert.equal(f.state.slept,15000);
});
test('timeout and API failure fail closed',async()=>{
 const f=fixture();f.run.status='in_progress';f.state.runs=[f.run];f.options.sleep=async ms=>{f.state.slept+=ms;};
 await assert.rejects(gate(f.github,f.context,f.core,f.options),/Timed out/);
 f.github.paginate=async()=>{throw new Error('API down');};
 await assert.rejects(gate(f.github,f.context,f.core),/API down/);
});
test('ordinary CI never looks up another run',async()=>{
 for (const event of ['push','pull_request','workflow_dispatch']) {
  const f=fixture(event);f.state.runs=[f.run];
  assert.deepEqual(await gate(f.github,f.context,f.core,f.options),{checks:true,run_id:100,reused:false});
  assert.equal(f.state.calls.length,0);
 }
});
test('every required phase must finish successfully',async()=>{
 for (const name of ['lint / lint','tests / tests','build / runtime']) {
  for (const conclusion of ['skipped','failure','cancelled',null]) {
   const f=fixture();f.state.runs=[f.run];f.state.jobs.find(job=>job.name===name).conclusion=conclusion;
   await assert.rejects(gate(f.github,f.context,f.core,f.options),/successful/);
  }
  const f=fixture();f.state.runs=[f.run];f.state.jobs=f.state.jobs.filter(job=>job.name!==name);
  await assert.rejects(gate(f.github,f.context,f.core,f.options),/successful/);
 }
});

test('ordinary push may skip release-only checks; a failed full dispatch still blocks reuse',async()=>{
 const f=fixture();f.state.runs=[f.run];
 f.state.jobs.push({name:'container-tests',status:'completed',conclusion:'skipped'});
 assert.equal((await gate(f.github,f.context,f.core,f.options)).reused,true);
 f.run.event='workflow_dispatch';f.run.conclusion='failure';
 await assert.rejects(gate(f.github,f.context,f.core,f.options),/failed/);
});
