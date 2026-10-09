const {test} = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');

function editor(options = {}) {
  const calls = [];
  const messages = [];
  const elements = new Map();
  const element = id => {
    if (!elements.has(id)) elements.set(id,{
      innerHTML:'',textContent:'',value:'',dataset:{},elements:[],classList:{toggle() {}},handlers:{},
      addEventListener(name,handler) { this.handlers[name] = handler; },setAttribute() {},showModal() { this.open = true; },close() { this.open = false; },
    });
    return elements.get(id);
  };
  const fields = ['title','goal','opening','acceptance'].map(name => ({name,checkValidity:() => true}));
  fields.namedItem = name => fields.find(field => field.name === name);
  element('scenario-form').elements = fields;
  element('agent-policy').value = 'fix';
  const context = vm.createContext({
    window:{scenarioBridge:{createTransport:() => ({
      subscribe() {},reportSize() {},initialize:() => Promise.resolve(null),canMessage:() => options.host !== false,
      messageUnavailableReason:() => 'Open this MCP App inside Codex for a direct handoff, or copy the prompt into your chat.',
      call:async (action,body) => { calls.push({action,body:JSON.parse(JSON.stringify(body))}); return options.call?.(action,body); },
      sendMessage:async text => { messages.push(text); if (options.rejectMessage) throw new Error('Host rejected handoff'); },
    })}},
    document:{getElementById:element,querySelector:element,querySelectorAll:() => [],body:{},documentElement:{getBoundingClientRect:() => ({height:1})}},
    ResizeObserver:class { observe() {} },setTimeout:() => 1,clearTimeout() {},navigator:{clipboard:{writeText:async () => {}}},
  });
  vm.runInContext(fs.readFileSync(`${__dirname}/app.js`,'utf8'),context);
  const scenario = {goal:'A supported estimate.',opening:'When will it arrive?',acceptance:'Accept uncertainty.',start:'tool-boundary',status:'In transit',estimate:''};
  const source = {id:'source',title:'Source',sourceKind:'recorded',scenario};
  vm.runInContext(`loadLaunch(${JSON.stringify({cases:[source],ideas:[],policies:{control:{name:'Control',prompt:'Reassure the customer'},fix:{name:'Fixed',prompt:'Use tool evidence'}}})})`,context);
  return {elements,context,calls,messages,click:id => element(id).handlers.click({target:element(id)}),evaluate:code => vm.runInContext(code,context),scenario};
}

test('recording failure cannot become a passing test-set row',() => {
  const app = editor();
  const complete = {status:'completed',checks:{no_unsupported_date:true,uses_supported_date:true}};
  assert.equal(app.evaluate(`runState(${JSON.stringify(complete)})`),'Pass');
  assert.equal(app.evaluate(`runState(${JSON.stringify({...complete,receiptStatus:'partial',receiptPassed:false})})`),'Incomplete');
  assert.equal(app.evaluate(`runState(${JSON.stringify({...complete,receiptStatus:'check-failed',receiptPassed:false})})`),'Fail');
  assert.equal(app.evaluate(`runState(${JSON.stringify({...complete,status:'turn-limit'})})`),'Incomplete');

});

test('source previews retain origin while edited historical messages show derived context',() => {
  const app = editor();
  const run = {scenario:app.scenario,messages:[{role:'customer',text:'Edited opening.',historical:true}]};
  assert.match(app.evaluate(`renderConversation(${JSON.stringify(run)},true)`),/Recorded source conversation/);
  const historical = app.evaluate(`renderConversation(${JSON.stringify(run)})`);
  assert.match(historical,/Starting context derived from source/);
  assert.doesNotMatch(historical,/Recorded source conversation/);
  app.evaluate("cases[0].sourceKind='imported'");
  assert.match(app.evaluate(`renderConversation(${JSON.stringify(run)},true)`),/Imported source conversation/);
});

test('recording errors remain visible without inventing a recorded session',() => {
  const app = editor();
  const run = {runId:'run',sessionId:null,status:'completed',variant:'fix',agentTurns:1,scenario:app.scenario,messages:[],checks:{no_unsupported_date:true,uses_supported_date:true},receiptStatus:'partial',receiptPassed:false,receiptError:'Result recording failed.',_title:'Source'};
  app.evaluate(`draft().runs=[${JSON.stringify(run)}]; renderPreview();`);
  const rendered = app.elements.get('sample-outcome').innerHTML;
  assert.match(rendered,/Simulation complete · recording incomplete/);
  assert.match(rendered,/Result recording failed\./);
  assert.doesNotMatch(rendered,/Recorded session:/);
  assert.equal(app.elements.get('save-button').disabled,true);
});

test('automatic revision names display plainly without changing saved identifiers or custom names',() => {
  const app = editor();
  app.evaluate("testSet={name:'delivery-regression-tests-a1b2c3d4',cohortId:'set-id',cohortVersionId:'version-id',cases:[]}; showingSet=true; applyLayout();");
  assert.equal(app.elements.get('case-title').textContent,'Regression test set');
  assert.match(app.elements.get('test-set').innerHTML,/Regression test set/);
  assert.doesNotMatch(app.elements.get('test-set').innerHTML,/delivery-regression-tests-a1b2c3d4/);
  assert.equal(app.evaluate('testSet.name'),'delivery-regression-tests-a1b2c3d4');
  assert.equal(app.evaluate('testSet.cohortId'),'set-id');
  assert.equal(app.evaluate('testSet.cohortVersionId'),'version-id');
  app.evaluate("testSet.name='Birthday regression tests'; applyLayout();");
  assert.equal(app.elements.get('case-title').textContent,'Birthday regression tests');
  assert.match(app.elements.get('test-set').innerHTML,/Birthday regression tests/);
});

const receipt = (status = 'completed',passed = true) => ({
  experiment_id:'experiment-id',cohort_version_id:'pinned-version',status,
  ready_for_handoff:status === 'completed' && passed,
  experiment_url:'https://kitaru.example/experiments/experiment-id',
  evaluator_name:'Delivery date',evaluator_version:'v1',
  baseline:{status:'completed',passed:false,cases:[{source_session_id:'case-id',result_session_id:'baseline-session',status:'completed',passed:false}]},
  candidate:{status,passed,cases:[{source_session_id:'case-id',result_session_id:'candidate-session',status,passed}]},
});
function savedSet(app) {
  app.evaluate("testSet={name:'Saved set',cohortVersionId:'pinned-version',cases:[{sessionId:'case-id',title:'Deadline pressure'}]}; showingSet=true;");
}

const policyOptions = {proposals:[
  {policy:{name:'Evidence first',prompt:'Only use confirmed evidence'},rationale:'Avoid unsupported promises'},
  {policy:{name:'Brief answers',prompt:'Give short evidence-based answers'},rationale:'Reduce repetitive explanations'},
  {policy:{name:'Clear next step',prompt:'Explain one available next step'},rationale:'Help the customer act on missing information'},
]};

test('visible policy suggestions generate options without a request and require selection',async () => {
  const app = editor({call:() => policyOptions});
  await app.click('suggest-policy');
  assert.equal(app.calls[0].action,'policy');
  assert.equal(app.calls[0].body.instruction,'');
  assert.deepEqual(app.calls[0].body.scenario,app.scenario);
  assert.equal(app.evaluate('candidatePolicy'),null);
  assert.equal(app.elements.get('policy-dialog').open,true);
  assert.match(app.elements.get('policy-options').innerHTML,/<table/);
  for (const item of policyOptions.proposals) assert.match(app.elements.get('policy-options').innerHTML,new RegExp(item.policy.name));
  assert.equal(app.elements.get('accept-policy').disabled,true);
  await app.click('accept-policy');
  assert.equal(app.evaluate('candidatePolicy'),null);
  app.evaluate('selectPolicy(1)');
  assert.match(app.elements.get('policy-proposal').innerHTML,/Inspect selected instructions/);
  assert.equal(app.elements.get('accept-policy').disabled,false);
  await app.click('accept-policy');
  assert.equal(app.evaluate('chosenPolicy().prompt'),'Give short evidence-based answers');
  assert.equal(app.evaluate('setPolicy'),'candidate');
});

test('a user direction regenerates reviewed options without silently applying them',async () => {
  const app = editor({call:() => policyOptions});
  await app.click('suggest-policy');
  app.evaluate('selectPolicy(0)');
  app.evaluate("$('policy-instruction').value='Avoid unsupported promises'");
  await app.click('generate-policies');
  assert.equal(app.calls[1].body.instruction,'Avoid unsupported promises');
  assert.equal(app.evaluate('candidatePolicy'),null);
  assert.equal(app.elements.get('accept-policy').disabled,true);
});

test('changed policy or scenario cannot accept stale policy options',async () => {
  for (const mutation of ["$('agent-policy').value='control'", "draft().scenario.opening='A different question'"]) {
    const app = editor({call:() => policyOptions});
    await app.click('suggest-policy');
    app.evaluate('selectPolicy(0)');
    app.evaluate(mutation);
    await app.click('accept-policy');
    assert.equal(app.evaluate('candidatePolicy'),null);
  }
});

test('policy generation errors keep the current policy and allow retry',async () => {
  const app = editor({call:() => { throw new Error('Generation unavailable'); }});
  await app.click('suggest-policy');
  assert.equal(app.evaluate('chosenPolicy().prompt'),'Use tool evidence');
  assert.equal(app.elements.get('accept-policy').disabled,true);
  assert.equal(app.elements.get('suggest-policy').disabled,false);
  assert.match(app.elements.get('policy-status').textContent,/Generation unavailable/);
});

test('native comparison uses the pinned cohort and identical evaluator selection across policies',async () => {
  const app = editor({call:action => ({receipt:action === 'experiment'?receipt('running',null):receipt(),experiment_url:'https://kitaru.example/experiments/experiment-id'})});
  savedSet(app);
  await app.evaluate('startExperiment()');
  assert.equal(app.calls[0].action,'experiment');
  assert.equal(app.calls[0].body.cohort_version_id,'pinned-version');
  assert.equal(app.calls[0].body.baseline.prompt,'Reassure the customer');
  assert.equal(app.calls[0].body.candidate.prompt,'Use tool evidence');
  assert.equal(app.calls[0].body.model,'gpt-6-luna');
  assert.match(app.elements.get('test-set').innerHTML,/id="prepare-handoff"[^>]*disabled/);
  assert.equal(app.messages.length,0);
  await app.evaluate('refreshExperiment()');
  assert.equal(app.calls[1].action,'experiment_status');
  assert.equal(app.calls[1].body.receipt.experiment_id,'experiment-id');
  assert.match(app.elements.get('test-set').innerHTML,/Experiment complete/);
  assert.match(app.elements.get('test-set').innerHTML,/candidate-session/);
  assert.doesNotMatch(app.elements.get('test-set').innerHTML,/id="prepare-handoff"[^>]*disabled/);
});

test('handoff requires server-validated passing evidence and sends only on user click',async () => {
  const prompt = 'Use /absolute/path/kitaru-regression-pr/SKILL.md with experiment-id and pinned-version.';
  const app = editor({call:() => ({receipt:receipt(),prompt})});
  savedSet(app);
  app.evaluate(`experiment=${JSON.stringify(receipt())};`);
  await app.evaluate('prepareHandoff()');
  assert.equal(app.calls[0].action,'handoff');
  assert.equal(app.elements.get('handoff-prompt').value,prompt);
  assert.equal(app.elements.get('handoff-details').open,false);
  assert.equal(app.elements.get('send-handoff').hidden,false);
  assert.equal(app.elements.get('browser-handoff').hidden,true);
  assert.equal(app.messages.length,0);
  await app.click('send-handoff');
  assert.deepEqual(app.messages,[prompt]);
  await app.click('send-handoff');
  assert.equal(app.messages.length,1);
});

test('failed and running experiments cannot prepare a PR; forged or stale receipts are rejected',async () => {
  const app = editor({call:() => ({receipt:receipt(),prompt:'unvalidated'})});
  savedSet(app);
  for (const state of [receipt('running',null),receipt('failed',false),{...receipt(),cohort_version_id:'earlier-version'}]) {
    app.evaluate(`experiment=${JSON.stringify(state)};`);
    await app.evaluate('prepareHandoff()');
  }
  assert.equal(app.calls.length,0);
  const invalid = editor({call:() => ({receipt:{...receipt(),experiment_id:'other-id'},prompt:'unvalidated'})});
  savedSet(invalid);
  invalid.evaluate(`experiment=${JSON.stringify(receipt())};`);
  await invalid.evaluate('prepareHandoff()');
  assert.equal(invalid.evaluate('handoff'),null);
  assert.match(invalid.elements.get('toast').textContent,/No validated/);
});

test('browser shows the identical copyable handoff and host rejection allows recovery',async () => {
  const result = {receipt:receipt(),prompt:'Exact pinned handoff'};
  const browser = editor({host:false,call:() => result});
  savedSet(browser);
  browser.evaluate(`experiment=${JSON.stringify(receipt())};`);
  await browser.evaluate('prepareHandoff()');
  assert.equal(browser.elements.get('send-handoff').hidden,true);
  assert.equal(browser.elements.get('browser-handoff').hidden,false);
  assert.equal(browser.elements.get('handoff-details').open,false);
  assert.match(browser.elements.get('handoff-status').textContent,/inside Codex/);
  assert.equal(browser.elements.get('handoff-prompt').value,result.prompt);
  const host = editor({call:() => result,rejectMessage:true});
  savedSet(host);
  host.evaluate(`experiment=${JSON.stringify(receipt())};`);
  await host.evaluate('prepareHandoff()');
  await host.click('send-handoff');
  assert.equal(host.elements.get('send-handoff').disabled,false);
  assert.match(host.elements.get('handoff-status').textContent,/Host rejected/);
});

test('experiment startup errors persist next to the comparison controls',async () => {
  const app = editor({call:() => { throw new Error('Save the test set with the current runner before comparing policies'); }});
  savedSet(app);
  await app.evaluate('startExperiment()');
  assert.match(app.elements.get('test-set').innerHTML,/Save the test set with the current runner/);
  assert.match(app.elements.get('test-set').innerHTML,/role="status"/);
  assert.equal(app.evaluate('experiment'),null);
});


test('pending evaluator results never display as failed and polling preserves expanded receipt details',() => {
  const app = editor();
  savedSet(app);
  app.evaluate(`experiment=${JSON.stringify(receipt('running',false))}; experiment.baseline.cases[0].status='running'; renderSet();`);
  assert.match(app.elements.get('test-set').innerHTML,/Deadline pressure · Pending/);
  assert.doesNotMatch(app.elements.get('test-set').innerHTML,/Deadline pressure · Fail/);
  app.elements.get('test-set').handlers.toggle({target:{dataset:{experimentDetails:''},open:true}});
  app.evaluate('renderSet()');
  assert.match(app.elements.get('test-set').innerHTML,/<details data-experiment-details open>/);
});

test('opening a source starts with its recorded trace before editing a scenario',async () => {
  const app = editor();
  assert.equal(app.evaluate('step'),'source');
  assert.equal(app.elements.get('original-trace').hidden,false);
  assert.equal(app.elements.get('back-step').hidden,true);
  assert.equal(app.elements.get('next-step').textContent,'Create a test scenario →');
  assert.match(app.elements.get('original-trace-messages').innerHTML,/No recorded transcript/);
  app.evaluate(`cases[0].preview={messages:[{role:'customer',text:'Can you guarantee Friday?'},{role:'tool',text:'{"estimated_delivery":null}'},{role:'agent',text:'It will definitely arrive Friday.'}]}; renderCase();`);
  assert.match(app.elements.get('original-trace-messages').innerHTML,/It will definitely arrive Friday/);
  assert.match(app.elements.get('original-trace-messages').innerHTML,/estimated_delivery/);
  assert.doesNotMatch(app.elements.get('original-trace-messages').innerHTML,/AI-generated/);
  await app.click('next-step');
  assert.equal(app.evaluate('step'),'brief');
  app.evaluate("draft().scenario.opening='A new simulated opening'; renderCase();");
  assert.match(app.elements.get('original-trace-messages').innerHTML,/Can you guarantee Friday/);
  assert.doesNotMatch(app.elements.get('original-trace-messages').innerHTML,/A new simulated opening/);
  app.evaluate("step='review'; applyLayout();");
  await app.click('next-step');
  assert.equal(app.evaluate('step'),'preview');
});

test('a new host launch resets old test-set context and starts from the original trace',() => {
  const app = editor();
  savedSet(app);
  app.evaluate("step='preview'; direction='tool-evidence'; experimentError='Old failure';");
  app.evaluate(`loadLaunch(${JSON.stringify({cases:[{id:'new-source',title:'New trace',sourceKind:'imported',scenario:app.scenario}],ideas:[],policies:{fix:{name:'Fixed',prompt:'Use evidence'},control:{name:'Control',prompt:'Reassure'}}})});`);
  assert.equal(app.evaluate('step'),'source');
  assert.equal(app.evaluate('showingSet'),false);
  assert.equal(app.evaluate('testSet'),null);
  assert.equal(app.evaluate('direction'),'baseline');
  assert.equal(app.elements.get('original-trace-title').textContent,'New trace');
});

test('policy generation disables running even when the suggestion dialog is closed',async () => {
  let finish;
  const app = editor({call:() => new Promise(resolve => { finish = resolve; })});
  const request = app.click('suggest-policy');
  assert.equal(app.elements.get('run-scenario').disabled,true);
  await app.click('close-policy-dialog');
  assert.equal(app.elements.get('run-scenario').disabled,true);
  finish(policyOptions);
  await request;
  assert.equal(app.elements.get('run-scenario').disabled,false);
});
