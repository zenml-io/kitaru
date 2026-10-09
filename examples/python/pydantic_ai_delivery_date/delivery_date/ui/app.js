const transport = window.scenarioBridge.createTransport(window);
const copy = value => JSON.parse(JSON.stringify(value));
const scenarioKey = value => JSON.stringify(Object.keys(value || {}).sort().map(key => [key,value[key]]));
const $ = id => document.getElementById(id);
const escape = value => String(value ?? '').replace(/[&<>"']/g, x => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[x]));
const steps = ['source','brief','evidence','variations','review','preview'];
const stepNames = {source:'original trace',brief:'customer situation',evidence:'agent evidence',variations:'test coverage',review:'review',preview:'conversation'};
const labels = {title:'Name',goal:'Customer goal',knownFacts:'Customer knowledge',opening:'Opening message',tone:'Tone',persistence:'Persistence',status:'Shipping status',estimate:'Delivery estimate',tracking:'Tracking link',acceptance:'Acceptance condition',maxTurns:'Agent-turn limit',start:'Starting point'};
const values = {calm:'Calm',frustrated:'Frustrated',skeptical:'Skeptical','accepts-answer':'Accepts an adequate answer','asks-once':'Asks one clarification','keeps-pressing':'Presses for certainty',full:'New conversation','n-minus-one':'Before a customer follow-up','tool-boundary':'After a tool result'};
const display = value => values[value] || (value === '' ? 'Unknown / not supplied' : String(value));
let cases = [];
let ideas = [];
const working = new Map();
let selectedId;
let step = 'source';
let direction = 'baseline';
let showingSet = false;
let testSet = null;
let setPolicy = 'fix';
let policies = {};
let candidatePolicy = null;
let policyProposal = null;
let policySuggestions = null;
let selectedPolicyIndex = null;
let policyGenerating = false;
let experiment = null;
let experimentUrl = null;
let experimentError = null;
let experimentDetailsOpen = false;
let experimentPolling = false;
let experimentTimer;
let handoff = null;
let handoffBusy = false;
let handoffSent = false;
let baselinePolicy = 'control';
let pendingProposal;
let generating = false;
let running = false;
let saving = false;
let loadingSet = false;
let comparing = false;
let toastTimer;
let reportedHeight = 0;

function selected() { return cases.find(c => c.id === selectedId); }
function source() { return cases.find(c => c.id === selected()?.sourceId) || selected(); }
function sourceId() { return selected().sourceId || selected().id; }
function draft() {
  if (!working.has(selectedId)) {
    const current = selected();
    working.set(selectedId,{title:current.title,scenario:copy(current.scenario),generation:current.generation,runs:copy(current.runs || []).map(run => ({...run,_title:run.title || current.title}))});
  }
  return working.get(selectedId);
}
function matchingRuns() {
  return (draft().runs || []).filter(run => scenarioKey(run.scenario) === scenarioKey(draft().scenario) && run._title === draft().title);
}
function latestRun() {
  const matching = matchingRuns();
  return matching.find(run => run.runId === draft().viewRunId) || matching.at(-1);
}
function changes() {
  const base = {title:source().title,...source().scenario};
  const next = {title:draft().title,...draft().scenario};
  return Object.keys(base).filter(key => base[key] !== next[key]).map(key => ({key,before:base[key],after:next[key]}));
}
function dirty() { return draft().title !== selected().title || scenarioKey(draft().scenario) !== scenarioKey(selected().scenario); }
function toast(message) {
  clearTimeout(toastTimer);
  $('toast').textContent = message;
  $('toast').hidden = false;
  toastTimer = setTimeout(() => $('toast').hidden = true,5000);
}
function reportSize() {
  const height = Math.ceil(document.documentElement.getBoundingClientRect().height);
  if (height !== reportedHeight) { reportedHeight = height; transport.reportSize(height); }
}
function normalizeSavedCase(item) {
  return {...item,id:item.sessionId || item.id,isDraft:true,sourceLabel:item.sourceLabel || 'Saved scenario',summary:item.summary || ''};
}
function updateTestSet(value) {
  if (!value?.cohortVersionId || !Array.isArray(value.cases)) throw new Error('No saved test-set version was returned.');
  testSet = value;
  const sourceCases = cases.filter(c => !c.isDraft);
  cases = [...sourceCases,...value.cases.map(normalizeSavedCase).filter(c => !sourceCases.some(source => source.id === c.id))];
  if (!cases.some(c => c.id === selectedId)) selectedId = cases[0]?.id;
}
function loadLaunch(data) {
  if (!Array.isArray(data?.cases)) throw new Error('No source conversations were returned.');
  cases = copy(data.cases).map(c => ({...c,id:c.id || c.sessionId}));
  ideas = data.ideas || [];
  policies = copy(data.policies || {});
  step = 'source';
  showingSet = false;
  testSet = null;
  direction = 'baseline';
  experimentError = null;
  experimentDetailsOpen = false;
  candidatePolicy = null;
  policyProposal = null;
  policySuggestions = null;
  selectedPolicyIndex = null;
  experiment = null;
  experimentUrl = null;
  setPolicy = 'fix';
  baselinePolicy = 'control';
  handoff = null;
  handoffSent = false;
  clearTimeout(experimentTimer);
  renderPolicy();
  working.clear();
  selectedId = cases[0]?.id;
  if (data.testSet) updateTestSet(data.testSet);
  if (!selectedId) throw new Error('Select at least one recorded delivery conversation to open the editor.');
  $('app-status').hidden = true;
  $('app').hidden = false;
  renderDirections();
  renderCase();
}
function renderLibrary() {
  const originals = cases.filter(c => !c.isDraft);
  $('case-count').textContent = originals.length;
  const button = (c,kept = false) => `<button class="case-button ${kept?'kept-case':''}" data-case="${escape(c.id)}" aria-current="${selectedId === c.id}"><strong>${escape(c.title)}</strong><span class="tag">${kept?'Saved scenario':escape(c.sourceLabel || (c.sourceKind === 'imported'?'Imported conversation':'Recorded conversation'))}</span></button>`;
  $('case-list').innerHTML = originals.map(c => button(c) + cases.filter(d => d.isDraft && d.sourceId === c.id).map(d => button(d,true)).join('')).join('') + cases.filter(c => c.isDraft && !originals.some(s => s.id === c.sourceId)).map(c => button(c,true)).join('');
}
function renderCase() {
  renderLibrary();
  $('case-summary').textContent = selected().summary || '';
  $('source-label').textContent = selected().isDraft ? `Saved scenario · source ${source().title}` : selected().sourceLabel || `${selected().sourceKind === 'imported'?'Imported':'Recorded'} conversation`;
  for (const element of $('scenario-form').elements) {
    if (element.name === 'title') element.value = draft().title;
    else if (Object.hasOwn(draft().scenario,element.name)) element.value = draft().scenario[element.name];
  }
  $('source-facts').innerHTML = ['goal','knownFacts','tone','persistence','status','estimate','acceptance','start'].map(key => `<div class="source-fact"><strong>${labels[key]}</strong>${escape(display(source().scenario[key]))}</div>`).join('');
  renderSourceTrace();
  refreshDetails();
  refreshDirection();
  applyLayout();
}
function renderSourceTrace() {
  const original = source();
  $('original-trace-title').textContent = original.title;
  $('original-trace-id').textContent = `Session ${original.id}`;
  $('original-trace-messages').innerHTML = original.preview?.messages?.length
    ? renderConversation({scenario:original.scenario,messages:original.preview.messages},true)
    : '<p class="empty">No recorded transcript is available for this session.</p>';
}

function refreshDetails() {
  const changed = changes();
  $('case-title').textContent = draft().title || 'Untitled scenario';
  $('draft-status').textContent = dirty() ? 'Unsaved changes' : selected().isDraft ? 'Saved scenario' : 'Source trace';
  $('draft-status').classList.toggle('dirty',dirty());
  $('change-count').textContent = `${changed.length} ${changed.length === 1?'change':'changes'}`;
  $('changes').innerHTML = changed.length ? changed.map(c => `<div class="change-row"><strong>${labels[c.key]}</strong><div class="change-values"><div class="before"><span class="value-caption">Source</span>${escape(display(c.before))}</div><div class="after"><span class="value-caption">Draft</span>${escape(display(c.after))}</div></div></div>`).join('') : '<p class="empty">No changes yet. Edit the brief or generate a variation.</p>';
  $('tool-json').textContent = JSON.stringify({order_id:'ORDER-1042',fulfillment_status:'fulfilled',status:draft().scenario.status,estimated_delivery:draft().scenario.estimate || null,tracking_url:draft().scenario.tracking || null},null,2);
  $('scenario-json').textContent = JSON.stringify({generation:draft().generation || null,source:sourceId(),title:draft().title,...draft().scenario},null,2);
  renderPreview();
  reportSize();
}
function chosenPolicy(value = $('agent-policy').value) { return value === 'candidate'?candidatePolicy:policies[value]; }
function renderPolicy(preferred) {
  const choice = typeof preferred === 'string'?preferred: $('agent-policy').value || 'fix';
  $('agent-policy').innerHTML = `<option value="fix" ${choice === 'fix'?'selected':''}>Evidence-based</option><option value="control" ${choice === 'control'?'selected':''}>Reassuring</option>${candidatePolicy?`<option value="candidate" ${choice === 'candidate'?'selected':''}>${escape(candidatePolicy.name)}</option>`:''}`;
  $('agent-policy').value = choice;
  const policy = chosenPolicy(choice);
  $('policy-name').value = policy?.name || '';
  $('policy-prompt').value = policy?.prompt || '';
  $('policy-help').textContent = choice === 'candidate'?'Reviewed candidate policy.':choice === 'control'?'Ask the agent to reassure the customer and give a concrete estimate.':'Only quote dates in tool evidence; acknowledge missing information.';
}
function acceptEditedPolicy() {
  const name = $('policy-name').value.trim();
  const prompt = $('policy-prompt').value.trim();
  if (!name || !prompt) { $('policy-edit-status').textContent = 'Give the policy a name and instructions.'; return; }
  candidatePolicy = {name,prompt};
  renderPolicy('candidate');
  setPolicy = 'candidate';
  $('policy-edit-status').textContent = 'Candidate ready. Run this scenario or compare on your test set.';
}
function policyContextMatches() {
  return policySuggestions && JSON.stringify(chosenPolicy()) === JSON.stringify(policySuggestions.baseline)
    && selectedId === policySuggestions.sourceId && scenarioKey(draft().scenario) === policySuggestions.scenarioKey;
}
function selectPolicy(index) {
  if (!policyContextMatches() || !policySuggestions.proposals[index]) return;
  selectedPolicyIndex = index;
  policyProposal = policySuggestions.proposals[index];
  renderPolicySuggestions();
}
function renderPolicySuggestions() {
  const proposals = policySuggestions?.proposals || [];
  $('policy-options').innerHTML = proposals.length ? `<table class="policy-table"><thead><tr><th scope="col">Policy</th><th scope="col">What changes</th></tr></thead><tbody>${proposals.map((item,index) => `<tr class="${index === selectedPolicyIndex?'selected':''}"><td><label><input type="radio" name="policy-option" value="${index}" ${index === selectedPolicyIndex?'checked':''}><strong>${escape(item.policy.name)}</strong></label></td><td>${escape(item.rationale)}</td></tr>`).join('')}</tbody></table>` : '';
  $('policy-proposal').innerHTML = policyProposal ? `<details class="policy-instructions"><summary>Inspect selected instructions</summary><pre>${escape(policyProposal.policy.prompt)}</pre></details>` : '';
  $('accept-policy').disabled = policyGenerating || !policyProposal || !policyContextMatches();
}
async function suggestPolicy() {
  if (policyGenerating || running) return;
  const baseline = chosenPolicy();
  if (!baseline) return;
  const instruction = $('policy-instruction').value.trim();
  const context = {baseline:copy(baseline),sourceId:selectedId,scenarioKey:scenarioKey(draft().scenario)};
  policyGenerating = true;
  policyProposal = null;
  policySuggestions = null;
  selectedPolicyIndex = null;
  $('suggest-policy').disabled = true;
  $('generate-policies').disabled = true;
  $('policy-status').textContent = 'Generating policy options…';
  renderPolicySuggestions();
  renderPreview();
  try {
    const result = await transport.call('policy',{baseline:copy(baseline),instruction,scenario:copy(draft().scenario)});
    if (JSON.stringify(chosenPolicy()) !== JSON.stringify(context.baseline) || selectedId !== context.sourceId || scenarioKey(draft().scenario) !== context.scenarioKey) throw new Error('The policy or scenario changed. Generate options for the current selection.');
    if (!Array.isArray(result.proposals) || result.proposals.length !== 3) throw new Error('No complete set of policy options was returned. Generate again.');
    policySuggestions = {...context,proposals:result.proposals};
    $('policy-status').textContent = 'Select a policy to try.';
  } catch (error) { $('policy-status').textContent = error.message; }
  finally {
    policyGenerating = false;
    $('suggest-policy').disabled = false;
    $('generate-policies').disabled = false;
    renderPolicySuggestions();
    renderPreview();
  }
}
function openPolicySuggestions() {
  if (running || policyGenerating) return;
  $('policy-instruction').value = '';
  $('policy-dialog').showModal();
  return suggestPolicy();
}
function runState(run) {
  if (run.receiptStatus === 'partial' || !['completed','boundary-completed'].includes(run.status)) return 'Incomplete';
  if (run.receiptPassed === false) return 'Fail';
  return run.checks?.no_unsupported_date && run.checks?.uses_supported_date ? 'Pass' : 'Fail';
}
function runError(run) { return run.receiptError || run.error; }
function testSetName(name) { return /^delivery-regression-tests-[0-9a-f]{8}$/.test(name)?'Regression test set':name; }
function renderSet() {
  const rows = testSet?.cases || [];
  $('test-set').innerHTML = `<div class="form-section"><div class="section-heading">${escape(testSetName(testSet?.name) || 'Test set')} <span class="pill">${rows.length} saved</span></div>${testSet?`<p class="section-intro version-label">Pinned version: <code>${escape(testSet.cohortVersionId)}</code></p>`:'<p class="section-intro">Run a scenario, then save the recorded run to create a test set.</p>'}${rows.map(c => `<div class="set-row"><div><strong>${escape(c.title)}</strong><span class="field-note">Saved recorded run</span></div><button data-edit="${escape(c.sessionId || c.id)}">Open scenario</button></div>`).join('')}<div class="actions"><button id="edit-baseline">Return to source case</button></div>${testSet?renderExperiment():''}</div>`;
  reportSize();
}
function policyOptions(current,includeCandidate = false) {
  return [['control','Reassuring'],['fix','Evidence-based'],...(includeCandidate && candidatePolicy?[['candidate',candidatePolicy.name]]:[])].map(([value,name]) => `<option value="${value}" ${current === value?'selected':''}>${escape(name)}</option>`).join('');
}
function experimentCaseState(item) { return item.status === 'completed'?(item.passed?'Pass':'Fail'):item.status === 'failed'?'Incomplete':'Pending'; }
function experimentArm(arm,name) {
  const cases = arm?.cases || [];
  return `<div class="experiment-arm"><strong>${escape(name)}</strong><p>${arm?`${cases.filter(c => experimentCaseState(c) === 'Pass').length} / ${cases.length} pass · ${escape(arm.status)}`:'Waiting to run'}</p>${cases.map(c => `<div class="set-row">${escape(testSet.cases.find(s => s.sessionId === c.source_session_id)?.title || 'Scenario')} · ${experimentCaseState(c)}${c.error?`<span class="field-note">${escape(c.error)}</span>`:''}${c.result_session_id?`<span class="field-note">Session ${escape(c.result_session_id)}</span>`:''}</div>`).join('')}</div>`;
}
function renderExperiment() {
  const active = experiment?.cohort_version_id === testSet.cohortVersionId?experiment:null;
  const runningExperiment = active?.status === 'running';
  const ready = active?.status === 'completed' && active.ready_for_handoff === true;
  const locked = comparing || runningExperiment;
  return `<div class="next-run"><div class="section-heading">Compare policies</div><div class="policy-controls"><label>Baseline<select id="baseline-policy" ${locked?'disabled':''}>${policyOptions(baselinePolicy)}</select></label><label>Candidate<select id="set-policy" ${locked?'disabled':''}>${policyOptions(setPolicy,true)}</select></label><button id="compare-set" class="primary" ${locked?'disabled':''}>${locked?'Experiment running…':'Compare policies'}</button></div><p class="field-note">Same ${testSet.cases.length} ${testSet.cases.length === 1?'case':'cases'} · same evaluators</p>${experimentError?`<p class="result-state" role="status">${escape(experimentError)}</p>`:''}${active?`<div class="experiment-summary"><strong>${active.status === 'completed'?'Experiment complete':active.status === 'failed'?'Experiment failed':'Running experiment'}</strong><div class="experiment-arms">${experimentArm(active.baseline,'Baseline')}${experimentArm(active.candidate,'Candidate')}</div><div class="actions">${experimentUrl?`<a href="${escape(experimentUrl)}" data-experiment-link="${escape(experimentUrl)}" target="_blank" rel="noopener noreferrer">Open experiment in Kitaru ↗</a>`:''}<button id="prepare-handoff" class="primary" ${ready && !handoffBusy && !comparing?'':'disabled'}>${handoffBusy?'Checking results…':'Prepare regression PR →'}</button>${runningExperiment?'<button id="refresh-experiment" class="ghost">Refresh</button>':''}</div><details data-experiment-details ${experimentDetailsOpen?'open':''}><summary>Experiment details</summary><p class="field-note">Experiment <code>${escape(active.experiment_id)}</code></p><p class="field-note">Baseline version <code>${escape(active.baseline_agent_version_id)}</code></p><p class="field-note">Candidate version <code>${escape(active.candidate_agent_version_id)}</code></p><p class="field-note">Evaluator <code>${escape(active.evaluator_name)} · ${escape(active.evaluator_version)}</code></p></details></div>`:''}</div>`;
}
async function startExperiment() {
  if (comparing || experiment?.status === 'running' || !testSet) return;
  const baseline = policies[baselinePolicy];
  const candidate = chosenPolicy(setPolicy);
  if (!baseline || !candidate) { toast('Select a baseline and candidate policy.'); return; }
  if (baseline.prompt === candidate.prompt) { toast('Choose two different policies to compare.'); return; }
  const version = testSet.cohortVersionId;
  comparing = true;
  experimentError = null;
  handoff = null;
  handoffSent = false;
  renderSet();
  try {
    const result = await transport.call('experiment',{cohort_version_id:version,baseline:copy(baseline),candidate:copy(candidate),model:'gpt-6-luna'});
    if (testSet.cohortVersionId !== version) return;
    if (!result.receipt?.experiment_id) throw new Error('No experiment receipt was returned.');
    experiment = result.receipt;
    experimentUrl = result.experiment_url || null;
    if (experiment.status === 'running') scheduleExperimentPoll();
  } catch (error) { experimentError = error.message; }
  finally { comparing = false; if (showingSet) renderSet(); }
}
function scheduleExperimentPoll() {
  clearTimeout(experimentTimer);
  experimentTimer = setTimeout(refreshExperiment,2500);
}
async function refreshExperiment() {
  if (experimentPolling || !experiment || experiment.status !== 'running') return;
  const original = experiment;
  experimentPolling = true;
  try {
    const result = await transport.call('experiment_status',{receipt:copy(original)});
    if (experiment?.experiment_id !== original.experiment_id) return;
    if (result.receipt?.experiment_id !== original.experiment_id) throw new Error('The experiment receipt changed unexpectedly.');
    experiment = result.receipt;
    experimentError = null;
    experimentUrl = result.experiment_url || experimentUrl;
    if (experiment.status === 'running') scheduleExperimentPoll();
  } catch (error) { experimentError = error.message; }
  finally { experimentPolling = false; if (showingSet) renderSet(); }
}
async function prepareHandoff() {
  if (comparing || handoffBusy || experiment?.cohort_version_id !== testSet?.cohortVersionId || !experiment || experiment.status !== 'completed' || experiment.ready_for_handoff !== true) return;
  handoffBusy = true;
  const receipt = copy(experiment);
  renderSet();
  try {
    const result = await transport.call('handoff',{receipt});
    if (experiment?.experiment_id !== receipt.experiment_id || testSet?.cohortVersionId !== receipt.cohort_version_id) return;
    if (!result.prompt || result.receipt?.experiment_id !== receipt.experiment_id || result.receipt.ready_for_handoff !== true) throw new Error('No validated regression handoff was returned.');
    handoff = result;
    handoffSent = false;
    $('handoff-prompt').value = result.prompt;
    $('handoff-status').textContent = transport.canMessage()?'Send the verified experiment to Codex to prepare the regression PR.':transport.messageUnavailableReason();
    $('send-handoff').hidden = !transport.canMessage();
    $('browser-handoff').hidden = transport.canMessage();
    $('handoff-details').open = false;
    $('send-handoff').disabled = false;
    $('handoff-dialog').showModal();
  } catch (error) { toast(error.message); }
  finally { handoffBusy = false; if (showingSet) renderSet(); }
}
async function sendHandoff() {
  if (!handoff || handoffSent || handoffBusy) return;
  handoffBusy = true;
  $('send-handoff').disabled = true;
  try {
    await transport.sendMessage(handoff.prompt);
    handoffSent = true;
    $('handoff-status').textContent = 'Sent to Codex. Continue in the chat.';
  } catch (error) { $('handoff-status').textContent = error.message; $('send-handoff').disabled = false; }
  finally { handoffBusy = false; }
}
function applyLayout() {
  $('app').dataset.step = showingSet?'set':step;
  document.querySelector('.steps').hidden = showingSet;
  document.querySelector('.editing-area').hidden = showingSet || ['source','preview'].includes(step);
  $('original-trace').hidden = showingSet || step !== 'source';
  document.querySelectorAll('[data-section]').forEach(element => element.hidden = showingSet || element.dataset.section !== step);
  $('changes-panel').hidden = showingSet || step !== 'review';
  $('preview-pane').hidden = showingSet || step !== 'preview';
  $('test-set').hidden = !showingSet;
  $('case-title').textContent = showingSet ? testSetName(testSet?.name) || 'Your test set' : step === 'source'?source().title:draft().title;
  $('draft-status').hidden = showingSet || step === 'source';
  document.querySelector('.evidence-disclosure').open = step === 'evidence';
  document.querySelector('.conversation-disclosure').open = step === 'preview';
  document.querySelector('#changes-panel details').open = step === 'review';
  document.querySelectorAll('[data-step]').forEach(element => element.setAttribute('aria-current',element.dataset.step === step?'step':'false'));
  $('next-step').hidden = showingSet || step === 'preview';
  $('back-step').hidden = showingSet || step === 'source';
  $('save-button').hidden = showingSet || step !== 'preview';
  $('revise-button').hidden = showingSet || step !== 'preview';
  $('set-button').hidden = showingSet;
  $('set-button').disabled = loadingSet;
  $('next-step').textContent = step === 'source'?'Create a test scenario →':step === 'variations'?'Continue with this scenario →':`Next: ${stepNames[steps[steps.indexOf(step)+1]] || ''} →`;
  $('ideas-button').hidden = direction === 'baseline';
  $('ideas-button').disabled = generating;
  if (showingSet) renderSet();
  reportSize();
}
function directions() {
  const titles = ['Change customer pressure','Change customer knowledge','Change tool evidence'];
  const reasons = ['Keep tool evidence fixed; change how the customer asks.','Keep tool evidence fixed; change what the customer can do.','Change the evidence and expected response together.'];
  return [{id:'baseline',title:'Keep the baseline',why:'Test the original situation without adding a variation.'},...ideas.map((idea,index) => ({id:typeof idea === 'string'?idea:idea.id,title:titles[index] || idea.title,why:reasons[index] || idea.why}))];
}
function renderDirections() { $('variation-directions').innerHTML = directions().map(d => `<button type="button" class="direction" data-direction="${escape(d.id)}" aria-pressed="${d.id === direction}"><span class="choice-dot" aria-hidden="true"></span><strong>${escape(d.title)}</strong></button>`).join(''); }
function refreshDirection() {
  document.querySelectorAll('[data-direction]').forEach(element => element.setAttribute('aria-pressed',String(element.dataset.direction === direction)));
  $('variation-reason').textContent = directions().find(d => d.id === direction)?.why || '';
}
function renderConversation(run,sourceOnly = false) {
  let section = '';
  return (run.messages || []).map((message,index) => {
    const opening = !sourceOnly && run.scenario.start === 'full' && index === 0 && message.role === 'customer';
    const historical = sourceOnly || message.historical;
    const context = historical || opening;
    const group = context?'context':'generated';
    const sourceOrigin = sourceOnly?(source().sourceKind === 'imported'?'Imported source conversation':'Recorded source conversation'):'Starting context derived from source';
    const divider = group !== section?`<div class="trace-divider">${context?(historical?sourceOrigin:'Scenario opening'):'New run'}${context?'':'<span>✦ AI-generated</span>'}</div>`:'';
    section = group;
    const title = message.role === 'customer'?'Customer':message.role === 'agent'?'Delivery agent':'check_shipping';
    const provenance = historical?sourceOrigin:opening?'Scenario opening':message.role === 'customer'?'AI-simulated customer':message.role === 'tool'?'Scenario tool fixture':'New agent response';
    const icon = message.role === 'customer'?'<circle cx="12" cy="7" r="3"/><path d="M5 21v-3a7 7 0 0 1 14 0v3"/>':message.role === 'agent'?'<rect x="4" y="7" width="16" height="13" rx="3"/><path d="M12 3v4M8 12v2M16 12v2M9 17h6"/>':'';
    const body = message.role === 'tool'?`<div class="tool-summary">${escape(run.scenario.status)} · Delivery estimate: ${escape(run.scenario.estimate || 'unknown')}</div><details><summary>Tool result JSON</summary><pre>${escape(message.text)}</pre></details>`:`<div class="message-body">${escape(message.text)}</div>`;
    return `${divider}<article class="trace-row ${escape(message.role)} ${context?'from-context':''}"><div class="trace-header"><span class="trace-number">${index+1}</span>${icon?`<svg class="speaker-icon" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5" stroke-linecap="round" aria-hidden="true">${icon}</svg>`:''}<strong title="${escape(provenance)}">${title}</strong>${!context && message.role !== 'tool'?`<span class="ai-mark" title="${escape(provenance)}" aria-label="${escape(provenance)}" tabindex="0">✦</span>`:''}</div><div class="trace-content">${body}</div></article>`;
  }).join('');
}
function renderPreview() {
  const matching = matchingRuns();
  const matchingLatest = latestRun();
  const latest = matchingLatest;
  const runChoices = [...new Map(matching.map(r => [r.policyHash || r.variant,r])).values()];
  $('run-picker').innerHTML = runChoices.length > 1 ? runChoices.map(r => `<button data-run="${escape(r.runId)}" aria-pressed="${latest?.runId === r.runId}">${escape(r.policy?.name || (r.variant === 'fix'?'Evidence-based':'Reassuring'))} result</button>`).join('') : '';
  const savedPreview = source().preview?.messages?.length;
  $('preview-context').textContent = latest?`${escape(latest.policy?.name || (latest.variant === 'fix'?'Evidence-based':'Reassuring'))} policy · ${latest.agentTurns} agent turns` : savedPreview?'Source conversation shown below. Run the current scenario to inspect new responses.':'Run this scenario to inspect the conversation.';
  $('run-scenario').disabled = running || generating || saving || policyGenerating;
  $('suggest-policy').disabled = running || generating || saving || policyGenerating;
  $('agent-policy').disabled = running || generating || saving;
  $('save-button').disabled = running || saving || !matchingLatest?.sessionId || runState(matchingLatest) === 'Incomplete';
  $('save-button').textContent = saving?'Saving recorded run…':'Save run to test set';
  $('save-button').title = matchingLatest?.sessionId?'Save the matching recorded run':'Run the current draft before saving it';
  $('conversation').innerHTML = latest?renderConversation(latest):savedPreview?renderConversation({scenario:source().scenario,messages:source().preview.messages},true):'<p class="empty">Run the scenario to see its inputs and new responses here.</p>';
  $('sample-outcome').hidden = !latest;
  if (latest) {
    const complete = ['completed','boundary-completed'].includes(latest.status);
    const status = complete?(latest.receiptStatus === 'partial'?'Simulation complete · recording incomplete':'Simulation complete'):latest.status === 'turn-limit'?'Simulation incomplete: turn limit reached':'Simulation incomplete: model or simulator error';
    $('sample-outcome').innerHTML = `<strong>${status}</strong>${runError(latest)?`<p>${escape(runError(latest))}</p>`:''}<p>${complete?'Preview checks':'Partial preview checks'}:</p><p>${latest.checks.no_unsupported_date?'✓':'✗'} No unsupported delivery date</p><p>${latest.checks.uses_supported_date?'✓':'✗'} Uses the supported date when available</p>${latest.sessionId?`<span class="field-note">Recorded session: ${escape(latest.sessionId)}</span>`:''}`;
  }
}
function validateDraft() {
  const invalid = Array.from($('scenario-form').elements).find(element => element.name && !element.checkValidity());
  if (!invalid) return true;
  step = invalid.closest('[data-section]').dataset.section;
  applyLayout();
  const disclosure = invalid.closest('details');
  if (disclosure) disclosure.open = true;
  invalid.reportValidity();
  return false;
}
async function openTestSet() {
  loadingSet = true;
  $('set-button').disabled = true;
  try {
    if (testSet) updateTestSet(await transport.call('set',{cohort_version_id:testSet.cohortVersionId}));
    showingSet = true;
    renderLibrary();
    applyLayout();
  } catch (error) { toast(error.message); }
  finally { loadingSet = false; $('set-button').disabled = false; }
}
async function saveRun() {
  if (saving || !validateDraft()) return;
  const run = latestRun();
  if (!run?.sessionId) { toast('Run the current draft before saving it.'); return; }
  saving = true;
  renderPreview();
  try {
    const saved = await transport.call('keep',{session_id:run.sessionId,...(testSet?{cohort_id:testSet.cohortId}:{})});
    updateTestSet(saved);
    selectedId = saved.cases.find(c => c.sessionId === run.sessionId)?.sessionId || selectedId;
    showingSet = true;
    renderCase();
    toast(`Recorded run saved to ${testSetName(saved.name)}.`);
  } catch (error) { toast(error.message); }
  finally { saving = false; renderPreview(); }
}
function proposalSnapshot() { return JSON.stringify({id:selectedId,direction,title:draft().title,scenario:draft().scenario}); }
async function propose() {
  if (generating || !validateDraft()) return;
  generating = true;
  pendingProposal = null;
  $('generation-status').textContent = 'Generating…';
  $('ideas-button').disabled = true;
  const snapshot = proposalSnapshot();
  try {
    const proposal = await transport.call('propose',{sourceId:sourceId(),title:draft().title,direction,scenario:copy(draft().scenario)});
    if (snapshot !== proposalSnapshot()) throw new Error('The draft changed while generating. Generate again for the current draft.');
    pendingProposal = {...proposal,snapshot};
    $('proposal-origin').textContent = `✦ AI-generated · ${proposal.generation.provider} / ${proposal.generation.model}`;
    $('idea-list').innerHTML = `<div class="generated-proposal"><h3>${escape(proposal.title)}</h3><p>${escape(proposal.why)}</p><div class="field-note">Validated: only permitted fields changed. Review whether the variation is useful.</div>${Object.entries(proposal.changes).map(([key,value]) => `<div class="change-row"><strong>${labels[key]}</strong><div class="change-values"><div class="before"><span class="value-caption">Current</span>${escape(display(draft().scenario[key]))}</div><div class="after"><span class="value-caption">Proposed</span>${escape(display(value))}</div></div></div>`).join('')}</div>`;
    $('generation-status').textContent = 'Proposal ready for review.';
    $('ideas-dialog').showModal();
  } catch (error) { $('generation-status').textContent = error.message; }
  finally { generating = false; $('ideas-button').disabled = false; }
}
async function runScenario(variant) {
  if (running || generating || !validateDraft()) return;
  const id = selectedId;
  const target = draft();
  const title = target.title;
  const snapshot = scenarioKey(target.scenario);
  running = true;
  $('run-status').textContent = `Running ${chosenPolicy()?.name || (variant === 'control'?'reassuring':'evidence-based')} policy…`;
  renderPreview();
  try {
    const result = await transport.call('run',{title,sourceId:sourceId(),scenario:copy(target.scenario),variant,...(chosenPolicy()?{policy:copy(chosenPolicy())}:{})});
    target.runs ||= [];
    target.runs.push({...result,_title:title});
    target.viewRunId = result.runId;
    if (selectedId !== id || scenarioKey(draft().scenario) !== snapshot || draft().title !== title) { toast('Run finished for the earlier draft. Return to that draft to inspect it.'); return; }
    $('run-status').textContent = '';
  } catch (error) { $('run-status').textContent = error.message; }
  finally { running = false; renderPreview(); reportSize(); }
}
$('case-list').addEventListener('click',event => {
  const button = event.target.closest('[data-case]');
  if (!button) return;
  selectedId = button.dataset.case;
  step = selected().isDraft?'preview':'source';
  showingSet = false;
  direction = 'baseline';
  $('run-status').textContent = '';
  $('generation-status').textContent = '';
  renderCase();
});
$('scenario-form').addEventListener('input',event => {
  const element = event.target;
  if (!element.name || !(element.name === 'title' || Object.hasOwn(draft().scenario,element.name))) return;
  if (element.name === 'title') draft().title = element.value;
  else draft().scenario[element.name] = element.name === 'maxTurns'?Number(element.value):element.value;
  refreshDetails();
});
$('scenario-form').addEventListener('change',() => refreshDetails());
['title','goal','opening','acceptance'].forEach(name => $('scenario-form').elements.namedItem(name).required = true);
$('save-button').addEventListener('click',saveRun);
$('ideas-button').addEventListener('click',propose);
$('apply-idea').addEventListener('click',() => {
  if (!pendingProposal) return;
  if (pendingProposal.snapshot !== proposalSnapshot()) { $('ideas-dialog').close(); $('generation-status').textContent = 'The draft changed. Generate a new proposal before applying.'; return; }
  Object.assign(draft().scenario,copy(pendingProposal.changes));
  draft().title = pendingProposal.title;
  draft().generation = copy(pendingProposal.generation);
  pendingProposal = null;
  $('ideas-dialog').close();
  step = 'review';
  renderCase();
  toast('AI proposal applied. Run it to inspect the resulting conversation.');
});
$('close-dialog').addEventListener('click',() => $('ideas-dialog').close());
$('next-step').addEventListener('click',() => { step = steps[Math.min(steps.indexOf(step)+1,steps.length-1)]; applyLayout(); });
$('back-step').addEventListener('click',() => { step = steps[Math.max(steps.indexOf(step)-1,0)]; applyLayout(); });
$('revise-button').addEventListener('click',() => { step = 'brief'; applyLayout(); });
$('set-button').addEventListener('click',openTestSet);
$('fullscreen').addEventListener('click',() => transport.fullscreen().catch(error => toast(error.message)));
document.querySelectorAll('[data-step]').forEach(button => button.addEventListener('click',() => { step = button.dataset.step; applyLayout(); }));
$('variation-directions').addEventListener('click',event => {
  const button = event.target.closest('[data-direction]');
  if (!button) return;
  direction = button.dataset.direction;
  refreshDirection();
  applyLayout();
});
$('test-set').addEventListener('toggle',event => { if (event.target.dataset.experimentDetails !== undefined) experimentDetailsOpen = event.target.open; },true);
$('test-set').addEventListener('change',event => { if (event.target.id === 'set-policy') setPolicy = event.target.value; if (event.target.id === 'baseline-policy') baselinePolicy = event.target.value; });
$('test-set').addEventListener('click',async event => {
  const edit = event.target.closest('[data-edit]');
  if (edit) {
    selectedId = edit.dataset.edit;
    step = 'preview';
    showingSet = false;
    renderCase();
  } else if (event.target.closest('#edit-baseline')) {
    selectedId = source().id;
    step = 'source';
    direction = 'baseline';
    showingSet = false;
    renderCase();
  } else if (event.target.closest('#compare-set')) await startExperiment();
  else if (event.target.closest('#refresh-experiment')) await refreshExperiment();
  else if (event.target.closest('#prepare-handoff')) await prepareHandoff();
  else if (event.target.closest('[data-experiment-link]')) {
    event.preventDefault();
    try { await transport.openLink(event.target.closest('[data-experiment-link]').dataset.experimentLink); } catch (error) { toast(error.message); }
  }

});
$('run-scenario').addEventListener('click',() => runScenario($('agent-policy').value === 'control'?'control':'fix'));
$('run-picker').addEventListener('click',event => { const button = event.target.closest('[data-run]'); if (button) { draft().viewRunId = button.dataset.run; renderPreview(); } });
$('agent-policy').addEventListener('change',renderPolicy);
$('apply-policy').addEventListener('click',acceptEditedPolicy);
$('reset-policy').addEventListener('click',renderPolicy);
$('suggest-policy').addEventListener('click',openPolicySuggestions);
$('generate-policies').addEventListener('click',suggestPolicy);
$('policy-options').addEventListener('change',event => { if (event.target.name === 'policy-option') selectPolicy(Number(event.target.value)); });
$('accept-policy').addEventListener('click',() => {
  if (policyGenerating || !policyProposal || !policyContextMatches()) { toast('Select a policy option for the current scenario.'); return; }
  candidatePolicy = copy(policyProposal.policy);
  policyProposal = null;
  $('policy-dialog').close();
  renderPolicy('candidate');
  setPolicy = 'candidate';
  toast('Candidate policy accepted. Run it or compare on your test set.');
});
$('close-policy-dialog').addEventListener('click',() => $('policy-dialog').close());
$('send-handoff').addEventListener('click',sendHandoff);
$('close-handoff').addEventListener('click',() => $('handoff-dialog').close());
async function copyHandoff() {
  if (!handoff) return;
  try { await navigator.clipboard.writeText(handoff.prompt); $('handoff-status').textContent = 'Prompt copied. Paste it into Codex.'; }
  catch { $('handoff-details').open = true; $('handoff-prompt').select(); $('handoff-status').textContent = 'Copy the selected prompt into Codex.'; }
}
$('copy-handoff').addEventListener('click',copyHandoff);
$('browser-handoff').addEventListener('click',copyHandoff);
transport.subscribe((method,params) => {
  if (method === 'ui/notifications/tool-result') {
    const envelope = transport.envelopeOf(params);
    if (envelope?.ok && Array.isArray(envelope.data?.cases) && envelope.data.ideas) {
      try { loadLaunch(envelope.data); } catch (error) { $('app-status').textContent = error.message; }
    } else if (envelope && !envelope.ok) $('app-status').textContent = envelope.error?.message || 'Could not load source conversations.';
  } else if (method === 'ui/notifications/tool-cancelled') $('app-status').textContent = 'Opening source conversations was cancelled.';
  else if (method === 'ui/notifications/host-context-changed') {
    $('fullscreen').hidden = !(params.availableDisplayModes || []).includes('fullscreen');
    $('fullscreen').setAttribute('aria-label',params.displayMode === 'fullscreen'?'Exit fullscreen':'Enter fullscreen');
  }
});
new ResizeObserver(reportSize).observe(document.body);
transport.initialize().then(data => { if (data) loadLaunch(data); }).catch(error => { $('app-status').textContent = error.message; });
