const {test} = require('node:test');
const assert = require('node:assert/strict');
const {createTransport,envelopeOf,unwrap} = require('./bridge.js');

function hostWindow() {
  const sent = [];
  let listener;
  const parent = {postMessage:message => sent.push(message)};
  const win = {
    parent,setTimeout,clearTimeout,
    addEventListener:(_,callback) => listener = callback,
    fetch:() => { throw new Error('An iframe must never call the local HTTP API.'); },
  };
  return {win,parent,sent,receive:(source,data) => listener({source,data})};
}

function initializationResult(overrides = {}) {
  return {protocolVersion:'2026-01-26',hostInfo:{name:'Codex',version:'1.0.0'},hostCapabilities:{message:{text:{}}},hostContext:{},...overrides};
}

async function initializeHost(host,transport,overrides) {
  const initialized = transport.initialize();
  host.receive(host.parent,{jsonrpc:'2.0',id:host.sent.at(-1).id,result:initializationResult(overrides)});
  await initialized;
}

test('structured and text tool results unwrap to the same data',() => {
  const data = {cases:[{id:'source-session'}]};
  assert.deepEqual(unwrap({structuredContent:{ok:true,data}}),data);
  assert.deepEqual(unwrap({content:[{type:'text',text:'Not the envelope'},{type:'text',text:JSON.stringify({ok:true,data})}]}),data);
  assert.deepEqual(envelopeOf({ok:true,data}),{ok:true,data});
  assert.throws(() => unwrap({structuredContent:{ok:false,error:{message:'Source not found'}}}),/Source not found/);
  assert.throws(() => unwrap({isError:true,content:[{type:'text',text:'Execution failed'}]}),/Execution failed/);
});

test('iframe tools use the request wrapper and reject messages from other sources',async () => {
  const host = hostWindow();
  const transport = createTransport(host.win);
  const body = {session_id:'recorded-run',cohort_id:'test-set'};
  const result = transport.call('keep',body);
  const message = host.sent[0];
  assert.equal(message.method,'tools/call');
  assert.deepEqual(message.params,{name:'kitaru_simulation_keep',arguments:{request:body}});
  host.receive({}, {jsonrpc:'2.0',id:message.id,result:{structuredContent:{ok:true,data:{name:'Wrong source'}}}});
  host.receive(host.parent,{jsonrpc:'2.0',id:message.id,result:{structuredContent:{ok:true,data:{name:'Saved set'}}}});
  assert.deepEqual(await result,{name:'Saved set'});
});

test('MCP initialization advertises fullscreen and forwards launch notifications',async () => {
  const host = hostWindow();
  const transport = createTransport(host.win);
  const notifications = [];
  transport.subscribe((method,params) => notifications.push({method,params}));
  const initialized = transport.initialize();
  const message = host.sent[0];
  assert.equal(message.method,'ui/initialize');
  assert.deepEqual(message.params.appCapabilities.availableDisplayModes,['inline','fullscreen']);
  host.receive(host.parent,{jsonrpc:'2.0',id:message.id,result:initializationResult({hostContext:{availableDisplayModes:['inline','fullscreen']}})});
  assert.equal(await initialized,null);
  assert.equal(host.sent[1].method,'ui/notifications/initialized');
  const launch = {structuredContent:{ok:true,data:{cases:[],ideas:[]}}};
  host.receive(host.parent,{jsonrpc:'2.0',method:'ui/notifications/tool-result',params:launch});
  assert.deepEqual(notifications.at(-1),{method:'ui/notifications/tool-result',params:launch});
  const fullscreen = transport.fullscreen();
  const request = host.sent.at(-1);
  assert.deepEqual(request.params,{mode:'fullscreen'});
  host.receive(host.parent,{jsonrpc:'2.0',id:request.id,result:{mode:'fullscreen'}});
  await fullscreen;
});

test('browser transport forwards source IDs and parses the same envelope',async () => {
  const calls = [];
  const win = {location:{search:'?session_ids=a,b&cohort_version_id=pinned'},addEventListener:() => {},fetch:async (path,options) => {
    calls.push({path,options});
    return {ok:true,json:async () => ({ok:true,data:{name:'Persistent set'}})};
  }};
  win.parent = win;
  const transport = createTransport(win);
  assert.deepEqual(await transport.initialize(),{name:'Persistent set'});
  assert.equal(calls[0].path,'/api/cases?session_ids=a%2Cb&cohort_version_id=pinned');
  await transport.call('compare',{cohort_version_id:'pinned',variant:'fix'});
  assert.equal(calls[1].path,'/api/compare');
  assert.deepEqual(JSON.parse(calls[1].options.body),{cohort_version_id:'pinned',variant:'fix'});
  await transport.call('experiment_status',{receipt:{experiment_id:'native'}});
  assert.equal(calls[2].path,'/api/experiment_status');
  assert.deepEqual(JSON.parse(calls[2].options.body),{receipt:{experiment_id:'native'}});
});

test('user handoff uses host ui/message content blocks and reports rejection',async () => {
  const host = hostWindow();
  const transport = createTransport(host.win);
  await initializeHost(host,transport);
  assert.equal(transport.canMessage(),true);
  const prompt = 'Use the regression skill with the validated pinned receipt.';
  const sending = transport.sendMessage(prompt);
  const message = host.sent.at(-1);
  assert.equal(message.method,'ui/message');
  assert.deepEqual(message.params,{role:'user',content:[{type:'text',text:prompt}]});
  host.receive(host.parent,{jsonrpc:'2.0',id:message.id,result:{}});
  await sending;
  const rejected = transport.sendMessage(prompt);
  host.receive(host.parent,{jsonrpc:'2.0',id:host.sent.at(-1).id,result:{isError:true}});
  await assert.rejects(rejected,/did not accept/);
  assert.equal(transport.canMessage(),true);
  const retry = transport.sendMessage(prompt);
  assert.equal(host.sent.at(-1).method,'ui/message');
  host.receive(host.parent,{jsonrpc:'2.0',id:host.sent.at(-1).id,result:{}});
  await retry;
});

test('an arbitrary iframe cannot message until a valid host advertises text messages',async () => {
  const host = hostWindow();
  const transport = createTransport(host.win);
  assert.equal(transport.canMessage(),false);
  assert.match(transport.messageUnavailableReason(),/not connected/);
  await assert.rejects(transport.sendMessage('Do not send'),/not connected/);
  assert.equal(host.sent.length,0);
  await initializeHost(host,transport,{hostCapabilities:{message:{image:{}}}});
  assert.equal(transport.canMessage(),false);
  assert.match(transport.messageUnavailableReason(),/does not support/);
  const before = host.sent.length;
  await assert.rejects(transport.sendMessage('Do not send'),/does not support/);
  assert.equal(host.sent.length,before);
  await initializeHost(host,transport);
  assert.equal(transport.canMessage(),true);
  assert.equal(transport.messageUnavailableReason(),'');
});

test('a message timeout reports uncertain delivery without retrying or falling back',async () => {
  const host = hostWindow();
  const transport = createTransport(host.win);
  await initializeHost(host,transport);
  host.win.setTimeout = callback => { queueMicrotask(callback); return 0; };
  const before = host.sent.length;
  await assert.rejects(transport.sendMessage('Validated handoff'),{message:'Codex did not confirm delivery. Check the chat before sending again.'});
  assert.equal(host.sent.length,before+1);
  assert.equal(host.sent.at(-1).method,'ui/message');
});

test('unsupported or malformed host negotiation never enables messaging',async () => {
  for (const overrides of [{protocolVersion:'2099-01-01'},{protocolVersion:undefined},{hostInfo:{}},{hostCapabilities:null},{hostContext:null}]) {
    const host = hostWindow();
    const transport = createTransport(host.win);
    await assert.rejects(initializeHost(host,transport,overrides),/protocol|initialization/);
    assert.equal(transport.canMessage(),false);
    assert.equal(host.sent.filter(message => message.method === 'ui/notifications/initialized').length,0);
  }
});

test('browser handoff never posts a chat message and only advertised hosts receive link requests',async () => {
  const win = {addEventListener() {}};
  win.parent = win;
  const browser = createTransport(win);
  assert.equal(browser.canMessage(),false);
  await assert.rejects(browser.sendMessage('Validated handoff'),/Open this App in Codex/);
  const host = hostWindow();
  const transport = createTransport(host.win);
  await initializeHost(host,transport,{hostCapabilities:{openLinks:{}}});
  const open = transport.openLink('https://kitaru.example/experiment/id');
  const message = host.sent.at(-1);
  assert.equal(message.method,'ui/open-link');
  assert.deepEqual(message.params,{url:'https://kitaru.example/experiment/id'});
  host.receive(host.parent,{jsonrpc:'2.0',id:message.id,result:{}});
  await open;
});

test('native experiment and receipt validation actions retain the request envelope',async () => {
  const host = hostWindow();
  const transport = createTransport(host.win);
  for (const action of ['policy','experiment','experiment_status','handoff']) {
    const body = {receipt:{experiment_id:'pinned-experiment'}};
    const calling = transport.call(action,body);
    const message = host.sent.at(-1);
    assert.deepEqual(message.params,{name:`kitaru_simulation_${action}`,arguments:{request:body}});
    host.receive(host.parent,{jsonrpc:'2.0',id:message.id,result:{structuredContent:{ok:true,data:{validated:true}}}});
    assert.deepEqual(await calling,{validated:true});
  }
});
