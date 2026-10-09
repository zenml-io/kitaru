/* MCP Apps JSON-RPC transport and the standalone browser transport. */
(function (root) {
  function envelopeOf(result) {
    if (result?.structuredContent) return result.structuredContent;
    if (typeof result?.ok === 'boolean') return result;
    for (const content of result?.content || []) {
      if (content.type !== 'text') continue;
      try { return JSON.parse(content.text); } catch { /* Continue to structured text. */ }
    }
    return null;
  }
  function unwrap(result) {
    const envelope = envelopeOf(result);
    if (!envelope || !envelope.ok || result?.isError) {
      const message = envelope?.error?.message || envelope?.error || result?.content?.find(c => c.type === 'text')?.text;
      throw new Error(typeof message === 'string' ? message : 'The request could not be completed.');
    }
    return envelope.data;
  }
  function createTransport(win) {
    let nextId = 1;
    const pending = new Map();
    const framed = win.parent !== win;
    const protocolVersion = '2026-01-26';
    const routes = {propose:'/api/proposals',run:'/api/runs',keep:'/api/keep',set:'/api/set',compare:'/api/compare',policy:'/api/policy',experiment:'/api/experiment',experiment_status:'/api/experiment_status',handoff:'/api/handoff'};
    let onNotification = () => {};
    let hostContext = {};
    let hostCapabilities = {};
    let initialized = false;
    function request(method, params, timeoutMs = 180000) {
      const id = nextId++;
      return new Promise((resolve, reject) => {
        const timer = win.setTimeout(() => {
          if (pending.delete(id)) reject(new Error(method === 'ui/message'?'Codex did not confirm delivery. Check the chat before sending again.':`${method} timed out. Check recorded runs before trying again.`));
        }, timeoutMs);
        pending.set(id, {resolve,reject,timer});
        win.parent.postMessage({jsonrpc:'2.0',id,method,params}, '*');
      });
    }
    function notify(method, params) {
      if (framed) win.parent.postMessage({jsonrpc:'2.0',method,params}, '*');
    }
    win.addEventListener('message', event => {
      if (!framed || event.source !== win.parent) return;
      const message = event.data;
      if (!message || message.jsonrpc !== '2.0') return;
      if (message.id != null && !message.method) {
        const waiter = pending.get(message.id);
        if (waiter) {
          pending.delete(message.id);
          win.clearTimeout(waiter.timer);
          message.error ? waiter.reject(new Error(message.error.message || 'Request failed.')) : waiter.resolve(message.result);
        }
        return;
      }
      if (message.id != null) {
        const known = message.method === 'ui/resource-teardown' || message.method === 'ping';
        win.parent.postMessage(known ? {jsonrpc:'2.0',id:message.id,result:{}} : {jsonrpc:'2.0',id:message.id,error:{code:-32601,message:'Method not found'}}, '*');
        return;
      }
      if (message.method === 'ui/notifications/host-context-changed') hostContext = {...hostContext,...message.params};
      onNotification(message.method, message.params || {});
    });
    async function browserRequest(path, body) {
      const response = await win.fetch(path, body === undefined ? {} : {
        method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(body),
      });
      const result = await response.json();
      if (!response.ok && !envelopeOf(result)) throw new Error(result.error || 'The request failed.');
      return unwrap(result);
    }
    return {
      framed, envelopeOf, unwrap,
      subscribe(callback) { onNotification = callback; },
      async initialize() {
        if (!framed) {
          const search = new URLSearchParams(win.location.search);
          const params = new URLSearchParams();
          for (const key of ['session_ids','cohort_version_id']) if (search.has(key)) params.set(key,search.get(key));
          return browserRequest(`/api/cases${params.size ? `?${params}` : ''}`);
        }
        initialized = false;
        const result = await request('ui/initialize', {
          protocolVersion,appInfo:{name:'kitaru-scenario-editor',version:'1.0.0'},
          appCapabilities:{availableDisplayModes:['inline','fullscreen']},
        },15000);
        if (result?.protocolVersion !== protocolVersion) throw new Error('The MCP host did not negotiate a supported App protocol version.');
        if (!result.hostInfo?.name || !result.hostInfo?.version || !result.hostCapabilities || typeof result.hostCapabilities !== 'object' || Array.isArray(result.hostCapabilities) || !result.hostContext || typeof result.hostContext !== 'object' || Array.isArray(result.hostContext)) throw new Error('The MCP host returned an incomplete App initialization response.');
        hostContext = result.hostContext;
        hostCapabilities = result.hostCapabilities;
        onNotification('ui/notifications/host-context-changed',hostContext);
        notify('ui/notifications/initialized',{});
        initialized = true;
        return null;
      },
      async call(action, body) {
        if (!Object.hasOwn(routes,action)) throw new Error('Unknown editor action.');
        return framed ? unwrap(await request('tools/call', {
          name:`kitaru_simulation_${action}`,arguments:{request:body},
        },action === 'compare'?960000:210000)) : browserRequest(routes[action],body);
      },
      canMessage() { return framed && initialized && Object.hasOwn(hostCapabilities.message || {},'text'); },
      messageUnavailableReason() {
        if (!framed) return 'Open this App in Codex to send the handoff.';
        if (!initialized) return 'The App has not connected to Codex yet.';
        if (!Object.hasOwn(hostCapabilities.message || {},'text')) return 'This host does not support sending messages to chat.';
        return '';
      },
      async sendMessage(text) {
        const unavailable = this.messageUnavailableReason();
        if (unavailable) throw new Error(unavailable);
        const result = await request('ui/message',{role:'user',content:[{type:'text',text}]},15000);
        if (result?.isError) throw new Error('Codex did not accept the handoff. You can retry sending it.');
      },
      async openLink(url) {
        if (framed && Object.hasOwn(hostCapabilities,'openLinks')) {
          const result = await request('ui/open-link',{url},15000);
          if (result?.isError) throw new Error('The link could not be opened.');
        } else win.open(url,'_blank','noopener,noreferrer');
      },
      async fullscreen() {
        const mode = hostContext.displayMode === 'fullscreen' ? 'inline' : 'fullscreen';
        const result = await request('ui/request-display-mode',{mode});
        hostContext.displayMode = result?.mode || mode;
        onNotification('ui/notifications/host-context-changed',hostContext);
      },
      reportSize(height) { notify('ui/notifications/size-changed',{height}); },
    };
  }
  if (typeof module !== 'undefined' && module.exports) module.exports = {createTransport,envelopeOf,unwrap};
  else root.scenarioBridge = {createTransport,envelopeOf,unwrap};
})(typeof window === 'undefined' ? globalThis : window);
