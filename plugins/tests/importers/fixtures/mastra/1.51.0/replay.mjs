// Recorded-context integration probe requiring the context-capable #1050 adapter.
// Run after building packages/core and packages/mastra with Node 22.
// Input: {sessions: ImportedSession[], history: [{tool_name, inputs, cache_key,
// result, status}]}; compute each cache_key with Python compute_tool_cache_key.
import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import { createRequire } from 'node:module';
import { resolve } from 'node:path';
import { pathToFileURL } from 'node:url';

const root = process.env.KITARU_MASTRA_ADAPTER_ROOT
  ? pathToFileURL(`${resolve(process.env.KITARU_MASTRA_ADAPTER_ROOT)}/`)
  : new URL('../../../../../../', import.meta.url);
const requireMastra = createRequire(new URL('packages/mastra/package.json', root));
const { Agent } = await import(requireMastra.resolve('@mastra/core/agent'));
const { createTool } = await import(requireMastra.resolve('@mastra/core/tools'));
const { z } = await import(requireMastra.resolve('zod/v4'));
const { KitaruAgent } = await import(new URL('packages/mastra/dist/index.js', root));
assert.ok(process.argv[2], 'Pass the Python-normalized sessions/history JSON path');
const { sessions, history } = JSON.parse(await readFile(process.argv[2], 'utf8'));
assert.equal(sessions.length, 2);
assert.deepEqual(history[0].inputs, { value: '3' });
const replayId = '018f0000-0000-7000-8000-000000000101';
const sessionId = '018f0000-0000-7000-8000-000000000102';
const apiCalls = [];
const prompts = [];
let liveExecutions = 0;
let lookups = 0;
let turn = 0;
const liveInstructions = 'LIVE INSTRUCTIONS MUST NOT REPLACE RECORDED SYSTEM MESSAGES';
const response = (body) => new Response(JSON.stringify(body), {
  headers: { 'Content-Type': 'application/json' },
});
const originalFetch = globalThis.fetch;
const savedEnvironment = { ...process.env };
globalThis.fetch = async (input, options = {}) => {
  const url = new URL(String(input));
  assert.equal(url.origin, 'https://fixture.invalid', 'Unexpected external request');
  const method = options.method ?? 'GET';
  const body = options.body ? JSON.parse(String(options.body)) : undefined;
  apiCalls.push({ path: url.pathname, method, body });
  if (method === 'GET' && url.pathname === `/api/v1/replays/${replayId}`) {
    return response({ id: replayId, job_id: sessionId, baseline_session_id: sessionId,
      status: 'pending', override: null,
      tool_policy: { default: { type: 'history', scope: 'baseline', on_miss: 'fail' }, tools: {} } });
  }
  if (method === 'POST' && url.pathname.endsWith('/tool-lookup')) {
    lookups++;
    const match = history.find((entry) => entry.tool_name === body.tool_name && entry.cache_key === body.cache_key);
    assert.ok(match, 'Lookup must match Python cache key for exported raw arguments');
    assert.equal(body.occurrence, 0);
    return response({ match: { result: match.result, status: match.status, error: null } });
  }
  if (method === 'POST' && url.pathname === '/api/v1/sessions') {
    assert.deepEqual(body.inputs, sessions[turn].inputs);
    return response({ id: sessionId, origin: 'replay', status: 'in_progress' });
  }
  if (method === 'POST' && url.pathname.endsWith('/nodes')) {
    return response(body.nodes.map((node) => ({ id: sessionId, index: node.index,
      node_type: node.node_type, status: node.status })));
  }
  if (method === 'PATCH' && url.pathname.startsWith('/api/v1/sessions/')) {
    return response({ id: sessionId, origin: 'replay', status: body.status ?? 'in_progress' });
  }
  throw new Error(`Unexpected fixture API call: ${method} ${url.pathname}`);
};
try {
  for (const name of Object.keys(process.env)) if (name.startsWith('KITARU_')) delete process.env[name];
  process.env.KITARU_REPLAY_ID = replayId;
  for (turn = 0; turn < sessions.length; turn++) {
    process.env.KITARU_TASK_INPUTS = JSON.stringify(sessions[turn].inputs);
    let step = 0;
    const model = {
      specificationVersion: 'v2', supportedUrls: {}, modelId: 'fixture-model', provider: 'fixture',
      doGenerate: async ({ prompt }) => {
        prompts.push({ turn, prompt });
        const expectedSystem = sessions[turn].inputs.mastra_conversation_context.messages
          .filter((message) => message.role === 'system');
        assert.deepEqual(prompt.filter((message) => message.role === 'system'), expectedSystem,
          'Model must receive the original system messages exactly');
        assert.ok(!JSON.stringify(prompt).includes(liveInstructions),
          'Live configured instructions must not enter the replay prompt');
        let content;
        let finishReason = 'stop';
        if (turn === 0 && step++ === 0) {
          content = [{ type: 'tool-call', toolCallId: 'call-double', toolName: 'double', input: JSON.stringify(history[0].inputs) }];
          finishReason = 'tool-calls';
        } else if (turn === 0) {
          assert.ok(JSON.stringify(prompt).includes('"doubled":6'), 'Model must receive recorded tool result');
          content = [{ type: 'text', text: 'The result is 6.' }];
        } else {
          const priorAnswer = prompt.find((message) => message.role === 'assistant');
          assert.ok(priorAnswer, 'Second turn must include prior assistant history');
          const priorText = typeof priorAnswer.content === 'string' ? priorAnswer.content : priorAnswer.content.map((part) => part.text ?? '').join('');
          const previousNumber = priorText.match(/\b\d+\b/)?.[0];
          assert.ok(previousNumber, 'Second answer must derive its number from prior history');
          content = [{ type: 'text', text: `The previous result was ${previousNumber}.` }];
        }
        return { content, finishReason, usage: { inputTokens: 20, outputTokens: 5, totalTokens: 25 }, warnings: [] };
      },
    };
    const double = createTool({ id: 'double', description: 'Double a number',
      inputSchema: z.object({ value: z.coerce.number(), label: z.string().default('defaulted') }),
      execute: async () => { liveExecutions++; throw new Error('Live tool must never execute during history replay'); } });
    const agent = new Agent({ id: 'fixture-agent', name: 'Fixture agent',
      instructions: liveInstructions, model, tools: { double } });
    const generate = agent.generate.bind(agent);
    agent.generate = (messages, options) => {
      for (const key of ['memory', 'threadId', 'resourceId', 'savePerStep']) assert.equal(options[key], undefined);
      const context = sessions[turn].inputs.mastra_conversation_context;
      assert.equal(context.complete, true);
      assert.deepEqual(messages, context.messages, 'Restore every message in the recorded snapshot');
      for (const key of ['instructions', 'system', 'context']) assert.deepEqual(options[key], []);
      return generate(messages, options);
    };
    const wrapped = new KitaruAgent(agent, { agentId: sessionId, apiKey: 'fixture-only',
      apiUrl: 'https://fixture.invalid', requestedModelId: 'fixture-model' });
    const result = await wrapped.generate('CALLER INPUT MUST BE REPLACED', {
      memory: { thread: 'live-thread', resource: 'live-user' }, threadId: 'live-thread',
      resourceId: 'live-user', savePerStep: true, modelSettings: { maxRetries: 0 },
    });
    assert.equal(result.text, sessions[turn].outputs.text);
  }
  assert.equal(liveExecutions, 0);
  assert.equal(lookups, 1);
  const toolNodes = apiCalls.filter((call) => call.path.endsWith('/nodes'))
    .flatMap((call) => call.body.nodes).filter((node) => node.node_type === 'tool_call');
  assert.ok(toolNodes.some((node) => node.attributes.mocked === true && node.attributes.policy === 'history'));
  console.log(JSON.stringify({ replayedSessions: sessions.length, historyLookups: lookups,
    liveToolExecutions: liveExecutions, priorHistoryVerified: true, memoryOptionsRemoved: true,
    fullSnapshotRestored: true, recordedSystemRestored: true,
    limitation: 'Local context-capable adapter integration probe with a stubbed Kitaru API, not a live worker/server.' }));
} finally {
  globalThis.fetch = originalFetch;
  for (const name of Object.keys(process.env)) if (!(name in savedEnvironment)) delete process.env[name];
  Object.assign(process.env, savedEnvironment);
}
