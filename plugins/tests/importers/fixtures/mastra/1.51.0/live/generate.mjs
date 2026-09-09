import assert from 'node:assert/strict';
import {writeFile} from 'node:fs/promises';
import {Agent} from '@mastra/core/agent';
import {Mastra} from '@mastra/core/mastra';
import {MastraMemory} from '@mastra/core/memory';
import {InMemoryStore} from '@mastra/core/storage';
import {createTool} from '@mastra/core/tools';
import {Observability,MastraStorageExporter} from '@mastra/observability';
import {z} from 'zod';
class HistoryMemory extends MastraMemory {
 constructor(){const storage=new InMemoryStore();super({name:'history-test',storage,options:{lastMessages:10}});this.domain=storage.stores.memory;}
 getThreadById=(a)=>this.domain.getThreadById(a);
 listThreads=(a)=>this.domain.listThreads(a);
 saveThread=(a)=>this.domain.saveThread(a);
 saveMessages=(a)=>this.domain.saveMessages(a);
 recall=(a)=>this.domain.listMessages(a);
 updateThread=(a)=>this.domain.updateThread(a);
 deleteThread=(threadId)=>this.domain.deleteThread({threadId});
 cloneThread=(a)=>this.domain.cloneThread(a);
 async deleteMessages(){throw new Error('Unused');}
 async getWorkingMemory(){return null;}
 async getWorkingMemoryTemplate(){return null;}
 async updateWorkingMemory(){throw new Error('Unused');}
 async __experimental_updateWorkingMemoryVNext(){throw new Error('Unused');}
}
assert.ok(process.env.OPENAI_API_KEY,'OPENAI_API_KEY unavailable');
const memory=new HistoryMemory();
const threadId='live-history-fixture';const resourceId='synthetic-user';
await memory.createThread({threadId,resourceId});
await memory.saveMessages({messages:[{id:'prior-message',role:'user',content:{format:2,parts:[{type:'text',text:'My secret code is ORBIT-47. Remember it for my next question.'}]},createdAt:new Date('2026-09-09T10:00:00Z'),threadId,resourceId}]});
const storage=new InMemoryStore();const exporter=new MastraStorageExporter();
const observability=new Observability({configs:{default:{serviceName:'kitaru-mastra-live-validation',exporters:[exporter]}}});
const executed=[];let providerCalls=0;const requests=[];
const originalFetch=globalThis.fetch;
globalThis.fetch=async(input,options)=>{
 const url=new URL(typeof input==='string'?input:input.url??String(input));
 if(url.hostname==='api.openai.com'){
  providerCalls++;assert.ok(providerCalls<=3,'Provider call budget exceeded');
  const body=options?.body?JSON.parse(String(options.body)):undefined;
  requests.push({path:url.pathname,model:body?.model,messages:body?.messages,input:body?.input,max_tokens:body?.max_tokens,max_output_tokens:body?.max_output_tokens});
 }
 return originalFetch(input,options);
};
const double=createTool({id:'double',description:'Double a supplied number. Call this exactly once to calculate the requested double.',inputSchema:z.object({value:z.number()}),execute:async(input)=>{executed.push(input);return {doubled:input.value*2};}});
const instructions='Use the prior conversation to recall the secret code. Call the double tool exactly once for the requested number. After the tool result, answer in one short sentence containing both the secret code and computed result. Never call the tool twice.';
const agent=new Agent({id:'live-fixture-agent',name:'Live fixture agent',instructions,model:'openai/gpt-4.1-mini',memory,tools:{double}});
const mastra=new Mastra({logger:false,storage,observability,agents:{agent}});
const traceId='10540000000000000000000000000001';
try {
 const result=await mastra.getAgent('agent').generate([{role:'user',content:'What is my secret code, and what is double 3? Use the tool.'}],{memory:{thread:threadId,resource:resourceId},tracingOptions:{traceId},maxSteps:2,modelSettings:{maxRetries:0,maxOutputTokens:150,temperature:0}});
 await exporter.flush();
 const trace=await (await storage.getStore('observability')).getTrace({traceId});
 await writeFile(new URL('./trace.json',import.meta.url),JSON.stringify(trace,null,2)+'\n');
 const evidence={model:'openai/gpt-4.1-mini',providerCalls,executed,output:result.text,requests,configuration:{memory:'Native Mastra history only; lastMessages10; no semantic/working/observational memory, prepareStep, or custom processors',instructions},versions:{core:'1.51.0',observability:'1.16.0',zod:'4.3.6'},command:'node generate.mjs'};
 await writeFile(new URL('./evidence.json',import.meta.url),JSON.stringify(evidence,null,2)+'\n');
 assert.ok(result.text.includes('ORBIT-47'));assert.match(result.text,/\b6\b/);assert.equal(executed.length,1);
 const root=trace.spans.find(s=>s.spanType==='agent_run');const generation=trace.spans.find(s=>s.spanType==='model_generation');
 assert.ok(!JSON.stringify(root.input).includes('ORBIT-47'));assert.ok(JSON.stringify(generation.input).includes('ORBIT-47'));
 console.log(JSON.stringify({output:result.text,providerCalls,toolExecutions:executed.length,spans:trace.spans.length,recalledHistoryVerified:true}));
} finally {await observability.shutdown();globalThis.fetch=originalFetch;}
