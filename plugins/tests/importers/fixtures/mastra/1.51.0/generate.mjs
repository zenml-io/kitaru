import {writeFile} from 'node:fs/promises';
import {Agent} from '@mastra/core/agent';
import {Mastra} from '@mastra/core/mastra';
import {InMemoryStore} from '@mastra/core/storage';
import {createTool} from '@mastra/core/tools';
import {Observability,MastraStorageExporter} from '@mastra/observability';
import {z} from 'zod';
const storage=new InMemoryStore();
const exporter=new MastraStorageExporter();
const observability=new Observability({configs:{default:{serviceName:'kitaru-mastra-fixture',exporters:[exporter]}}});
let call=0; const executed=[]; const prompts=[];
const model=({specificationVersion:'v2',supportedUrls:{},modelId:'fixture-model',provider:'fixture',doGenerate:async(options)=>{
 prompts.push(options.prompt);const step=call++;
 return {content: step===0 ? [{type:'tool-call',toolCallId:'call-double',toolName:'double',input:JSON.stringify({value:'3'})}] : [{type:'text',text:step===1?'The result is 6.':'The previous result was 6.'}],finishReason:step===0?'tool-calls':'stop',usage:{inputTokens:20+step,outputTokens:5+step,totalTokens:25+2*step},warnings:[]};
}});
const double=createTool({id:'double',description:'Double a number',inputSchema:z.object({value:z.coerce.number(),label:z.string().default('defaulted')}),execute:async(input)=>{executed.push(input);return {doubled:input.value*2,label:input.label};}});
const agent=new Agent({id:'fixture-agent',name:'Fixture agent',instructions:'Answer arithmetic questions using the double tool.',model,tools:{double}});
const mastra=new Mastra({logger:false,storage,observability,agents:{agent}});
const messages=[{role:'user',content:'Double 3.'}];
const traces=[];
for(let turn=0;turn<2;turn++){
 const traceId=(turn+1).toString(16).padStart(32,'0');
 const result=await mastra.getAgent('agent').generate(messages,{tracingOptions:{traceId},memory:{thread:'fixture-thread',resource:'fixture-user'},modelSettings:{maxRetries:0}});
 await exporter.flush();
 traces.push(await (await storage.getStore('observability')).getTrace({traceId}));
 if(turn===0) messages.push({role:'assistant',content:result.text},{role:'user',content:'What was the previous result?'});
}
await writeFile(new URL('./traces.json',import.meta.url),JSON.stringify(traces,null,2)+'\n');
for (let index=0; index<traces.length; index++) await writeFile(new URL(`./trace-${index+1}.json`,import.meta.url),JSON.stringify(traces[index],null,2)+'\n');
await writeFile(new URL('./evidence.json',import.meta.url),JSON.stringify({executed,prompts},null,2)+'\n');
await observability.shutdown();
console.log(JSON.stringify({traces:traces.map(t=>({traceId:t.traceId,spans:t.spans.length})),executed}));
