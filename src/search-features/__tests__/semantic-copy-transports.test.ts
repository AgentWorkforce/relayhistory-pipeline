import {expect,test} from "bun:test";
import net from "node:net";import pg from "pg";
import {SemanticPostgresExecutor,scopeSemanticSql} from "../semantic-copy-postgres.ts";
import {scopedSemanticCh,createSemanticClickHouseClient} from "../semantic-copy-ch.ts";
import {createClickHouseClient} from "../../session-pipeline/vendor/search-index/clickhouse.ts";
test.each(["command","select","insert"] as const)("job abort cancels actual CH %s HTTP request and keeps parameters",async method=>{
 const controller=new AbortController();let start!:()=>void;const started=new Promise<void>(r=>start=r);let request:URL|undefined;
 const ch=createSemanticClickHouseClient({url:"http://127.0.0.1:8123",user:"synthetic",password:"synthetic",database:"test"},(async(url:any,init:any)=>{
 request=new URL(String(url));start();return new Promise((_resolve,reject)=>{const cancel=()=>reject(Object.assign(new Error("aborted"),{name:"AbortError"}));if(init.signal.aborted)cancel();else init.signal.addEventListener("abort",cancel,{once:true})});
 }) as typeof fetch),bound=ch.scoped({signal:controller.signal,deadline:Date.now()+5000});
 const pending=method==="insert"?bound.insert("fixture",[{x:1}]):method==="select"?bound.select("SELECT {x:String}",{params:{x:"kept"}}):bound.command("SELECT {x:String}",{params:{x:"kept"}});
 await started;controller.abort();await expect(pending).rejects.toMatchObject({name:"AbortError"});
 if(method!=="insert")expect(request!.searchParams.get("param_x")).toBe("kept");
});
test("metadata acquisition is bounded against a real silent TCP peer and dispatches no query",async()=>{
 let accepted=0,querySeen=false;const sockets=new Set<net.Socket>(),server=net.createServer(socket=>{accepted++;sockets.add(socket);socket.on("data",data=>{querySeen ||= data.includes(Buffer.from("SELECT 1"))});socket.on("close",()=>sockets.delete(socket))});
 await new Promise<void>(r=>server.listen(0,"127.0.0.1",r));const port=(server.address() as net.AddressInfo).port;
 const pool=new pg.Pool({host:"127.0.0.1",port,user:"synthetic",database:"synthetic",max:1,connectionTimeoutMillis:100});pool.on("error",()=>{});
 try{const start=performance.now(),db=new SemanticPostgresExecutor(pool);await expect(db.queryBounded("SELECT 1",undefined,{signal:AbortSignal.timeout(50),deadline:Date.now()+50})).rejects.toThrow();expect(performance.now()-start).toBeLessThan(1000);expect(accepted).toBe(1);expect(querySeen).toBe(false)}
 finally{await pool.end();for(const socket of sockets)socket.destroy();await new Promise<void>(r=>server.close(()=>r()))}
});
test("scoped heartbeat cancellation uses its own signal without aborting sibling metadata",async()=>{
 const parent=new AbortController(),beat=new AbortController(),signals:AbortSignal[]=[];
 const raw={query:async<T>()=>({rows:[] as T[]}),queryBounded:async<T>(_sql:string,_args:unknown[]|undefined,c:any)=>{signals.push(c.signal);return{rows:[] as T[]}}};
 const scope=scopeSemanticSql(raw,{signal:parent.signal,deadline:Date.now()+5000});
 await scope.query("main");await scope.queryWithSignal("heartbeat",undefined,beat.signal);beat.abort();expect(signals[0]!.aborted).toBe(false);expect(signals[1]!.aborted).toBe(true);parent.abort();expect(signals[0]!.aborted).toBe(true);
});


test("remaining budget bounds the actual CH insert five-minute default",async()=>{const controller=new AbortController();const ch=createClickHouseClient({url:"http://127.0.0.1:8123",user:"synthetic",password:"synthetic",database:"test"},(async(_url:any,init:any)=>new Promise((_resolve,reject)=>{const stop=()=>reject(new Error("deadline"));if(init.signal.aborted)stop();else init.signal.addEventListener("abort",stop,{once:true})})) as typeof fetch);const start=performance.now();await expect(scopedSemanticCh(ch,{signal:controller.signal,deadline:Date.now()+40}).insert("fixture",[{x:1}])).rejects.toThrow("deadline");expect(performance.now()-start).toBeLessThan(500)});
