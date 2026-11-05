import {expect,test} from "bun:test";import {semanticCopyMain} from "../semantic-copy-main.ts";
test("disabled standalone Node job never constructs clients or reads credentials",async()=>{let calls=0;const r=await semanticCopyMain({},()=>{calls++;throw Error("factory must not run")});expect(r).toEqual({enabled:false});expect(calls).toBe(0)});
