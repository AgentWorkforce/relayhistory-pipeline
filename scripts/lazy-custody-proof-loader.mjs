import {existsSync} from "node:fs";import {fileURLToPath,pathToFileURL} from "node:url";
const root=fileURLToPath(new URL("../",import.meta.url));
export async function resolve(specifier,context,nextResolve){
 if(specifier.startsWith("@rhc/"))specifier=pathToFileURL(root+"packages/pipeline/src/"+specifier.slice(5)).href;
 else if(specifier==="@rhc-object-store")specifier=pathToFileURL(root+"packages/object-store/src/index.ts").href;
 try{return await nextResolve(specifier,context)}
 catch(error){if((specifier.startsWith(".")||specifier.startsWith("file:"))&&specifier.endsWith(".js")){
  const url=new URL(specifier,context.parentURL).href.replace(/\.js$/,".ts");
  if(existsSync(fileURLToPath(url)))return nextResolve(url,context);
 }throw error}
}
