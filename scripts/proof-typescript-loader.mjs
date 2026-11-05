/** Test/proof-only resolver for native Node TypeScript. No network/install hook. */
import { registerHooks } from 'node:module';
import { existsSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
const pipeline = new URL('../packages/pipeline/src/', import.meta.url);
const objectStore = new URL('../packages/object-store/src/index.ts', import.meta.url);
registerHooks({ resolve(specifier,context,next) {
  if (specifier.startsWith('@rhc/')) return next(new URL(specifier.slice(5),pipeline).href,context);
  if (specifier === '@rhc-object-store') return next(objectStore.href,context);
  try { return next(specifier,context); } catch(error) {
    if (specifier.startsWith('.') && specifier.endsWith('.js') && context.parentURL) {
      const candidate = new URL(specifier.slice(0,-3)+'.ts',context.parentURL);
      if (existsSync(fileURLToPath(candidate))) return next(candidate.href,context);
    }
    throw error;
  }
}});
