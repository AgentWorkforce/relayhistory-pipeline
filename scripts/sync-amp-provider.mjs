#!/usr/bin/env node
// Amp parsing belongs to RelayHistory. This checked-in copy is a build input,
// not a second implementation. Updates must name an upstream commit.
import { createHash } from 'node:crypto';
import { readFile, writeFile } from 'node:fs/promises';
import { fileURLToPath } from 'node:url';
import path from 'node:path';
const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const target = path.join(root, 'src/amp/provider.ts');
const manifestPath = path.join(root, 'src/amp/provider-origin.json');
const argument = name => { const i = process.argv.indexOf(name); return i < 0 ? undefined : process.argv[i + 1]; };
const sourcePath = argument('--source');
const digest = value => createHash('sha256').update(value).digest('hex');
const prefix = '// Generated from AgentWorkforce/relayhistory sdk-ts/src/amp-source.ts. Do not edit here.\n';
if (process.argv.includes('--check')) {
 const manifest = JSON.parse(await readFile(manifestPath, 'utf8'));
 const copy = await readFile(target, 'utf8');
 const body = copy.slice((prefix + `// sha256: ${manifest.sha256}\n`).length);
 if (!copy.startsWith(prefix + `// sha256: ${manifest.sha256}\n`) || digest(body) !== manifest.sha256) throw new Error('Amp provider copy differs from its pinned upstream hash. Regenerate it.');
 if (sourcePath && await readFile(sourcePath, 'utf8') !== body) throw new Error('Amp provider differs from the selected RelayHistory source.');
 console.log('Amp provider matches its pinned RelayHistory source.');
} else {
 const commit = argument('--commit');
 if (!sourcePath || !/^[a-f0-9]{40}$/.test(commit ?? '')) throw new Error('Use --source PATH --commit UPSTREAM_SHA, or --check [--source PATH].');
 const body = await readFile(sourcePath, 'utf8'); const sha256 = digest(body);
 await writeFile(target, prefix + `// sha256: ${sha256}\n` + body);
 await writeFile(path.join(root, 'src/amp/provider-LICENSE.txt'), await readFile(path.resolve(path.dirname(sourcePath), '../../LICENSE'), 'utf8'));
 await writeFile(manifestPath, JSON.stringify({ repository: 'https://github.com/AgentWorkforce/relayhistory', commit, path: 'sdk-ts/src/amp-source.ts', sha256 }, null, 2) + '\n');
}
