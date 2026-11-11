import { afterEach, expect, test } from 'vitest';
import { mkdtemp, rm } from 'node:fs/promises';
import { join } from 'node:path';
import { tmpdir } from 'node:os';
import { zstdDecompressSync } from 'node:zlib';
import { createFsStore } from '../../object-store/src/fs.js';
import { readAll, type ObjectStore } from '../src/lib/pipeline/object-layout.js';
import type { SealRawChunk } from '../src/lib/pipeline/raw-chunks.js';
import { hasRecordContent, isRustWhitespaceOnly } from '../src/lib/pipeline/engine-raw-chunks.js';
import { resetScrubEngineForTests } from '../src/lib/scrub-engine/index.js';
import { fixtureSealer } from './support/engine-sealer.js';

/** Rust `char::is_whitespace` (Unicode White_Space), which `str::trim()` uses. */
const RUST_WHITE_SPACE = [
  0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x20, 0x85, 0xa0, 0x1680,
  0x2000, 0x2001, 0x2002, 0x2003, 0x2004, 0x2005, 0x2006, 0x2007, 0x2008, 0x2009, 0x200a,
  0x2028, 0x2029, 0x202f, 0x205f, 0x3000,
];
/** Explicit non-members. JS `\s` includes U+FEFF; the rest are common confusions. */
const NOT_WHITE_SPACE = [0xfeff, 0x200b, 0x180e, 0x0e, 0x00, 0x1f, 0x7f, 0x200c, 0x200d, 0x2060, 0x21];
const NEL = '\u0085', BOM = '﻿';
const BOUNDARY = 65_536;

const enc = new TextEncoder();
const dirs: string[] = [];
afterEach(async () => { resetScrubEngineForTests(); await Promise.all(dirs.splice(0).map(d => rm(d, { recursive: true, force: true }))); });
async function store(): Promise<ObjectStore> {
  const root = await mkdtemp(join(tmpdir(), 'engine-seal-blank-')); dirs.push(root);
  const inner = createFsStore({ root });
  return { backend: inner.backend, head: inner.head.bind(inner), get: inner.get.bind(inner), getObject: inner.getObject.bind(inner), list: inner.list.bind(inner), presignGet: inner.presignGet.bind(inner), put: inner.put.bind(inner), delete: inner.delete.bind(inner) } satisfies ObjectStore;
}
function input(source: Uint8Array): SealRawChunk {
  return { location: { prefix: '96bulls/o', workspaceId: 'w', machineId: 'm', harness: 'h', sessionId: 's', startDate: '2026-11-10' }, format: 'ndjson', seq: 0, epoch: 0, startOffset: 0, source };
}
/** An engine double that echoes its exact input bytes (no BOM stripping) as one record. */
function echoSealer() {
  const f = fixtureSealer();
  f.engine.scrubRecords = source => { f.state.calls++; f.state.seen.push(new Uint8Array(source)); return { scrubbed: new Uint8Array(source), records: 1, scrub: f.engine.marker }; };
  return f;
}
function concat(...parts: Uint8Array[]): Uint8Array {
  const out = new Uint8Array(parts.reduce((n, p) => n + p.byteLength, 0)); let at = 0;
  for (const p of parts) { out.set(p, at); at += p.byteLength; }
  return out;
}
/** ASCII-only test names: escape every non-printable-ASCII code unit. */
const show = (t: string) => t.replace(/[^\x21-\x7e]/g, c => `U+${c.charCodeAt(0).toString(16).padStart(4, '0')}`);
const spaces = (n: number) => new Uint8Array(n).fill(0x20);

test('whitespace set is exactly Unicode White_Space (25 code points)', () => {
  expect(new Set(RUST_WHITE_SPACE).size).toBe(25);
  for (const cp of RUST_WHITE_SPACE) {
    const ch = String.fromCodePoint(cp);
    expect(isRustWhitespaceOnly(ch), `U+${cp.toString(16)}`).toBe(true);
    expect(isRustWhitespaceOnly(ch.repeat(3)), `U+${cp.toString(16)} x3`).toBe(true);
    expect(isRustWhitespaceOnly(` ${ch}\n`), `U+${cp.toString(16)} mixed`).toBe(true);
    expect(hasRecordContent(enc.encode(ch)), `bytes U+${cp.toString(16)}`).toBe(false);
  }
  expect(isRustWhitespaceOnly(RUST_WHITE_SPACE.map(cp => String.fromCodePoint(cp)).join(''))).toBe(true);
  expect(isRustWhitespaceOnly('')).toBe(true);
});

test('explicit non-members are content (U+FEFF, U+200B, U+180E, U+000E, ...)', () => {
  for (const cp of NOT_WHITE_SPACE) {
    if (RUST_WHITE_SPACE.includes(cp)) continue;
    const ch = String.fromCodePoint(cp);
    expect(isRustWhitespaceOnly(ch), `U+${cp.toString(16)}`).toBe(false);
    expect(isRustWhitespaceOnly(` ${ch}\n`), `U+${cp.toString(16)} mixed`).toBe(false);
    expect(hasRecordContent(enc.encode(ch)), `bytes U+${cp.toString(16)}`).toBe(true);
  }
});

test('every BMP and astral code point agrees with the explicit table', () => {
  const members = new Set(RUST_WHITE_SPACE);
  for (let cp = 0; cp <= 0x10ffff; cp++) {
    if (cp >= 0xd800 && cp <= 0xdfff) continue;
    if (isRustWhitespaceOnly(String.fromCodePoint(cp)) !== members.has(cp)) throw new Error(`mismatch at U+${cp.toString(16)}`);
  }
});

test('JS \\s disagrees with Rust on exactly U+0085 and U+FEFF (regression guard)', () => {
  expect(/\s/u.test(NEL)).toBe(false);
  expect(isRustWhitespaceOnly(NEL)).toBe(true);
  expect(/\s/u.test(BOM)).toBe(true);
  expect(isRustWhitespaceOnly(BOM)).toBe(false);
});

for (const text of [NEL, NEL.repeat(1000), ` ${NEL}\n\r${NEL}\t`, `${NEL}\n${NEL}\n`, `　${NEL} `])
  test(`NEL-only stream is blank: chunk_empty refusal, engine not poisoned (${show(text.slice(0, 8))} len=${text.length})`, async () => {
    const s = await store(), f = echoSealer();
    await expect(f.sealer(s, input(enc.encode(text)))).rejects.toMatchObject({ code: 'chunk_empty', retryable: false });
    expect(f.state.calls).toBe(0); expect(f.state.failed).toBe(false); expect(f.engine.failed).toBe(false);
    expect((await s.list('96bulls' as never)).items).toHaveLength(0);
    await expect(f.sealer(s, input(enc.encode('{"text":"after"}\n')))).resolves.toMatchObject({ created: true });
    expect(f.state.calls).toBe(1);
  });

for (const text of [BOM, BOM.repeat(3), `${BOM}\n`, ` ${BOM} `, `\n${NEL}${BOM}`])
  test(`U+FEFF is content: reaches the engine and is sealed (${show(text)})`, async () => {
    const s = await store(), f = echoSealer(), bytes = enc.encode(text);
    const out = await f.sealer(s, input(bytes));
    expect(out).toMatchObject({ created: true, entry: { records: 1 } });
    expect(f.state.calls).toBe(1); expect(f.state.seen[0]).toEqual(bytes); expect(f.engine.failed).toBe(false);
    const o = await s.getObject(out.key as never);
    expect(new Uint8Array(zstdDecompressSync(await readAll(o.body)))).toEqual(bytes);
  });

test('NEL split across the 65,536-byte boundary (C2 | 85) is blank', async () => {
  const bytes = concat(spaces(BOUNDARY - 1), enc.encode(NEL), enc.encode('\n'));
  expect(bytes[BOUNDARY - 1]).toBe(0xc2); expect(bytes[BOUNDARY]).toBe(0x85);
  expect(hasRecordContent(bytes)).toBe(false);
  const s = await store(), f = echoSealer();
  await expect(f.sealer(s, input(bytes))).rejects.toMatchObject({ code: 'chunk_empty', retryable: false });
  expect(f.state.calls).toBe(0); expect(f.engine.failed).toBe(false);
  await expect(f.sealer(s, input(enc.encode('{"a":1}\n')))).resolves.toMatchObject({ created: true });
});

test('NEL ending exactly at the boundary, and NEL-only longer than one piece, stay blank', () => {
  expect(hasRecordContent(concat(spaces(BOUNDARY - 2), enc.encode(NEL)))).toBe(false);
  expect(hasRecordContent(enc.encode(NEL.repeat(BOUNDARY)))).toBe(false); // 128 KiB
  expect(hasRecordContent(concat(spaces(1), enc.encode(NEL.repeat(BOUNDARY))))).toBe(false); // every NEL split-aligned
});

for (const offset of [BOUNDARY - 2, BOUNDARY - 1])
  test(`U+FEFF split across the boundary at offset ${offset} (EF BB BF) is content and sealed`, async () => {
    const bytes = concat(spaces(offset), enc.encode(BOM), enc.encode('\n'));
    expect(bytes.subarray(offset, offset + 3)).toEqual(new Uint8Array([0xef, 0xbb, 0xbf]));
    expect(hasRecordContent(bytes)).toBe(true);
    const s = await store(), f = echoSealer();
    await expect(f.sealer(s, input(bytes))).resolves.toMatchObject({ created: true, entry: { records: 1 } });
    expect(f.state.calls).toBe(1); expect(f.engine.failed).toBe(false);
  });

test('leading BOM is preserved as content by the streaming decoder (ignoreBOM)', () => {
  expect(hasRecordContent(new Uint8Array([0xef, 0xbb, 0xbf]))).toBe(true);
  expect(hasRecordContent(new Uint8Array([0xef, 0xbb, 0xbf, 0x0a, 0x20]))).toBe(true);
});

for (const [name, bytes] of [
  ['content after 64 KiB of NEL', concat(enc.encode(NEL.repeat(BOUNDARY / 2)), enc.encode('x'))],
  ['content after 64 KiB of NEL, NEL split', concat(spaces(1), enc.encode(NEL.repeat(BOUNDARY / 2)), enc.encode('{"a":1}\n'))],
  ['content after many pieces of NEL', concat(enc.encode(NEL.repeat(BOUNDARY * 2)), enc.encode('y'))],
] as const)
  test(`${name} reaches the engine`, async () => {
    expect(hasRecordContent(bytes)).toBe(true);
    const s = await store(), f = echoSealer();
    await expect(f.sealer(s, input(bytes))).resolves.toMatchObject({ created: true });
    expect(f.state.calls).toBe(1);
  });

for (const [name, bytes] of [
  ['lone high surrogate (ED A0 80)', new Uint8Array([0x20, 0xed, 0xa0, 0x80, 0x0a])],
  ['truncated NEL at the end of a piece', concat(spaces(BOUNDARY - 1), new Uint8Array([0xc2]))],
  ['truncated BOM across the boundary', concat(spaces(BOUNDARY - 1), new Uint8Array([0xef, 0xbb]))],
  ['NEL as a bare C1 byte (Latin-1)', new Uint8Array([0x85])],
] as const)
  test(`invalid UTF-8 still refuses at preflight before the engine (${name})`, async () => {
    const s = await store(), f = echoSealer();
    await expect(f.sealer(s, input(bytes))).rejects.toMatchObject({ code: 'chunk_not_utf8', retryable: false });
    expect(f.state.calls).toBe(0); expect(f.engine.failed).toBe(false);
    expect((await s.list('96bulls' as never)).items).toHaveLength(0);
  });

test('hasRecordContent refuses invalid UTF-8 as retryable admission unavailability, never as blank', () => {
  expect(() => hasRecordContent(new Uint8Array([0xff]))).toThrow(expect.objectContaining({ code: 'scrub_admission_unavailable', retryable: true }));
});
