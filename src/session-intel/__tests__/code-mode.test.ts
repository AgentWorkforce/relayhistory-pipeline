/** Synthetic code strings are parsed as evidence, never executed. */
import { expect, test } from "bun:test";
import { CODE_MODE_BOUNDS, codeModeShells, codexCommitSha } from "../import/code-mode.ts";

test("literal commands, immutable string aliases and inline argv keep source order", () => {
  const input = `const command = 'git ' + 'commit -m fixture';
    text(await tools.exec_command({cmd:command}));
    await tools.shell_command({command:\`git status\`});
    await tools.shell({command:['bash','-lc','git commit -m another']});`;
  expect(codeModeShells(input)).toEqual({ commands: ["git commit -m fixture", "git status", ["bash", "-lc", "git commit -m another"]], incomplete: false });
});

test("quoted examples and comments are not calls, and JavaScript is never evaluated", () => {
  const key = "__codeModeImportMustNotRun";
  expect(codeModeShells(`// tools.exec_command({cmd:'git commit'})
    /* tools.exec_command({cmd:'git commit'}) */
    const quoted = "tools.exec_command({cmd:'git commit'})";
    globalThis.${key}=true;`)).toEqual({ commands: [], incomplete: false });
  expect((globalThis as Record<string, unknown>)[key]).toBeUndefined();
});

for (const [name, input] of [
  ["dynamic input", "tools.exec_command({cmd: process.env.COMMAND})"],
  ["mutable alias", "let cmd='git commit';cmd='git status';tools.exec_command({cmd})"],
  ["shadowed alias", "const cmd='git commit';function f(cmd){};tools.exec_command({cmd})"],
  ["shadowed tools", "const tools = factory();tools.exec_command({cmd:'git commit'})"],
  ["imported tools", "import tools from 'fixture';tools.exec_command({cmd:'git commit'})"],
  ["rewritten tool", "tools.exec_command = other;tools.exec_command({cmd:'git commit'})"],
  ["spread override", "tools.exec_command({cmd:'git commit',...other})"],
  ["duplicate property", "tools.exec_command({cmd:'git commit',cmd:'git status'})"],
  ["interpolation", "tools.exec_command({cmd:`git commit -m ${message}`})"],
  ["uncalled function", "function f(){tools.exec_command({cmd:'git commit'})}"],
  ["conditional call", "if(false)tools.exec_command({cmd:'git commit'})"],
  ["short circuit", "false && tools.exec_command({cmd:'git commit'})"],
  ["logical or assignment", "const x=true; x ||= tools.exec_command({cmd:'git commit'})"],
  ["nullish assignment", "let x=1; x ??= tools.exec_command({cmd:'git commit'})"],
  ["logical and assignment", "let x=false; x &&= tools.exec_command({cmd:'git commit'})"],
  ["optional argument call", "x?.(tools.exec_command({cmd:'git commit'}))"],
  ["optional index", "o?.[tools.exec_command({cmd:'git commit'})]"],
  ["optional tool call", "tools.exec_command?.({cmd:'git commit'})"],
  ["optional tool namespace", "tools?.exec_command({cmd:'git commit'})"],
  ["after throw", "throw new Error('fixture'); tools.exec_command({cmd:'git commit'})"],
  ["after nested throw", "{ { throw new Error('fixture'); } } tools.exec_command({cmd:'git commit'})"],
  ["after possible throw", "if (guard) throw new Error('fixture'); tools.exec_command({cmd:'git commit'})"],
  ["after return", "return; tools.exec_command({cmd:'git commit'})"],
  ["after labelled break", "fixture: { break fixture; tools.exec_command({cmd:'git commit'}) }"],
  ["try body", "try { tools.exec_command({cmd:'git commit'}) } catch {}"],
  ["const used before declaration", "tools.exec_command({cmd}); const cmd='git commit'"],
  ["object binding default", "const {a = tools.exec_command({cmd:'git commit -m x'})} = {a:1}"],
  ["array binding default", "const [a = tools.exec_command({cmd:'git commit -m x'})] = [1]"],
  ["array assignment default", "let a; [a = tools.exec_command({cmd:'git commit -m x'})] = [1]"],
  ["object shorthand assignment default", "let a; ({a = tools.exec_command({cmd:'git commit -m x'})} = {a:1})"],
  ["object property assignment default", "let a; ({b: a = tools.exec_command({cmd:'git commit -m x'})} = {b:1})"],
  ["nested assignment default", "let a; ({b: [a = tools.exec_command({cmd:'git commit -m x'})]} = {b:[1]})"],
  ["with binding", "with(o){tools.exec_command({cmd:'git commit -m x'})}"],
  ["malformed code", "tools.exec_command({cmd:'git commit'"],
] as const) test(`unsupported ${name} is counted as incomplete rather than guessed`, () => {
  expect(codeModeShells(input)).toEqual({ commands: [], incomplete: true });
});

test("source, node, command and nesting budgets fail closed", () => {
  expect(codeModeShells(" ".repeat(CODE_MODE_BOUNDS.input) + "tools.exec_command({cmd:'git commit'})").incomplete).toBe(true);
  expect(codeModeShells("const fixture = [" + "0,".repeat(CODE_MODE_BOUNDS.nodes) + "];tools.exec_command({cmd:'git commit'})")).toEqual({ commands: [], incomplete: true });
  expect(codeModeShells("tools.exec_command({cmd:'" + "x".repeat(CODE_MODE_BOUNDS.command + 1) + "'})")).toEqual({ commands: [], incomplete: true });
  expect(codeModeShells("tools.exec_command({cmd:'git status'});".repeat(CODE_MODE_BOUNDS.commands + 1))).toEqual({ commands: [], incomplete: true });
  expect(codeModeShells("(".repeat(20_000) + "tools.exec_command({cmd:'git commit'})" + ")".repeat(20_000)).incomplete).toBe(true);
});

const line = "[fixture abcdef12] synthetic commit\n 1 file changed";
test("SHA metadata is read from plain, terminal JSON, MCP text blocks and parallel wrappers", () => {
  for (const output of [line, { output: line }, { content: line },
    JSON.stringify({ output: [{ type: "text", text: JSON.stringify({ output: line, exit_code: 0 }) }] }),
    [{ type: "text", text: JSON.stringify({ output: line, exit_code: 0 }) }],
    [{ type: "input_text", text: JSON.stringify({ output: line, exit_code: 0 }) }],
    [{ type: "text", text: JSON.stringify({ i: 0, status: "fulfilled", value: { output: line } }) }],
    [{ type: "text", text: JSON.stringify({ i: 0, result: { output: line } }) }],
  ]) expect(codexCommitSha(output)).toBe("abcdef12");
});

test("ambiguous, rejected, unrelated and bounded-out output never invents a SHA", () => {
  for (const output of [line + "\n[fixture 12345678] another", [{ output: line }, { output: "[fixture 12345678] another" }],
    { status: "rejected", value: { output: line } }, { example: line }, { type: "image", text: line },
    "x".repeat(CODE_MODE_BOUNDS.output + 1), Array.from({ length: CODE_MODE_BOUNDS.outputNodes + 1 }, () => line),
  ]) expect(codexCommitSha(output)).toBeNull();
  let nested: unknown = line;
  for (let i = 0; i < 100_000; i++) nested = [nested];
  expect(codexCommitSha(nested)).toBeNull();
  expect(codexCommitSha([line, line])).toBe("abcdef12");
});

test("transfer barriers preserve earlier attempts and ignore transfers in uncalled functions", () => {
  expect(codeModeShells("tools.exec_command({cmd:'git commit'}); throw new Error('fixture'); tools.exec_command({cmd:'git commit -m unreachable'})"))
    .toEqual({ commands: ["git commit"], incomplete: true });
  expect(codeModeShells("function helper(){ return; } tools.exec_command({cmd:'git commit'})"))
    .toEqual({ commands: ["git commit"], incomplete: false });
  expect(codeModeShells("const base='git '; const cmd=base+'commit'; tools.exec_command({cmd})"))
    .toEqual({ commands: ["git commit"], incomplete: false });
});

test("parser and source failures are incomplete even before shell calls can be recognized", () => {
  expect(codeModeShells(" ".repeat(CODE_MODE_BOUNDS.input + 1))).toEqual({ commands: [], incomplete: true });
  expect(codeModeShells("// exec_command\nconst =")).toEqual({ commands: [], incomplete: true });
});

test("ordinary array/object expressions and scalar assignments are not destructuring defaults", () => {
  for (const source of [
    "let a; [a = tools.exec_command({cmd:'git commit'})]",
    "let a; ({b: a = tools.exec_command({cmd:'git commit'})})",
    "let a; a = tools.exec_command({cmd:'git commit'})",
    "let a,b; [a = tools.exec_command({cmd:'git commit'}), b = tools.exec_command({cmd:'git status'})]",
  ]) expect(codeModeShells(source)).toEqual({ commands: source.includes("git status") ? ["git commit", "git status"] : ["git commit"], incomplete: false });
});
