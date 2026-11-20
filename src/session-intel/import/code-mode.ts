/** Bounded static code-mode evidence. Never evaluate JavaScript or run a command. */
import ts from "typescript";
import { commitShaFromOutput } from "./git.ts";
import { isRecord } from "../replay/util.ts";

export const CODE_MODE_BOUNDS = { input: 1024 * 1024, nodes: 20_000, commands: 32, command: 64 * 1024, output: 64 * 1024, outputNodes: 512, depth: 8 } as const;
const SHELL = new Set(["exec_command", "shell_command", "shell", "local_shell"]);
export interface StaticShells { commands: Array<string | string[]>; incomplete: boolean }

function method(node: ts.Node): string | null {
  if (!ts.isCallExpression(node) || !ts.isPropertyAccessExpression(node.expression) || node.expression.questionDotToken
    || !ts.isIdentifier(node.expression.expression) || node.expression.expression.text !== "tools") return null;
  return SHELL.has(node.expression.name.text) ? node.expression.name.text : null;
}
function hiddenFlow(n: ts.Node): boolean {
  return ts.isFunctionLike(n) || ts.isClassDeclaration(n) || ts.isClassExpression(n) || ts.isIfStatement(n)
    || ts.isSwitchStatement(n) || ts.isForStatement(n) || ts.isForOfStatement(n) || ts.isForInStatement(n)
    || ts.isWhileStatement(n) || ts.isDoStatement(n) || ts.isConditionalExpression(n)
    || (ts.isBinaryExpression(n) && [ts.SyntaxKind.AmpersandAmpersandToken, ts.SyntaxKind.BarBarToken, ts.SyntaxKind.QuestionQuestionToken].includes(n.operatorToken.kind));
}

/** Supported: unconditional tools.<shell>({cmd|command: literal, or immutable top-level const string}). */
export function codeModeShells(source: string): StaticShells {
  const none = (incomplete: boolean): StaticShells => ({ commands: [], incomplete });
  if (Buffer.byteLength(source) > CODE_MODE_BOUNDS.input) return none(true);
  if (!/\b(?:exec_command|shell_command|local_shell|shell)\b/u.test(source)) return none(false);
  try {
    const file = ts.createSourceFile("code-mode.js", source, ts.ScriptTarget.ESNext, true, ts.ScriptKind.JS);
    // Pinned TypeScript supplies parser diagnostics on SourceFile; fail closed if unavailable.
    const diagnostics = (file as ts.SourceFile & { parseDiagnostics?: readonly unknown[] }).parseDiagnostics;
    if (!diagnostics || diagnostics.length) return none(true);
    const bindings = new Map<string, number>(), constants = new Map<string, string>();
    let count = 0, knownCalls = 0, toolsWritten = false;
    const todo: ts.Node[] = [file];
    while (todo.length) {
      const n = todo.pop()!;
      if (++count > CODE_MODE_BOUNDS.nodes) return none(true);
      if (method(n)) knownCalls++;
      if ((ts.isVariableDeclaration(n) || ts.isParameter(n) || ts.isBindingElement(n) || ts.isFunctionDeclaration(n) || ts.isClassDeclaration(n)
        || ts.isImportSpecifier(n) || ts.isImportClause(n) || ts.isNamespaceImport(n) || ts.isImportEqualsDeclaration(n))
        && n.name && ts.isIdentifier(n.name)) bindings.set(n.name.text, (bindings.get(n.name.text) ?? 0) + 1);
      if (ts.isBinaryExpression(n) && n.operatorToken.kind >= ts.SyntaxKind.FirstAssignment && n.operatorToken.kind <= ts.SyntaxKind.LastAssignment) {
        let target = n.left;
        while (ts.isPropertyAccessExpression(target) || ts.isElementAccessExpression(target)) target = target.expression;
        if (ts.isIdentifier(target) && target.text === "tools") toolsWritten = true;
      }
      ts.forEachChild(n, (child) => { todo.push(child); });
    }
    if (bindings.has("tools") || toolsWritten || knownCalls > CODE_MODE_BOUNDS.commands) return none(knownCalls > 0);
    const literal = (node: ts.Expression, depth = 0): string | null => {
      if (depth > CODE_MODE_BOUNDS.depth) return null;
      if (ts.isStringLiteral(node) || ts.isNoSubstitutionTemplateLiteral(node)) return node.text.length <= CODE_MODE_BOUNDS.command ? node.text : null;
      if (ts.isIdentifier(node)) return constants.get(node.text) ?? null;
      if (ts.isParenthesizedExpression(node)) return literal(node.expression, depth + 1);
      if (ts.isBinaryExpression(node) && node.operatorToken.kind === ts.SyntaxKind.PlusToken) {
        const a = literal(node.left, depth + 1), b = literal(node.right, depth + 1);
        return a !== null && b !== null && a.length + b.length <= CODE_MODE_BOUNDS.command ? a + b : null;
      }
      return null;
    };
    for (const statement of file.statements) if (ts.isVariableStatement(statement) && (statement.declarationList.flags & ts.NodeFlags.Const)) {
      for (const d of statement.declarationList.declarations) if (ts.isIdentifier(d.name) && bindings.get(d.name.text) === 1 && d.initializer) {
        const value = literal(d.initializer); if (value !== null) constants.set(d.name.text, value);
      }
    }
    const commands: StaticShells["commands"] = [];
    let visitedCalls = 0, incomplete = false;
    todo.push(file);
    while (todo.length) {
      const n = todo.pop()!;
      if (hiddenFlow(n)) continue;
      if (method(n) && ts.isCallExpression(n)) {
        visitedCalls++;
        const arg = n.arguments[0];
        if (n.arguments.length !== 1 || !arg || !ts.isObjectLiteralExpression(arg)) { incomplete = true; continue; }
        let value: ts.Expression | null = null, unsafe = false;
        for (const p of arg.properties) {
          if (ts.isSpreadAssignment(p) || !p.name || ts.isComputedPropertyName(p.name)) { unsafe = true; break; }
          const name = ts.isIdentifier(p.name) || ts.isStringLiteral(p.name) ? p.name.text : null;
          if (name !== "cmd" && name !== "command") continue;
          if (value) { unsafe = true; break; }
          value = ts.isPropertyAssignment(p) ? p.initializer : ts.isShorthandPropertyAssignment(p) ? p.name : null;
          if (!value) { unsafe = true; break; }
        }
        if (unsafe || !value) { incomplete = true; continue; }
        const text = literal(value);
        if (text !== null) commands.push(text);
        else if (ts.isArrayLiteralExpression(value) && value.elements.length <= CODE_MODE_BOUNDS.commands) {
          const parts = value.elements.map((e) => ts.isSpreadElement(e) ? null : literal(e));
          if (parts.every((s): s is string => s !== null) && parts.join(" ").length <= CODE_MODE_BOUNDS.command) commands.push(parts);
          else incomplete = true;
        } else incomplete = true;
      }
      // Preserve source order despite the iterative stack.
      const children: ts.Node[] = []; ts.forEachChild(n, (child) => { children.push(child); }); todo.push(...children.reverse());
    }
    return { commands, incomplete: incomplete || visitedCalls !== knownCalls };
  } catch { return none(true); } // Parser stack/shape failures yield missing evidence, never execution or a content-bearing error.
}

/** A unique SHA in known text/output envelopes; rejected, ambiguous or censored evidence stays unknown. */
export function codexCommitSha(output: unknown): string | null {
  const todo: Array<{ value: unknown; depth: number }> = [{ value: output, depth: 0 }];
  const shas = new Set<string>(); let nodes = 0, bytes = 0;
  while (todo.length) {
    const { value, depth } = todo.pop()!;
    if (++nodes > CODE_MODE_BOUNDS.outputNodes || depth > CODE_MODE_BOUNDS.depth) return null;
    if (typeof value === "string") {
      bytes += Buffer.byteLength(value);
      if (bytes > CODE_MODE_BOUNDS.output) return null;
      try {
        const parsed: unknown = JSON.parse(value);
        if (typeof parsed === "object" && parsed !== null) { todo.push({ value: parsed, depth: depth + 1 }); continue; }
      } catch { /* Plain command output. */ }
      for (const line of value.split("\n")) { const sha = commitShaFromOutput(line); if (sha) shas.add(sha); }
      if (shas.size > 1) return null;
    } else if (Array.isArray(value)) {
      if (value.length > CODE_MODE_BOUNDS.outputNodes) return null;
      for (let i = value.length - 1; i >= 0; i--) todo.push({ value: value[i], depth: depth + 1 });
    } else if (isRecord(value)) {
      if (value.status === "rejected") continue;
      const nested = typeof value.output === "string" || Array.isArray(value.output) ? value.output
        : (value.type === "text" || value.type === "input_text") && typeof value.text === "string" ? value.text
        : value.content !== undefined ? value.content : value.status === "fulfilled" ? value.value
          : typeof value.i === "number" && value.result !== undefined ? value.result : undefined;
      if (nested !== undefined) todo.push({ value: nested, depth: depth + 1 });
    }
  }
  return shas.size === 1 ? [...shas][0]! : null;
}
