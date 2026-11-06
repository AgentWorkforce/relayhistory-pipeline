// Vite `?raw` imports: migration SQL inlined as a string at transform time so the
// workerd tests can check RPC argument names against the real signatures.
declare module "*.sql?raw" {
  const text: string;
  export default text;
}
