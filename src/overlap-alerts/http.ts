import { assertDestinationUrl, destinationPolicy } from "../shared/destination-url.ts";
export function endpoint(name: string, value: string): URL {
  return assertDestinationUrl(name, value, destinationPolicy());
}
export async function boundedJson(response: Response, maxBytes: number): Promise<unknown> {
  if (!response.ok) { await response.body?.cancel(); throw new Error(`http_${response.status}`); }
  if (!response.body) throw new Error("empty_response");
  const reader = response.body.getReader();
  const chunks: Uint8Array[] = [];
  let total = 0;
  try {
    while (true) {
      const { value, done } = await reader.read();
      if (done) break;
      total += value.byteLength;
      if (total > maxBytes) throw new Error("response_too_large");
      chunks.push(value);
    }
    return JSON.parse(Buffer.concat(chunks).toString("utf8"));
  } catch { throw new Error("invalid_or_oversized_response"); }
  finally { await reader.cancel().catch(() => {}); }
}
