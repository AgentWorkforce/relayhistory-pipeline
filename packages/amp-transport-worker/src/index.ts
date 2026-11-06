import { Container, getContainer } from "@cloudflare/containers";
import { gateway } from "./gateway.ts";
import { NATIVE_SERVER_PROGRAM } from "./native-server.ts";

interface Env {
  AMP_TRANSPORT_TOKEN?: string;
  AMP_TRANSPORT: DurableObjectNamespace<AmpTransport>;
}

export class AmpTransport extends Container<Env> {
  defaultPort = 8080;
  sleepAfter = "1h";
  entrypoint = ["bun", "-e", NATIVE_SERVER_PROGRAM];
  envVars: Record<string, string>;

  constructor(ctx: ConstructorParameters<typeof Container<Env>>[0], env: Env) {
    super(ctx, env);
    this.envVars = { AMP_TRANSPORT_TOKEN: env.AMP_TRANSPORT_TOKEN ?? "", PORT: "8080" };
  }
}

export default {
  fetch(request: Request, env: Env) {
    return gateway(request, env.AMP_TRANSPORT_TOKEN, bounded =>
      getContainer(env.AMP_TRANSPORT, "amp-transport").fetch(bounded));
  },
};
