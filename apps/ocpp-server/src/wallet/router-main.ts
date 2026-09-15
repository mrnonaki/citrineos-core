// SPDX-License-Identifier: Apache-2.0
// ChargeMai fork — lean entrypoint for the OCPP ROUTER deployment. Deploy with:
//   command: ["node", "dist/wallet/router-main.js"]
//
// The router runs in RabbitMQ ROUTER MODE (one instance queue + per-charger bindings;
// upstream feat 610c7131c) so it scales past the per-charger-queue explosion of module
// mode. It must NOT load the wallet auth layer (WalletAuthorizationRepository override,
// RPC authorizer, gates/consumers) — that belongs only on the module server
// (dist/wallet/main.js). So this entrypoint is stock CitrineOSServer + routerMode only.
// Keep in sync with the stock apps/ocpp-server/src/index.ts on rebases.

import { ConfigLoader } from '@citrineos/base';
import { CitrineOSServer } from '@citrineos/ocpp';
import { EventGroup } from '@citrineos/types';
import type { AwilixContainer } from 'awilix';
import { registerRouterMode } from './registerWalletServices.js';

class RouterServer extends CitrineOSServer {
  protected registerAdditionalServices(container: AwilixContainer): void {
    registerRouterMode(container);
  }
}

async function main() {
  const config = await ConfigLoader.loadConfig();
  const server = new RouterServer(process.env.APP_NAME?.toLowerCase() as EventGroup, config);
  server.run().catch((error: any) => {
    console.error(error);
    process.exit(1);
  });
}

main().catch((error) => {
  console.error('Failed to initialize router server:', error);
  process.exit(1);
});
