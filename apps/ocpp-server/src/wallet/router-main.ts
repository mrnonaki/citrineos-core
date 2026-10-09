// SPDX-License-Identifier: Apache-2.0
// ChargeMai fork — lean entrypoint for the OCPP ROUTER deployment. Deploy with:
//   command: ["node", "dist/wallet/router-main.js"]
//
// Stock CitrineOSServer plus the multi-replica router guard. Upstream's router already
// runs one queue per router instance (RabbitMqRouterReceiver, #1162); this entrypoint
// only refuses to boot without a unique instanceIdentifier and asserts that receiver is
// really in place (see routerGuard.ts). It must NOT load the wallet auth
// layer (WalletAuthorizationRepository override, RPC authorizer, gates/consumers) —
// that belongs only on the module server (dist/wallet/main.js).
// Keep main() in sync with the stock apps/ocpp-server/src/index.ts on rebases.

import { ConfigLoader } from '@citrineos/base';
import { CitrineOSServer } from '@citrineos/ocpp';
import { EventGroup } from '@citrineos/types';
import type { AwilixContainer } from 'awilix';
import { assertRouterInstanceIdentifier, assertRouterReceiver } from './routerGuard.js';

class RouterServer extends CitrineOSServer {
  // Before any token is resolved: fail before the router receiver picks a queue name.
  protected registerAdditionalServices(container: AwilixContainer): void {
    assertRouterInstanceIdentifier(container);
  }

  // Everything is wired (the message router has resolved its routerHandler).
  protected async onInitialized(): Promise<void> {
    await super.onInitialized();
    assertRouterReceiver(this._container);
    this._logger?.info(
      'wallet: router guard OK — RabbitMqRouterReceiver on instance queue ' +
        `rabbit_queue_router_${this._config.messageBroker.amqp?.instanceIdentifier}`,
    );
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
