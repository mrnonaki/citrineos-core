// SPDX-License-Identifier: Apache-2.0
// ChargeMai wallet fork — alternate entrypoint. Deploy with:
//   command: ["node", "dist/wallet/main.js"]  (k8s manifest; stock image otherwise)
// Upstream index.ts stays untouched; keep this main() in sync with it on rebases
// (rebase checklist: `git diff <old>..<new> -- apps/ocpp-server/src/index.ts`).

import { loadBootstrapConfig } from '@citrineos/base';
import { EventGroup } from '@citrineos/types';
import { CitrineOSServer } from '../citrineOSServer.js';
import { getSystemConfig } from '../config/index.js';
import { RemoteStartConsumer, RemoteStopConsumer } from './consumers.js';
import { PreparingGate, SuspendedEvGate } from './gates.js';
import { assertWalletOverrides, registerWalletServices } from './registerWalletServices.js';

const on = (flag: string) => process.env[flag] === 'true';

class WalletServer extends CitrineOSServer {
  private _walletStoppables: Array<{ stop: () => void | Promise<void> }> = [];

  async initialize(): Promise<void> {
    // Before super.initialize(): container is built (ctor) but no token has been
    // resolved yet — equivalent timing to registering inside buildContainer.
    registerWalletServices(this._container);
    await super.initialize();
    assertWalletOverrides(this._container);

    // Gates + consumers are constructed with plain `new` (not via awilix) so a
    // scoped ocppSender never leaks into a singleton registration (strict mode).
    const scope = this._container.createScope();
    const deps = {
      channelManager: this._container.resolve('channelManager') as any,
      logger: this._container.resolve('logger') as any,
      cache: this._container.resolve('cache') as any,
      locationRepository: this._container.resolve('locationRepository') as any,
      transactionEventRepository: this._container.resolve('transactionEventRepository') as any,
      authorizationRepository: this._container.resolve('authorizationRepository') as any,
      ocppSender: scope.resolve('ocppSender') as any,
    };

    if (on('RABBITMQ_PREPARING')) {
      const gate = new PreparingGate(deps);
      gate.start();
      this._walletStoppables.push(gate);
    }
    if (on('RABBITMQ_SUSPENDED')) {
      const gate = new SuspendedEvGate(deps);
      gate.start();
      this._walletStoppables.push(gate);
    }
    if (on('RABBITMQ_REMOTESTART')) {
      const consumer = new RemoteStartConsumer(deps);
      await consumer.start();
      this._walletStoppables.push(consumer);
    }
    if (on('RABBITMQ_REMOTESTOP')) {
      const consumer = new RemoteStopConsumer(deps);
      await consumer.start();
      this._walletStoppables.push(consumer);
    }
  }

  async shutdown(): Promise<void> {
    for (const s of this._walletStoppables.splice(0)) {
      try {
        await s.stop();
      } catch {
        /* best-effort teardown */
      }
    }
    await super.shutdown();
  }
}

async function main() {
  const bootstrapConfig = loadBootstrapConfig();
  const config = await getSystemConfig(bootstrapConfig);
  const server = new WalletServer(
    process.env.APP_NAME?.toLowerCase() as EventGroup,
    bootstrapConfig,
    config,
  );
  server.run().catch((error: any) => {
    console.error(error);
    process.exit(1);
  });
}

main().catch((error) => {
  console.error('Failed to initialize wallet server:', error);
  process.exit(1);
});
