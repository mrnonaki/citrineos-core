// SPDX-License-Identifier: Apache-2.0
// ChargeMai wallet fork — alternate entrypoint. Deploy with:
//   command: ["node", "dist/wallet/main.js"]  (k8s manifest; stock image otherwise)
// Mirrors the stock entry apps/ocpp-server/src/index.ts; keep this main() in sync
// with it on rebases (rebase checklist: `git diff <old>..<new> -- apps/ocpp-server/src/index.ts`).
// Wallet wiring uses the CitrineOSServer extension seams (packages/ocpp/src/server/
// DEPENDENCY_INJECTION.md): registerAdditionalServices() to register/override
// container tokens, onInitialized() to start the AMQP gates/consumers.

import { ConfigLoader } from '@citrineos/base';
import { CitrineOSServer } from '@citrineos/ocpp';
import { EventGroup } from '@citrineos/types';
import type { AwilixContainer } from 'awilix';
import { RemoteStartConsumer, RemoteStopConsumer } from './consumers.js';
import { GatesFrameSource, PreparingGate, SuspendedEvGate } from './gates.js';
import { assertWalletOverrides, registerWalletServices } from './registerWalletServices.js';

const on = (flag: string) => process.env[flag] === 'true';

class WalletServer extends CitrineOSServer {
  private _walletStoppables: Array<{ stop: () => void | Promise<void> }> = [];

  // Runs inside initContainer(), after buildContainer() and before any token is
  // resolved — the documented seam for adding/overriding registrations. awilix
  // register is last-write-wins, so our authorizationRepository/authorizers win.
  protected registerAdditionalServices(container: AwilixContainer): void {
    registerWalletServices(container);
  }

  // Runs after the container, message broker, modules and DB are all wired, before
  // the server starts listening — the right point to start the AMQP gates/consumers.
  protected async onInitialized(): Promise<void> {
    await super.onInitialized();
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

    // Gates are triggered from the upstream `messages` frame exchange (shared
    // durable queue) — one consumer feeds both, each gate filters its actions.
    // Push the frame source FIRST so shutdown cancels the consumer before the gates
    // tear down their timers (no frame can arm a timer we just cleared).
    const preparing = on('RABBITMQ_PREPARING') ? new PreparingGate(deps) : undefined;
    const suspended = on('RABBITMQ_SUSPENDED') ? new SuspendedEvGate(deps) : undefined;
    if (preparing || suspended) {
      const frames = new GatesFrameSource(deps, { preparing, suspended });
      await frames.start();
      this._walletStoppables.push(frames);
    }
    if (preparing) this._walletStoppables.push(preparing);
    if (suspended) this._walletStoppables.push(suspended);
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
  const config = await ConfigLoader.loadConfig();
  const server = new WalletServer(process.env.APP_NAME?.toLowerCase() as EventGroup, config);
  server.run().catch((error: any) => {
    console.error(error);
    process.exit(1);
  });
}

main().catch((error) => {
  console.error('Failed to initialize wallet server:', error);
  process.exit(1);
});
