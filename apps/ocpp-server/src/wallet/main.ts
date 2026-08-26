// SPDX-License-Identifier: Apache-2.0
// ChargeMai wallet fork — alternate entrypoint. Deploy with:
//   command: ["node", "dist/wallet/main.js"]  (k8s manifest; stock image otherwise)
// Upstream index.ts stays untouched; keep this main() in sync with it on rebases
// (rebase checklist: `git diff <old>..<new> -- apps/ocpp-server/src/index.ts`).

import { loadBootstrapConfig } from '@citrineos/base';
import { EventGroup } from '@citrineos/types';
import { CitrineOSServer } from '../citrineOSServer.js';
import { getSystemConfig } from '../config/index.js';
import { assertWalletOverrides, registerWalletServices } from './registerWalletServices.js';

class WalletServer extends CitrineOSServer {
  async initialize(): Promise<void> {
    // Before super.initialize(): container is built (ctor) but no token has been
    // resolved yet — equivalent timing to registering inside buildContainer.
    registerWalletServices(this._container);
    await super.initialize();
    assertWalletOverrides(this._container);
    // TODO(phase 3): CRUD-event gates (preparing / suspendedEV listeners)
    // TODO(phase 3): remote start/stop AMQP consumers (safe here: initDb done)
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
