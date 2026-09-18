// SPDX-License-Identifier: Apache-2.0
// ChargeMai wallet fork — container overrides. Called from WalletServer BEFORE
// super.initialize(), i.e. after buildContainer() but before any token resolution,
// which is equivalent to registering at the end of buildContainer (awilix register
// is last-write-wins). Root container required (strict mode + singletons).

import { RabbitMqReceiver } from '@citrineos/ocpp';
import { asClass, asFunction, type AwilixContainer } from 'awilix';
import { WalletAuthorizationRepository } from './WalletAuthorizationRepository.js';
import { WalletRpcAuthorizer } from './WalletRpcAuthorizer.js';
import { WalletRpcClient } from './WalletRpcClient.js';

export function walletRpcEnabled(): boolean {
  // Same enable flag as the 1.9.1 fork so deploy manifests carry over.
  return process.env.RABBITMQ_AUTH === 'true';
}

export function registerWalletServices(container: AwilixContainer): void {
  // WalletAuthorizationRepository extends the SEQUELIZE repository. Upstream's
  // CITRINEOS_USE_DRIZZLE=true swaps the stock registration to
  // DrizzleAuthorizationRepository — our override would still win the token but
  // silently lose the Drizzle implementation underneath (assertWalletOverrides
  // cannot catch that: the instanceof check passes either way). Refuse to boot
  // until the subclass is ported to the Drizzle repo.
  if (process.env.CITRINEOS_USE_DRIZZLE === 'true') {
    throw new Error(
      'wallet: CITRINEOS_USE_DRIZZLE=true is not supported — ' +
        'WalletAuthorizationRepository extends the Sequelize repository. ' +
        'Unset the flag or port the wallet override to the Drizzle repository first.',
    );
  }
  container.register({
    authorizationRepository: asClass(WalletAuthorizationRepository).singleton(),
    walletRpcClient: asClass(WalletRpcClient).singleton(),
    walletRpcAuthorizer: asClass(WalletRpcAuthorizer).singleton(),
    // Gate the authorizer behind RABBITMQ_AUTH so an unconfigured deploy keeps
    // upstream behavior (pre-synced rows only, no RPC).
    authorizers: asFunction(({ walletRpcAuthorizer }: { walletRpcAuthorizer: WalletRpcAuthorizer }) =>
      walletRpcEnabled() ? [walletRpcAuthorizer] : [],
    ).singleton(),
  });
}

// Multi-replica ROUTER MODE (upstream feat 610c7131c). beta5 ships the two-mode
// RabbitMqReceiver — module mode = one queue per charger identifier; router mode =
// ONE queue per router instance (`rabbit_queue_router_<instanceIdentifier>`) + a
// headers-exchange binding per charger. At scale (hundreds→thousands of chargers)
// router mode avoids the per-charger-queue explosion + reconnect-storm that module
// mode hits. But stock beta5 never passes routerMode:true (container.ts registers
// routerHandler without it), so we re-register routerHandler here with it ON.
//
// Scoped to routerHandler ONLY (the router's singleton receiver — module handlers are
// never touched, so this is safe on the module server too, where routerHandler is not
// resolved). Only invoked from the lean router entrypoint (dist/wallet/router-main.js).
//
// SAFETY (advisor): a per-instance queue is only correct if instanceIdentifier is
// UNIQUE per pod — a shared id makes two pods consume one queue and round-robins ALL
// traffic. So we HARD-FAIL if it is unset rather than fall back to the receiver's
// ephemeral `router-${Date.now()}` default. The deploy sets it from the pod name
// (valueFrom metadata.name → CITRINEOS_MESSAGEBROKER_AMQP_INSTANCEIDENTIFIER), which is
// unique + stable per pod.
export function registerRouterMode(container: AwilixContainer): void {
  container.register({
    routerHandler: asFunction(
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      ({ config, channelManager, logger }: any) => {
        const instanceId = config?.messageBroker?.amqp?.instanceIdentifier;
        if (!instanceId) {
          throw new Error(
            'wallet: router mode requires messageBroker.amqp.instanceIdentifier ' +
              '(env CITRINEOS_MESSAGEBROKER_AMQP_INSTANCEIDENTIFIER), unique per pod — ' +
              'refusing to start the router without a unique instance queue.',
          );
        }
        return new RabbitMqReceiver({ config, channelManager, logger, routerMode: true });
      },
    ).singleton(),
  });
}

// Boot-time tripwire: upstream renaming a token would make our overrides silently
// unapplied (awilix has no unknown-key error). Resolve back and verify.
export function assertWalletOverrides(container: AwilixContainer): void {
  const repo = container.resolve('authorizationRepository');
  if (!(repo instanceof WalletAuthorizationRepository)) {
    throw new Error('wallet: authorizationRepository override NOT applied — upstream token renamed?');
  }
  const authorizers = container.resolve('authorizers') as unknown[];
  if (walletRpcEnabled() && !authorizers.some((a) => a instanceof WalletRpcAuthorizer)) {
    throw new Error('wallet: authorizers override NOT applied — upstream token renamed?');
  }
}
