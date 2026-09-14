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

// Multi-replica router mode. Upstream ships a two-mode RabbitMqReceiver (queue-per-
// router-instance vs queue-per-charger) but registers `routerHandler` WITHOUT
// routerMode, so it defaults OFF — which drops CALLRESULTs at >1 router replica:
// the per-station queue is shared across every replica that ever served the station,
// and RabbitMQ round-robins each response, so only the socket-owning pod can deliver.
// Re-register routerHandler with routerMode enabled when INSTANCE_IDENTIFIER (per-pod,
// unique) is set, so each replica binds its own rabbit_queue_router_<id>. Gated on the
// env so single-replica deploys keep upstream default behavior. (Replaces our old
// apps/ocpp-server/src/container.ts patch — that file was refactored into @citrineos/ocpp.)
export function registerRouterMode(container: AwilixContainer): void {
  if (!process.env.INSTANCE_IDENTIFIER) {
    return;
  }
  container.register({
    routerHandler: asFunction(
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      ({ config, channelManager, logger }: any) =>
        new RabbitMqReceiver({ config, channelManager, logger, routerMode: true }),
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
