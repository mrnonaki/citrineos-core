// SPDX-License-Identifier: Apache-2.0
// ChargeMai wallet fork — container overrides. Called from WalletServer BEFORE
// super.initialize(), i.e. after buildContainer() but before any token resolution,
// which is equivalent to registering at the end of buildContainer (awilix register
// is last-write-wins). Root container required (strict mode + singletons).

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
    authorizers: asFunction(
      ({ walletRpcAuthorizer }: { walletRpcAuthorizer: WalletRpcAuthorizer }) =>
        walletRpcEnabled() ? [walletRpcAuthorizer] : [],
    ).singleton(),
  });
}

// Boot-time tripwire: upstream renaming a token would make our overrides silently
// unapplied (awilix has no unknown-key error). Resolve back and verify.
export function assertWalletOverrides(container: AwilixContainer): void {
  const repo = container.resolve('authorizationRepository');
  if (!(repo instanceof WalletAuthorizationRepository)) {
    throw new Error(
      'wallet: authorizationRepository override NOT applied — upstream token renamed?',
    );
  }
  const authorizers = container.resolve('authorizers') as unknown[];
  if (walletRpcEnabled() && !authorizers.some((a) => a instanceof WalletRpcAuthorizer)) {
    throw new Error('wallet: authorizers override NOT applied — upstream token renamed?');
  }
}
