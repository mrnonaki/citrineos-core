// SPDX-License-Identifier: Apache-2.0
// ChargeMai wallet fork — real-time wallet decision as an IAuthorizer.
//
// Runs only after a local Authorization row resolved to Accepted (upstream chain
// semantics), so it can only downgrade — which is exactly the gate contract.
// The built-in RealTimeAuthorizer is prepended before us in TransactionService but
// is a pure pass-through while rows have no realTimeAuthUrl.
//
// Protocol constraint: OCPP 1.6 mappers throw on statuses outside
// {Accepted, Blocked, Expired, Invalid, ConcurrentTx}; richer statuses (NoCredit, …)
// are 2.x-only. We resolve the station's protocol and clamp accordingly.

import type { IMessageContext } from '@citrineos/base';
import { AuthorizationStatusEnum } from '@citrineos/types';
import type { ILogObj, Logger } from 'tslog';
import { detectTokenType } from './WalletRpcClient.js';

const OCPP16_SAFE_STATUSES: ReadonlySet<string> = new Set([
  AuthorizationStatusEnum.Accepted,
  AuthorizationStatusEnum.Blocked,
  AuthorizationStatusEnum.Expired,
  AuthorizationStatusEnum.Invalid,
  AuthorizationStatusEnum.ConcurrentTx,
]);

const CACHE_NAMESPACE = 'walletAuth';
// Same knob as the 1.9.1 fork (ms); used as the Redis last-attempt TTL here.
const CACHE_TTL_SECONDS = Math.max(1, Number(process.env.RABBITMQ_AUTH_CACHE_TTL_MS ?? 120000) / 1000);

interface WalletRpcAuthorizerCradle {
  cache: any;
  logger: Logger<ILogObj>;
  locationRepository: any;
  walletRpcClient: any;
}

export class WalletRpcAuthorizer {
  private readonly _cache: any;
  private readonly _logger: Logger<ILogObj>;
  private readonly _locationRepository: any;
  private readonly _rpc: any;

  constructor({ cache, logger, locationRepository, walletRpcClient }: WalletRpcAuthorizerCradle) {
    this._cache = cache;
    this._logger = logger.getSubLogger({ name: this.constructor.name });
    this._locationRepository = locationRepository;
    this._rpc = walletRpcClient;
  }

  async authorize(
    authorization: any,
    context: IMessageContext,
    evse?: any,
    connector?: any,
  ): Promise<any> {
    const idToken: string = authorization.idToken;
    const station = context.ocppConnectionName;
    const cacheKey = `${context.tenantId}:${idToken}:${station}:${evse?.evseId ?? connector?.connectorId ?? '-'}`;

    try {
      // Contract payload is exactly these 4 fields (extras are ignored wallet-side).
      const decision = await this._rpc.authorizeToken({
        tenantId: context.tenantId,
        stationId: station,
        idToken,
        idTokenType: detectTokenType(idToken, authorization.idTokenType),
      });
      const status = await this._clampForProtocol(context, decision.status);
      // Last-attempt cache: cover the Authorize → StartTransaction window without
      // touching cacheExpiryDateTime (multi-replica-safe via shared cache/redis).
      // Wallet-supplied cacheExpiryDateTime, when present, wins over the default TTL.
      let ttl = CACHE_TTL_SECONDS;
      if (decision.cacheExpiryDateTime) {
        const ms = Date.parse(decision.cacheExpiryDateTime) - Date.now();
        if (Number.isFinite(ms) && ms > 0) ttl = Math.ceil(ms / 1000);
      }
      await this._cache.set(cacheKey, status, CACHE_NAMESPACE, ttl);
      return status;
    } catch (err) {
      const cached: string | null = await this._cache.get(cacheKey, CACHE_NAMESPACE);
      if (cached) {
        this._logger.warn(`wallet RPC failed; using last-attempt cache (${cached}): ${err}`);
        return cached;
      }
      this._logger.error(`wallet RPC failed with no cached decision — failing closed: ${err}`);
      return this._clampForProtocol(context, AuthorizationStatusEnum.Invalid);
    }
  }

  private async _clampForProtocol(context: IMessageContext, status: string): Promise<string> {
    try {
      const station = await this._locationRepository.readChargingStationByOcppConnectionName(
        context.tenantId,
        context.ocppConnectionName,
      );
      if (station?.protocol === 'ocpp1.6' && !OCPP16_SAFE_STATUSES.has(status)) {
        return AuthorizationStatusEnum.Invalid;
      }
    } catch {
      // Station lookup is best-effort; unknown protocol → be conservative.
      if (!OCPP16_SAFE_STATUSES.has(status)) {
        return AuthorizationStatusEnum.Invalid;
      }
    }
    return status;
  }
}
