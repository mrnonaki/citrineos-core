// SPDX-License-Identifier: Apache-2.0
// ChargeMai wallet fork — thin repository override.
//
// Unknown idTokens (e.g. autocharge MACs discovered at plug time) must reach the
// IAuthorizer chain, but every auth path short-circuits to Unknown/Invalid when the
// repository returns zero rows — BEFORE authorizers run. So on a miss we persist a
// stub row and return it, letting WalletRpcAuthorizer make the real decision.
//
// Stub-row invariants (violating any of these silently bypasses or breaks the gate):
//   1. status must be the literal 'Accepted' — upstream (next, post-beta4) REJECTS
//      a NULL status outright on the 1.6 paths (authorize-request-ocpp-16-handler +
//      TransactionService StartTransaction both fail closed on null), and only an
//      'Accepted' stored status lets the authorizer chain run at all.
//   2. Exactly one row per (tenantId, idToken) — regardless of idTokenType. 1.6
//      Authorize has NO type so it queries by idToken alone; if 1.6 (type NULL) and
//      2.x (type MacAddress) each stub their own row for the same value, the 1.6
//      query matches BOTH → authorizations.length !== 1 → Invalid before the wallet
//      gate runs (the charger is denied). A mixed-version fleet (a card/MAC used at
//      a 1.6 charger AND a 2.x charger) hits this on the SECOND cross-version use.
//      So: dedup on idToken VALUE only, and always store a value-DERIVED type
//      (detectTokenType) so the 2.x type-filtered query still resolves the one row.
//   3. Must be a real persisted row (createTransactionByStartTransaction reuses its id).

import { SequelizeAuthorizationRepository } from '@citrineos/dal';
import { AuthorizationStatusEnum } from '@citrineos/types';
import { detectTokenType } from './WalletRpcClient.js';

type AuthorizationQuerystring = {
  idToken?: string | null;
  type?: string | null;
  id?: number | null;
};

function stubbingEnabled(): boolean {
  // Unprefixed on purpose: CITRINEOS_* env vars are Zod-validated by defineConfig.
  return process.env.WALLET_STUB_UNKNOWN_TOKENS !== 'false';
}

export class WalletAuthorizationRepository extends SequelizeAuthorizationRepository {
  async readAllByQuerystring(tenantId: number, query: AuthorizationQuerystring): Promise<any[]> {
    const rows = await super.readAllByQuerystring(tenantId, query);
    if (rows.length > 0 || !query.idToken || !stubbingEnabled()) {
      return rows;
    }
    return [await this._stubCreate(tenantId, query)];
  }

  async readOnlyOneByQuerystring(
    tenantId: number,
    query: AuthorizationQuerystring,
  ): Promise<any | undefined> {
    const row = await super.readOnlyOneByQuerystring(tenantId, query);
    if (row || !query.idToken || !stubbingEnabled()) {
      return row;
    }
    return await this._stubCreate(tenantId, query);
  }

  /** Upsert an Accepted row (preparing gate / remote-start pre-auth). */
  async ensureAccepted(tenantId: number, idToken: string, idTokenType?: string): Promise<any> {
    // Dedup on idToken VALUE only (invariant #2); store a value-derived type.
    const [row] = await this._readOrCreateByQuery(tenantId, {
      where: { idToken },
      defaults: {
        idTokenType: detectTokenType(idToken, idTokenType),
        status: AuthorizationStatusEnum.Accepted,
        concurrentTransaction: false,
      },
    });
    if (row.status !== AuthorizationStatusEnum.Accepted) {
      await row.update({ status: AuthorizationStatusEnum.Accepted });
    }
    return row;
  }

  private async _stubCreate(tenantId: number, query: AuthorizationQuerystring): Promise<any> {
    // Dedup on idToken VALUE only — NEVER include query.type in the WHERE, or a 1.6
    // (type-less) and a 2.x (typed) lookup create two rows for the same value and
    // break the 1.6 length!==1 check. Store a value-derived type so the 2.x
    // type-filtered handler query still resolves this single row.
    const [row] = await this._readOrCreateByQuery(tenantId, {
      where: { idToken: query.idToken },
      defaults: {
        idTokenType: detectTokenType(query.idToken as string, query.type),
        status: AuthorizationStatusEnum.Accepted,
        concurrentTransaction: false,
      },
    });
    this.logger.info(
      `wallet: stub-created Authorization id=${row.id} type=${row.idTokenType} for unknown idToken (tenant ${tenantId})`,
    );
    return row;
  }
}
