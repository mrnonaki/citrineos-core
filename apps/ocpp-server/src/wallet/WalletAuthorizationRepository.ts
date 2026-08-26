// SPDX-License-Identifier: Apache-2.0
// ChargeMai wallet fork — thin repository override.
//
// Unknown idTokens (e.g. autocharge MACs discovered at plug time) must reach the
// IAuthorizer chain, but every auth path short-circuits to Unknown/Invalid when the
// repository returns zero rows — BEFORE authorizers run. So on a miss we persist a
// stub row and return it, letting WalletRpcAuthorizer make the real decision.
//
// Stub-row invariants (violating any of these silently bypasses or breaks the gate):
//   1. status must be the literal 'Accepted' — a NULL status short-circuits to
//      Accepted in the 1.6 paths WITHOUT running authorizers.
//   2. Exactly one row per (tenantId, idToken, idTokenType) — >1 rows throws
//      uncaught in the 2.x Authorize handlers (CallError InternalError).
//   3. Must be a real persisted row (createTransactionByStartTransaction reuses its id).

import { SequelizeAuthorizationRepository } from '@citrineos/core';
import { AuthorizationStatusEnum } from '@citrineos/types';

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

  private async _stubCreate(tenantId: number, query: AuthorizationQuerystring): Promise<any> {
    const where: Record<string, unknown> = { idToken: query.idToken };
    if (query.type) {
      where.idTokenType = query.type;
    }
    const [row] = await this._readOrCreateByQuery(tenantId, {
      where,
      defaults: {
        status: AuthorizationStatusEnum.Accepted,
        concurrentTransaction: false,
      },
    });
    this.logger.info(
      `wallet: stub-created Authorization id=${row.id} for unknown idToken (tenant ${tenantId})`,
    );
    return row;
  }
}
