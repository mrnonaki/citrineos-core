// SPDX-FileCopyrightText: 2025 Contributors to the CitrineOS Project
//
// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it } from 'vitest';
import { GET_LIVE_STATE_QUERY } from '../../../src/transport/graphql/queries/live-queries.js';

describe('GET_LIVE_STATE_QUERY', () => {
  it('scopes both legs by tenantId (Hasura runs as admin — no row-level tenancy)', () => {
    const matches = GET_LIVE_STATE_QUERY.match(/tenantId: \{ _eq: \$tenantId \}/g);
    expect(matches).toHaveLength(2);
  });

  it('selects the fields LiveService depends on', () => {
    for (const field of [
      'status',
      'powerType',
      'ocppConnectionName',
      'isOnline',
      'chargingState',
      'sampledValue',
      'timestamp',
      'transactionId',
    ]) {
      expect(GET_LIVE_STATE_QUERY).toContain(field);
    }
  });

  it('only reads active transactions, newest meter values first', () => {
    expect(GET_LIVE_STATE_QUERY).toContain('isActive: { _eq: true }');
    expect(GET_LIVE_STATE_QUERY).toContain('order_by: { timestamp: desc }');
  });
});
