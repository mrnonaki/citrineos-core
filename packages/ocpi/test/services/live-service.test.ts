// SPDX-FileCopyrightText: 2025 Contributors to the CitrineOS Project
//
// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it } from 'vitest';
import { etaMinutes, LiveService, roundSoc, SOC_STALE_MS } from '../../src/services/live-service.js';

const NOW = Date.parse('2026-01-01T12:00:00Z');
const iso = (msAgo: number) => new Date(NOW - msAgo).toISOString();

function connector(over: Record<string, unknown> = {}) {
  return {
    id: 1,
    stationId: 2,
    connectorId: 1,
    status: 'Charging',
    timestamp: iso(30_000),
    powerType: 'DC',
    ChargingStation: { ocppConnectionName: 'ZEEDA1', isOnline: true },
    ...over,
  };
}

function tx(
  meterValues: Array<{ timestamp: string; sampledValue: any[] }>,
  over: Record<string, unknown> = {},
) {
  return {
    stationId: 2,
    connectorId: 1,
    transactionId: 'tx-1',
    chargingState: 'Charging',
    meterValues,
    ...over,
  };
}

const socSample = (msAgo: number, soc: number, powerW?: number) => ({
  timestamp: iso(msAgo),
  sampledValue: [
    { value: soc, measurand: 'SoC' },
    ...(powerW !== undefined ? [{ value: powerW, measurand: 'Power.Active.Import' }] : []),
  ],
});

describe('roundSoc', () => {
  it('rounds to 5% steps and clamps', () => {
    expect(roundSoc(63)).toBe(65);
    expect(roundSoc(62)).toBe(60);
    expect(roundSoc(99)).toBe(100);
    expect(roundSoc(101)).toBe(100);
    expect(roundSoc(-1)).toBe(0);
  });
});

describe('etaMinutes', () => {
  it('projects linearly to 100% from the slope', () => {
    // 60% → 62% over 2 minutes = 1%/min → 38 min to 100
    const eta = etaMinutes([
      { soc: 62, at: NOW },
      { soc: 60, at: NOW - 120_000 },
    ]);
    expect(eta).toBe(38);
  });

  it('returns undefined for flat slope or a single sample', () => {
    expect(etaMinutes([{ soc: 80, at: NOW }])).toBeUndefined();
    expect(
      etaMinutes([
        { soc: 80, at: NOW },
        { soc: 80, at: NOW - 120_000 },
      ]),
    ).toBeUndefined();
  });
});

describe('LiveService.build', () => {
  it('joins active transaction SoC/power onto the connector', () => {
    const out = LiveService.build(
      {
        Connectors: [connector()],
        Transactions: [tx([socSample(30_000, 63, 55_000), socSample(90_000, 62)])],
      } as any,
      NOW,
    );
    expect(out).toHaveLength(1);
    expect(out[0].station_id).toBe('ZEEDA1');
    expect(out[0].charging?.soc_pct).toBe(65);
    expect(out[0].charging?.power_w).toBe(55_000);
    expect(out[0].charging?.eta_minutes).toBeGreaterThan(0);
    expect(out[0].conf).toEqual([]);
  });

  it('cuts SoC when every sample is older than the stale window', () => {
    const out = LiveService.build(
      {
        Connectors: [connector()],
        Transactions: [tx([socSample(SOC_STALE_MS + 1_000, 63)])],
      } as any,
      NOW,
    );
    expect(out[0].charging).not.toBeNull();
    expect(out[0].charging?.soc_pct).toBeUndefined();
  });

  it('flags AC connectors NO_SOC_AC and never reports their SoC', () => {
    const out = LiveService.build(
      {
        Connectors: [connector({ powerType: 'AC3Phase' })],
        Transactions: [tx([socSample(30_000, 63)])],
      } as any,
      NOW,
    );
    expect(out[0].conf).toEqual(['NO_SOC_AC']);
    expect(out[0].charging?.soc_pct).toBeUndefined();
  });

  it('joins on the connector ROW id — Transactions.connectorId is a FK to Connectors.id, not the OCPP connector number', () => {
    // Real-cluster shape: ABB station 6 has OCPP connector #1 stored as
    // Connectors row id 5; its transactions carry connectorId=5.
    const out = LiveService.build(
      {
        Connectors: [connector({ id: 5, connectorId: 1, stationId: 6 })],
        Transactions: [tx([socSample(30_000, 63)], { stationId: 6, connectorId: 5 })],
      } as any,
      NOW,
    );
    expect(out[0].charging).not.toBeNull();

    // A tx carrying the OCPP number (1) instead of the row id must NOT join.
    const wrong = LiveService.build(
      {
        Connectors: [connector({ id: 5, connectorId: 1, stationId: 6 })],
        Transactions: [tx([socSample(30_000, 63)], { stationId: 6, connectorId: 1 })],
      } as any,
      NOW,
    );
    expect(wrong[0].charging).toBeNull();
  });

  it('reports idle connectors with charging: null', () => {
    const out = LiveService.build(
      { Connectors: [connector({ status: 'Available' })], Transactions: [] } as any,
      NOW,
    );
    expect(out[0].charging).toBeNull();
    expect(out[0].status).toBe('Available');
  });
});
