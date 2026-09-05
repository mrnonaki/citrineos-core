// SPDX-FileCopyrightText: 2025 Contributors to the CitrineOS Project
//
// SPDX-License-Identifier: Apache-2.0

import { Service } from 'typedi';
import { OcpiGraphqlClient } from '../graphql/OcpiGraphqlClient.js';
import { GET_LIVE_STATE_QUERY } from '../graphql/queries/live.queries.js';

/** SoC is rounded to this step before leaving the instance (privacy fuzzing). */
export const SOC_STEP_PCT = 5;
/** Samples older than this are cut — never served as "live". */
export const SOC_STALE_MS = 2 * 60 * 1000;
/** Minimum SoC slope (%/min) considered meaningful for an ETA. */
const MIN_ETA_RATE_PCT_PER_MIN = 0.05;
const MAX_ETA_MINUTES = 600;

interface SampledValue {
  value: number;
  measurand?: string;
  context?: string;
}

interface LiveQueryResult {
  Connectors: Array<{
    id: number;
    stationId: number | null;
    connectorId: number | null;
    status: string | null;
    timestamp: string | null;
    powerType: string | null;
    ChargingStation: { ocppConnectionName: string | null; isOnline: boolean | null } | null;
  }>;
  Transactions: Array<{
    stationId: number | null;
    connectorId: number | null;
    transactionId: string | null;
    chargingState: string | null;
    meterValues: Array<{ timestamp: string; sampledValue: SampledValue[] }>;
  }>;
}

export interface LiveEvse {
  station_id: string;
  connector_id: number;
  status: string | null;
  status_updated_at: string | null;
  power_type: string | null;
  online: boolean;
  charging: {
    transaction_id: string | null;
    charging_state: string | null;
    soc_pct?: number;
    power_w?: number;
    eta_minutes?: number;
    sampled_at?: string;
  } | null;
  conf: string[];
}

export function roundSoc(soc: number): number {
  return Math.min(100, Math.max(0, Math.round(soc / SOC_STEP_PCT) * SOC_STEP_PCT));
}

/** Linear ETA to 100% from the SoC slope across the sampled window. */
export function etaMinutes(
  samples: Array<{ soc: number; at: number }>,
): number | undefined {
  if (samples.length < 2) return undefined;
  const newest = samples[0];
  const oldest = samples[samples.length - 1];
  const minutes = (newest.at - oldest.at) / 60_000;
  if (minutes <= 0) return undefined;
  const rate = (newest.soc - oldest.soc) / minutes;
  if (rate < MIN_ETA_RATE_PCT_PER_MIN) return undefined;
  return Math.min(MAX_ETA_MINUTES, Math.round((100 - newest.soc) / rate));
}

@Service()
export class LiveService {
  constructor(private readonly graphqlClient: OcpiGraphqlClient) {}

  async getLive(tenantId: number, now: () => number = Date.now): Promise<LiveEvse[]> {
    const result = await this.graphqlClient.request<LiveQueryResult, { tenantId: number }>(
      GET_LIVE_STATE_QUERY,
      { tenantId },
    );
    return LiveService.build(result, now());
  }

  static build(result: LiveQueryResult, nowMs: number): LiveEvse[] {
    // Transactions.connectorId is a FK to Connectors.id (the row id), NOT the
    // OCPP connector number — verified against live rows (e.g. the ABB station
    // writes connectorId=5 for its single OCPP connector #1). Join on the
    // connector row id alone; it is unique across stations.
    const txByConnectorRowId = new Map<number, LiveQueryResult['Transactions'][number]>();
    for (const tx of result.Transactions) {
      if (tx.connectorId != null) txByConnectorRowId.set(tx.connectorId, tx);
    }

    return result.Connectors.map((connector) => {
      const conf: string[] = [];
      const isDc = connector.powerType === 'DC';
      if (!isDc) conf.push('NO_SOC_AC');

      const tx = txByConnectorRowId.get(connector.id);
      let charging: LiveEvse['charging'] = null;
      if (tx) {
        charging = {
          transaction_id: tx.transactionId,
          charging_state: tx.chargingState,
        };
        // Collect SoC + power samples, newest first; cut anything stale.
        const socSamples: Array<{ soc: number; at: number }> = [];
        let powerW: number | undefined;
        let sampledAt: string | undefined;
        for (const mv of tx.meterValues) {
          const at = Date.parse(mv.timestamp);
          const soc = mv.sampledValue.find((sv) => sv.measurand === 'SoC');
          const power = mv.sampledValue.find((sv) => sv.measurand === 'Power.Active.Import');
          if (soc && nowMs - at <= SOC_STALE_MS) {
            socSamples.push({ soc: soc.value, at });
            sampledAt ??= mv.timestamp;
          } else if (soc && socSamples.length > 0) {
            // Older-than-cutoff samples still help the slope once we have a
            // fresh anchor; without one, the whole series is stale — cut it.
            socSamples.push({ soc: soc.value, at });
          }
          if (power && powerW === undefined && nowMs - at <= SOC_STALE_MS) {
            powerW = power.value;
          }
        }
        if (socSamples.length > 0 && isDc) {
          charging.soc_pct = roundSoc(socSamples[0].soc);
          charging.sampled_at = sampledAt;
          const eta = etaMinutes(socSamples);
          if (eta !== undefined) charging.eta_minutes = eta;
        }
        if (powerW !== undefined) charging.power_w = powerW;
      }

      return {
        station_id: connector.ChargingStation?.ocppConnectionName ?? String(connector.stationId),
        connector_id: connector.connectorId ?? 0,
        status: connector.status,
        status_updated_at: connector.timestamp,
        power_type: connector.powerType,
        online: connector.ChargingStation?.isOnline ?? false,
        charging,
        conf,
      };
    });
  }
}
