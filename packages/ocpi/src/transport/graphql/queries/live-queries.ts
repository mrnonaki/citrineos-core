// SPDX-FileCopyrightText: 2025 Contributors to the CitrineOS Project
//
// SPDX-License-Identifier: Apache-2.0

import { gql } from 'graphql-request';

// Live-state query for the /ocpi-x/v1/live extension: one round-trip fetching
// (a) every connector's latest OCPP status and (b) the active transactions
// with their most recent meter values (SoC extraction happens in LiveService —
// sampledValue is a JSON column, so measurand filtering can't be pushed down).
// tenantId is filtered explicitly: the server queries Hasura as admin, so
// row-level tenancy is NOT enforced for us.
export const GET_LIVE_STATE_QUERY = gql`
  query GetLiveState($tenantId: Int!) {
    Connectors(where: { tenantId: { _eq: $tenantId } }) {
      id
      stationId
      connectorId
      status
      timestamp
      powerType
      ChargingStation {
        ocppConnectionName
        isOnline
      }
    }
    Transactions(where: { tenantId: { _eq: $tenantId }, isActive: { _eq: true } }) {
      stationId
      connectorId
      transactionId
      chargingState
      meterValues: MeterValues(order_by: { timestamp: desc }, limit: 10) {
        timestamp
        sampledValue
      }
    }
  }
`;
