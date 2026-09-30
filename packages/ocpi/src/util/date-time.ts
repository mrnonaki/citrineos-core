// SPDX-FileCopyrightText: 2026 Contributors to the CitrineOS Project
//
// SPDX-License-Identifier: Apache-2.0

// OCPI DateTime is RFC 3339 string(25) in UTC. Hasura hands timestamptz columns over as
// strings with microseconds and a numeric offset (32 characters), which strict receivers
// reject; a Date serialises to the conformant form.
export function toOcpiDateTime(value: Date | string): Date {
  return value instanceof Date ? value : new Date(value);
}
