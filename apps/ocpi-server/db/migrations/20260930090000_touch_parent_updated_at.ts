// SPDX-FileCopyrightText: 2026 Contributors to the CitrineOS Project
//
// SPDX-License-Identifier: Apache-2.0

'use strict';

import type { QueryInterface } from 'sequelize';

// OCPI last_updated is the row's updatedAt and GET /locations?date_from= filters
// Locations.updatedAt only, but a StatusNotification writes just the Connector
// row: an EVSE status flip never moved its parents, so incremental pulls missed
// every status change. Propagate updatedAt upwards. An updatedAt-only write is
// not "changed data" for the *Notify triggers, so this adds no pg_notify traffic.

export default {
  up: async (queryInterface: QueryInterface) => {
    await queryInterface.sequelize.query(`
      CREATE OR REPLACE FUNCTION "ConnectorTouchEvse"()
      RETURNS trigger AS $$
      DECLARE
        touchedAt timestamptz;
      BEGIN
        IF TG_OP = 'UPDATE' AND to_jsonb(OLD) = to_jsonb(NEW) THEN
          RETURN NULL;
        END IF;
        touchedAt := CASE
          WHEN TG_OP = 'UPDATE' AND NEW."updatedAt" IS NOT DISTINCT FROM OLD."updatedAt" THEN now()
          ELSE COALESCE(NEW."updatedAt", now())
        END;

        UPDATE "Evses"
        SET "updatedAt" = touchedAt
        WHERE "id" = NEW."evseId" AND "updatedAt" < touchedAt;

        RETURN NULL;
      END;
      $$ LANGUAGE plpgsql;
    `);

    await queryInterface.sequelize.query(`
      CREATE TRIGGER "ConnectorTouchEvse"
      AFTER INSERT OR UPDATE ON "Connectors"
      FOR EACH ROW
      EXECUTE FUNCTION "ConnectorTouchEvse"();
    `);

    await queryInterface.sequelize.query(`
      CREATE OR REPLACE FUNCTION "EvseTouchLocation"()
      RETURNS trigger AS $$
      DECLARE
        touchedAt timestamptz;
        parentLocationId integer;
      BEGIN
        IF TG_OP = 'UPDATE' AND to_jsonb(OLD) = to_jsonb(NEW) THEN
          RETURN NULL;
        END IF;
        touchedAt := CASE
          WHEN TG_OP = 'UPDATE' AND NEW."updatedAt" IS NOT DISTINCT FROM OLD."updatedAt" THEN now()
          ELSE COALESCE(NEW."updatedAt", now())
        END;

        SELECT "locationId" INTO parentLocationId
        FROM "ChargingStations"
        WHERE "id" = NEW."stationId";

        IF parentLocationId IS NOT NULL THEN
          UPDATE "Locations"
          SET "updatedAt" = touchedAt
          WHERE "id" = parentLocationId AND "updatedAt" < touchedAt;
        END IF;

        RETURN NULL;
      END;
      $$ LANGUAGE plpgsql;
    `);

    await queryInterface.sequelize.query(`
      CREATE TRIGGER "EvseTouchLocation"
      AFTER INSERT OR UPDATE ON "Evses"
      FOR EACH ROW
      EXECUTE FUNCTION "EvseTouchLocation"();
    `);
  },

  down: async (queryInterface: QueryInterface) => {
    await queryInterface.sequelize.query(`
      DROP TRIGGER IF EXISTS "EvseTouchLocation" ON "Evses";
    `);
    await queryInterface.sequelize.query(`
      DROP FUNCTION IF EXISTS "EvseTouchLocation"();
    `);
    await queryInterface.sequelize.query(`
      DROP TRIGGER IF EXISTS "ConnectorTouchEvse" ON "Connectors";
    `);
    await queryInterface.sequelize.query(`
      DROP FUNCTION IF EXISTS "ConnectorTouchEvse"();
    `);
  },
};
