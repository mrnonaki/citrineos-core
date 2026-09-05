// SPDX-FileCopyrightText: 2025 Contributors to the CitrineOS Project
//
// SPDX-License-Identifier: Apache-2.0

import type Koa from 'koa';
import { Ctx, Get, JsonController, UseBefore, useKoaServer } from 'routing-controllers';
import { Service } from 'typedi';
import { buildOcpiResponse, OcpiResponseStatusCode } from '../model/OcpiResponse.js';
import { LiveService } from '../services/LiveService.js';
import { RegistrationAuthMiddleware } from '../util/middleware/AuthMiddleware.js';
import { OcpiExceptionHandler } from '../util/middleware/OcpiExceptionHandler.js';

/**
 * /ocpi-x/v1/live — chargemai extension, NOT part of OCPI 2.2.1: per-EVSE
 * live state (status + SoC rounded to 5% + ETA) for the feed-hub. Mounted
 * under its own /ocpi-x prefix (see OcpiServer.initKoaServer). Auth is the
 * same OCPI credentials token as the standard endpoints, but without the
 * OCPI from/to routing headers (RegistrationAuthMiddleware) — the resource
 * is tenant-scoped by the token itself.
 */
@JsonController('/v1')
@Service()
export class LiveController {
  constructor(readonly liveService: LiveService) {}

  @Get('/live')
  @UseBefore(RegistrationAuthMiddleware)
  @UseBefore(OcpiExceptionHandler)
  async getLive(@Ctx() ctx: any) {
    const tenantId = ctx.state?.tenantPartner?.tenant?.id;
    const evses = await this.liveService.getLive(tenantId);
    return buildOcpiResponse(OcpiResponseStatusCode.GenericSuccessCode, evses);
  }
}

/**
 * Mount the /ocpi-x extension namespace on the OCPI server's Koa app.
 * Lives here (not in KoaServer/OcpiServer) so the fork's footprint on
 * upstream files stays at a single two-line hook in index.ts — the smaller
 * the upstream diff, the cheaper every upstream sync.
 */
export function mountOcpiX(koa: Koa): void {
  useKoaServer(koa, {
    controllers: [LiveController],
    routePrefix: '/ocpi-x',
    middlewares: [],
    defaultErrorHandler: false,
  });
}
