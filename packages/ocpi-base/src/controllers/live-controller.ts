// SPDX-FileCopyrightText: 2025 Contributors to the CitrineOS Project
//
// SPDX-License-Identifier: Apache-2.0

import type Koa from 'koa';
import { Ctx, Get, JsonController, UseBefore, useKoaServer } from 'routing-controllers';
import { buildOcpiResponse, OcpiResponseStatusCode } from '../model/ocpi-response.js';
import type { LiveService } from '../services/live-service.js';
import { RegistrationAuthMiddleware } from '../util/middleware/auth-middleware.js';
import { OcpiExceptionHandler } from '../util/middleware/ocpi-exception-handler.js';

export interface LiveControllerDependencies {
  liveService: LiveService;
}

/**
 * /ocpi-x/v1/live — chargemai extension, NOT part of OCPI 2.2.1: per-EVSE
 * live state (status + SoC rounded to 5% + ETA) for the feed-hub. Mounted
 * under its own /ocpi-x prefix (see mountOcpiX / OcpiServer.initKoaServer).
 * Auth is the same OCPI credentials token as the standard endpoints, but
 * without the OCPI from/to routing headers (RegistrationAuthMiddleware) — the
 * resource is tenant-scoped by the token itself.
 */
@JsonController('/v1')
export class LiveController {
  private readonly liveService: LiveService;

  constructor({ liveService }: LiveControllerDependencies) {
    this.liveService = liveService;
  }

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
 * upstream files stays at a single hook in index.ts — the smaller the
 * upstream diff, the cheaper every upstream sync. Controllers/middlewares
 * are resolved through the routing-controllers IoC adapter already installed
 * by buildOcpiContainer, so LiveController + LiveService must be registered
 * in the awilix container (see registerLiveExtension in container.ts).
 */
export function mountOcpiX(koa: Koa): void {
  useKoaServer(koa, {
    controllers: [LiveController],
    routePrefix: '/ocpi-x',
    middlewares: [],
    defaultErrorHandler: false,
  });
}
