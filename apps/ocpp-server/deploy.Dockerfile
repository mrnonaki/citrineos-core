#  SPDX-FileCopyrightText: 2025 Contributors to the CitrineOS Project
#
#  SPDX-License-Identifier: Apache-2.0

# Use a specific base image with platform support
FROM --platform=${BUILDPLATFORM:-linux/amd64} node:24.16.0 AS build

RUN corepack enable

WORKDIR /usr/local/apps/citrineos

COPY . .
RUN pnpm install --frozen-lockfile
RUN pnpm --filter "@citrineos/ocpp-server..." build

# Prune to a production-only bundle of ocpp-server + its workspace deps.
# The old COPY-everything approach shipped the whole monorepo with every
# package's devDependencies (1.77Gi); `pnpm deploy` keeps only what the
# ocpp-server actually needs at runtime. --legacy: pnpm 10 requires either
# injected workspace packages or this flag for deploy.
RUN pnpm --filter "@citrineos/ocpp-server" deploy --legacy --prod /deploy

# The final stage, which copies built files and prepares the run environment
# Using a slim image to reduce the final image size
FROM node:24.16.0-slim

# no corepack in the final image: the entrypoint calls
# ./node_modules/.bin/sequelize-cli directly, so nothing needs pnpm (or a
# network fetch of it) at runtime.

COPY --from=build /deploy /usr/local/apps/citrineos
# pnpm deploy honours the package's `files` list (["dist"]), so runtime files
# living outside dist/ must be copied explicitly.
COPY --from=build /usr/local/apps/citrineos/apps/ocpp-server/entrypoint.sh /usr/local/apps/citrineos/entrypoint.sh
COPY --from=build /usr/local/apps/citrineos/apps/ocpp-server/.sequelizerc /usr/local/apps/citrineos/.sequelizerc

WORKDIR /usr/local/apps/citrineos

RUN chmod +x /usr/local/apps/citrineos/entrypoint.sh

EXPOSE 8080

# entrypoint.sh self-locates via SCRIPT_DIR, so the flattened layout
# (package root = image root dir, no apps/ prefix) works unchanged.
ENTRYPOINT ["/usr/local/apps/citrineos/entrypoint.sh"]
