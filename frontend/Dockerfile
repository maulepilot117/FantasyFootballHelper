# syntax=docker/dockerfile:1
FROM oven/bun:1.3.12-debian AS build
WORKDIR /app
COPY package.json bun.lock ./
# Lockfiles generated on x86 can poison arm installs (DEPLOYMENT.md §6). If --frozen-lockfile
# fails on the arm runner, regenerate the lockfile there and commit it.
RUN bun install --frozen-lockfile
COPY . .
RUN bun run build

FROM nginx:1.28.0-bookworm
COPY nginx.conf /etc/nginx/conf.d/default.conf
COPY --from=build /app/dist /usr/share/nginx/html
EXPOSE 3000
