# ── build stage: compile TS → dist ───────────────────────────────────────────
FROM node:22-slim AS build
WORKDIR /app
COPY package.json package-lock.json* ./
RUN npm install --legacy-peer-deps
COPY tsconfig.json ./
COPY src ./src
COPY public ./public
RUN npm run build

# ── runtime stage: prod deps + compiled dist + static dashboard + bundled client
FROM node:22-slim AS runtime
ENV NODE_ENV=production
WORKDIR /app
COPY package.json package-lock.json* ./
RUN npm install --omit=dev --legacy-peer-deps && npm cache clean --force
COPY --from=build /app/dist ./dist
COPY --from=build /app/public ./public

# Stream port (override with PORT). The MPP-paid WS lives at /ws/stream/:symbol.
EXPOSE 7070

# Basic liveness: the REST health endpoint.
HEALTHCHECK --interval=15s --timeout=4s --start-period=10s --retries=3 \
  CMD node -e "fetch('http://127.0.0.1:'+(process.env.PORT||7070)+'/health').then(r=>process.exit(r.ok?0:1)).catch(()=>process.exit(1))"

CMD ["node", "dist/server.js"]
