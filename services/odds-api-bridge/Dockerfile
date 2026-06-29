FROM node:22-alpine

WORKDIR /app

# Install deps first for layer caching.
COPY package.json ./
RUN npm install --omit=dev --no-audit --no-fund

# Then copy source.
COPY preflight.mjs ./
COPY src ./src

ENV NODE_ENV=production
EXPOSE 3000

USER node
CMD ["npm", "start"]
