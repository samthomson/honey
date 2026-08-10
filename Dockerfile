FROM node:22-slim

WORKDIR /app

COPY package.json package-lock.json* ./
RUN npm ci --production || npm install --production

COPY . .

RUN mkdir -p /app/data

ENV PORT=8080
ENV DATA_DIR=/app/data

EXPOSE 8080

CMD ["node", "src/index.js"]
