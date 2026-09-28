FROM node:20-alpine
WORKDIR /app
COPY package*.json ./
COPY tsconfig.base.json ./
COPY apps/api-gateway ./apps/api-gateway
COPY libs ./libs
RUN npm install --include-workspace-root --workspaces --silent
RUN npm run build --workspace=@poc/common
RUN npm run build --workspace=@poc/contracts
RUN npm run build --workspace=@poc/api-gateway
EXPOSE 3000
CMD ["npm", "run", "start:prod", "--workspace=@poc/api-gateway"]
