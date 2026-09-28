FROM node:20-alpine
WORKDIR /app
COPY package*.json ./
COPY tsconfig.base.json ./
COPY apps/inventory-service ./apps/inventory-service
COPY libs ./libs
RUN npm install --include-workspace-root --workspaces --silent
RUN npm run build --workspace=@poc/common
RUN npm run build --workspace=@poc/contracts
RUN npm run build --workspace=@poc/messaging
RUN npm run build --workspace=@poc/inventory-service
EXPOSE 3003
CMD ["npm", "run", "start:prod", "--workspace=@poc/inventory-service"]
