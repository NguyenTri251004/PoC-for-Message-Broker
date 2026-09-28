FROM node:20-alpine
WORKDIR /app
COPY package*.json ./
COPY tsconfig.base.json ./
COPY apps/notification-service ./apps/notification-service
COPY libs ./libs
RUN npm install --include-workspace-root --workspaces --silent
RUN npm run build --workspace=@poc/common
RUN npm run build --workspace=@poc/contracts
RUN npm run build --workspace=@poc/messaging
RUN npm run build --workspace=@poc/notification-service
EXPOSE 3004
CMD ["npm", "run", "start:prod", "--workspace=@poc/notification-service"]
