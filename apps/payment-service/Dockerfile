FROM node:20-alpine
WORKDIR /app
COPY package*.json ./
COPY tsconfig.base.json ./
COPY apps/payment-service ./apps/payment-service
COPY libs ./libs
RUN npm install --include-workspace-root --workspaces --silent
RUN npm run build --workspace=@poc/common
RUN npm run build --workspace=@poc/contracts
RUN npm run build --workspace=@poc/messaging
RUN npm run build --workspace=@poc/payment-service
EXPOSE 3002
CMD ["npm", "run", "start:prod", "--workspace=@poc/payment-service"]
