export const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));
export const buildTraceId = () => `trace-${Date.now()}-${Math.random().toString(16).slice(2, 10)}`;
