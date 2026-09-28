"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.buildTraceId = exports.sleep = void 0;
const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
exports.sleep = sleep;
const buildTraceId = () => `trace-${Date.now()}-${Math.random().toString(16).slice(2, 10)}`;
exports.buildTraceId = buildTraceId;
//# sourceMappingURL=index.js.map