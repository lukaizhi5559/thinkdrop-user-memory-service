import logger from '../utils/logger.js';

const metrics = {
  requestCount: 0,
  errorCount: 0,
  totalResponseTime: 0,
  requestsByAction: {}
};

export function metricsMiddleware(req, res, next) {
  const startTime = Date.now();

  // Increment request count
  metrics.requestCount++;

  // Track by action
  const action = req.body?.action || 'unknown';
  if (!metrics.requestsByAction[action]) {
    metrics.requestsByAction[action] = { count: 0, errors: 0, totalTime: 0 };
  }
  metrics.requestsByAction[action].count++;

  // Override res.json to capture response
  const originalJson = res.json.bind(res);
  res.json = function(data) {
    const elapsedMs = Date.now() - startTime;
    metrics.totalResponseTime += elapsedMs;
    metrics.requestsByAction[action].totalTime += elapsedMs;

    // Track errors
    if (data.status === 'error') {
      metrics.errorCount++;
      metrics.requestsByAction[action].errors++;
    }

    // Add metrics to response
    if (data.version === 'mcp.v1') {
      data.metrics = {
        ...(data.metrics || {}),
        elapsedMs
      };
    }

    logger.info('Request completed', {
      action,
      status: data.status,
      elapsedMs,
      requestId: req.body?.requestId
    });

    return originalJson(data);
  };

  next();
}

export function getMetrics() {
  const avgResponseTime = metrics.requestCount > 0 
    ? metrics.totalResponseTime / metrics.requestCount 
    : 0;

  const errorRate = metrics.requestCount > 0 
    ? metrics.errorCount / metrics.requestCount 
    : 0;

  return {
    requestCount: metrics.requestCount,
    errorCount: metrics.errorCount,
    errorRate: errorRate.toFixed(4),
    avgResponseTime: Math.round(avgResponseTime),
    requestsByAction: metrics.requestsByAction
  };
}

export default metricsMiddleware;
