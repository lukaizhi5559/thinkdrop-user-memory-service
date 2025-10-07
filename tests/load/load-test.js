import http from 'http';
import { performance } from 'perf_hooks';

const API_KEY = 'k7F9qLp3XzR2vH8sT1mN4bC0yW6uJ5eQG4tY9bH2wQ6nM1vS8xR3cL5pZ0kF7uDe';
const HOST = 'localhost';
const PORT = 3001;

// Test configuration
const config = {
  duration: 60000, // 60 seconds
  concurrency: 10,
  requestsPerSecond: 100
};

// Metrics
const metrics = {
  totalRequests: 0,
  successfulRequests: 0,
  failedRequests: 0,
  responseTimes: [],
  errors: []
};

/**
 * Make HTTP request
 */
function makeRequest(action, payload) {
  return new Promise((resolve, reject) => {
    const data = JSON.stringify({
      version: 'mcp.v1',
      service: 'user-memory',
      action,
      requestId: `load-test-${Date.now()}-${Math.random()}`,
      context: { userId: 'load_test_user' },
      payload
    });

    const options = {
      hostname: HOST,
      port: PORT,
      path: `/${action}`,
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Content-Length': Buffer.byteLength(data),
        'Authorization': `Bearer ${API_KEY}`
      }
    };

    const startTime = performance.now();

    const req = http.request(options, (res) => {
      let responseData = '';

      res.on('data', (chunk) => {
        responseData += chunk;
      });

      res.on('end', () => {
        const endTime = performance.now();
        const responseTime = endTime - startTime;

        metrics.totalRequests++;
        metrics.responseTimes.push(responseTime);

        if (res.statusCode === 200) {
          metrics.successfulRequests++;
          resolve({ statusCode: res.statusCode, responseTime, data: responseData });
        } else {
          metrics.failedRequests++;
          reject(new Error(`HTTP ${res.statusCode}: ${responseData}`));
        }
      });
    });

    req.on('error', (error) => {
      metrics.totalRequests++;
      metrics.failedRequests++;
      metrics.errors.push(error.message);
      reject(error);
    });

    req.write(data);
    req.end();
  });
}

/**
 * Generate random memory text
 */
function generateMemoryText() {
  const templates = [
    'Meeting with {person} on {date} at {time}',
    'Reminder to {action} {item} by {date}',
    'Note: {person} mentioned {topic} during our conversation',
    'Task: Complete {task} before {date}',
    'Appointment with {person} at {location} on {date}'
  ];

  const persons = ['Alice', 'Bob', 'Charlie', 'Diana', 'Eve'];
  const actions = ['buy', 'review', 'send', 'update', 'check'];
  const items = ['report', 'document', 'email', 'presentation', 'code'];
  const topics = ['project deadline', 'budget', 'new feature', 'bug fix', 'meeting'];
  const tasks = ['analysis', 'testing', 'deployment', 'documentation', 'review'];
  const locations = ['office', 'conference room', 'cafe', 'home', 'zoom'];

  const template = templates[Math.floor(Math.random() * templates.length)];
  
  return template
    .replace('{person}', persons[Math.floor(Math.random() * persons.length)])
    .replace('{action}', actions[Math.floor(Math.random() * actions.length)])
    .replace('{item}', items[Math.floor(Math.random() * items.length)])
    .replace('{topic}', topics[Math.floor(Math.random() * topics.length)])
    .replace('{task}', tasks[Math.floor(Math.random() * tasks.length)])
    .replace('{location}', locations[Math.floor(Math.random() * locations.length)])
    .replace('{date}', 'tomorrow')
    .replace('{time}', '3pm');
}

/**
 * Run load test scenario
 */
async function runScenario() {
  const scenarios = [
    // 60% store operations
    { action: 'memory.store', weight: 0.6, payload: () => ({ text: generateMemoryText() }) },
    // 30% search operations
    { action: 'memory.search', weight: 0.3, payload: () => ({ query: 'meeting', limit: 10 }) },
    // 10% list operations
    { action: 'memory.list', weight: 0.1, payload: () => ({ limit: 25, offset: 0 }) }
  ];

  const random = Math.random();
  let cumulativeWeight = 0;

  for (const scenario of scenarios) {
    cumulativeWeight += scenario.weight;
    if (random <= cumulativeWeight) {
      try {
        await makeRequest(scenario.action, scenario.payload());
      } catch (error) {
        // Error already tracked in metrics
      }
      break;
    }
  }
}

/**
 * Calculate percentile
 */
function percentile(arr, p) {
  if (arr.length === 0) return 0;
  const sorted = arr.slice().sort((a, b) => a - b);
  const index = Math.ceil((p / 100) * sorted.length) - 1;
  return sorted[index];
}

/**
 * Print metrics
 */
function printMetrics() {
  const successRate = (metrics.successfulRequests / metrics.totalRequests) * 100;
  const errorRate = (metrics.failedRequests / metrics.totalRequests) * 100;
  const avgResponseTime = metrics.responseTimes.reduce((a, b) => a + b, 0) / metrics.responseTimes.length;

  console.log('\n=== Load Test Results ===\n');
  console.log(`Total Requests:       ${metrics.totalRequests}`);
  console.log(`Successful Requests:  ${metrics.successfulRequests} (${successRate.toFixed(2)}%)`);
  console.log(`Failed Requests:      ${metrics.failedRequests} (${errorRate.toFixed(2)}%)`);
  console.log('\nResponse Times:');
  console.log(`  Average:            ${avgResponseTime.toFixed(2)}ms`);
  console.log(`  p50 (Median):       ${percentile(metrics.responseTimes, 50).toFixed(2)}ms`);
  console.log(`  p95:                ${percentile(metrics.responseTimes, 95).toFixed(2)}ms`);
  console.log(`  p99:                ${percentile(metrics.responseTimes, 99).toFixed(2)}ms`);
  console.log(`  Min:                ${Math.min(...metrics.responseTimes).toFixed(2)}ms`);
  console.log(`  Max:                ${Math.max(...metrics.responseTimes).toFixed(2)}ms`);
  
  if (metrics.errors.length > 0) {
    console.log('\nTop Errors:');
    const errorCounts = {};
    metrics.errors.forEach(err => {
      errorCounts[err] = (errorCounts[err] || 0) + 1;
    });
    Object.entries(errorCounts)
      .sort((a, b) => b[1] - a[1])
      .slice(0, 5)
      .forEach(([err, count]) => {
        console.log(`  ${err}: ${count}`);
      });
  }

  console.log('\n=== Performance Assessment ===\n');
  
  // Check against targets
  const p50 = percentile(metrics.responseTimes, 50);
  const p95 = percentile(metrics.responseTimes, 95);
  const p99 = percentile(metrics.responseTimes, 99);

  console.log(`p50 latency: ${p50.toFixed(2)}ms ${p50 < 50 ? '✅ PASS' : '❌ FAIL'} (target: <50ms)`);
  console.log(`p95 latency: ${p95.toFixed(2)}ms ${p95 < 500 ? '✅ PASS' : '❌ FAIL'} (target: <500ms)`);
  console.log(`p99 latency: ${p99.toFixed(2)}ms ${p99 < 1000 ? '✅ PASS' : '❌ FAIL'} (target: <1000ms)`);
  console.log(`Error rate:  ${errorRate.toFixed(2)}% ${errorRate < 1 ? '✅ PASS' : '❌ FAIL'} (target: <1%)`);
  
  const throughput = (metrics.totalRequests / (config.duration / 1000)).toFixed(2);
  console.log(`Throughput:  ${throughput} req/s ${throughput > 100 ? '✅ PASS' : '❌ FAIL'} (target: >100 req/s)`);
}

/**
 * Main load test
 */
async function runLoadTest() {
  console.log('=== Starting Load Test ===\n');
  console.log(`Duration:       ${config.duration / 1000}s`);
  console.log(`Concurrency:    ${config.concurrency}`);
  console.log(`Target RPS:     ${config.requestsPerSecond}`);
  console.log(`Host:           ${HOST}:${PORT}\n`);

  const startTime = Date.now();
  const interval = 1000 / config.requestsPerSecond;

  // Run concurrent workers
  const workers = [];
  for (let i = 0; i < config.concurrency; i++) {
    workers.push(
      (async () => {
        while (Date.now() - startTime < config.duration) {
          await runScenario();
          await new Promise(resolve => setTimeout(resolve, interval * config.concurrency));
        }
      })()
    );
  }

  // Progress indicator
  const progressInterval = setInterval(() => {
    const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);
    const progress = ((Date.now() - startTime) / config.duration * 100).toFixed(1);
    process.stdout.write(`\rProgress: ${progress}% | Elapsed: ${elapsed}s | Requests: ${metrics.totalRequests} | Success: ${metrics.successfulRequests} | Failed: ${metrics.failedRequests}`);
  }, 1000);

  // Wait for all workers to complete
  await Promise.all(workers);

  clearInterval(progressInterval);
  console.log('\n\nLoad test completed!');

  printMetrics();
}

// Check if service is running
console.log('Checking if service is running...');
http.get(`http://${HOST}:${PORT}/service.health`, (res) => {
  if (res.statusCode === 200) {
    console.log('Service is running. Starting load test...\n');
    runLoadTest().catch(console.error);
  } else {
    console.error(`Service returned status ${res.statusCode}. Please start the service first.`);
    process.exit(1);
  }
}).on('error', (err) => {
  console.error(`Cannot connect to service at ${HOST}:${PORT}`);
  console.error('Please start the service first with: npm start');
  process.exit(1);
});
