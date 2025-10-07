// Jest setup file
import { jest } from '@jest/globals';

// Set test environment variables
process.env.NODE_ENV = 'test';
process.env.LOG_LEVEL = 'error';
process.env.DB_PATH = ':memory:';
process.env.API_KEY = 'k7F9qLp3XzR2vH8sT1mN4bC0yW6uJ5eQG4tY9bH2wQ6nM1vS8xR3cL5pZ0kF7uDe';

// Increase timeout for embedding model loading
jest.setTimeout(30000);

// Suppress console logs during tests
global.console = {
  ...console,
  log: jest.fn(),
  debug: jest.fn(),
  info: jest.fn(),
  warn: jest.fn(),
  error: jest.fn(),
};
