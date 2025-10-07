#!/bin/bash

# Quick test script - runs essential tests only

echo "🚀 Quick Test Suite"
echo "==================="
echo ""

# Run unit tests only (fast)
echo "Running unit tests..."
NODE_OPTIONS=--experimental-vm-modules npm test -- tests/unit --silent

if [ $? -eq 0 ]; then
    echo "✅ All unit tests passed!"
else
    echo "❌ Some tests failed. Run 'npm test' for details."
    exit 1
fi
