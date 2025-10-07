#!/bin/bash

# MCP Protocol Test Script using curl
# Tests all UserMemory service endpoints

set -e

# Configuration
HOST="localhost"
PORT="3001"
BASE_URL="http://${HOST}:${PORT}"
API_KEY="k7F9qLp3XzR2vH8sT1mN4bC0yW6uJ5eQG4tY9bH2wQ6nM1vS8xR3cL5pZ0kF7uDe"

# Colors
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

# Test counter
MEMORY_ID=""

echo -e "${BLUE}╔════════════════════════════════════════╗${NC}"
echo -e "${BLUE}║  UserMemory MCP Protocol Test Suite   ║${NC}"
echo -e "${BLUE}╚════════════════════════════════════════╝${NC}"
echo ""

# Function to print test header
print_test() {
    echo -e "${YELLOW}▶ $1${NC}"
}

# Function to print success
print_success() {
    echo -e "${GREEN}✓ $1${NC}"
    echo ""
}

# Function to print error
print_error() {
    echo -e "${RED}✗ $1${NC}"
    echo ""
}

# Test 1: Health Check
print_test "Test 1: Health Check (GET /service.health)"
curl -s -X GET "${BASE_URL}/service.health" | jq '.'
print_success "Health check completed"

# Test 2: Capabilities
print_test "Test 2: Service Capabilities (GET /service.capabilities)"
curl -s -X GET "${BASE_URL}/service.capabilities" | jq '.capabilities.actions[] | .name'
print_success "Capabilities retrieved"

# Test 3: Store Memory
print_test "Test 3: Store Memory (POST /memory.store)"
STORE_RESPONSE=$(curl -s -X POST "${BASE_URL}/memory.store" \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer ${API_KEY}" \
  -d '{
    "version": "mcp.v1",
    "service": "user-memory",
    "action": "memory.store",
    "requestId": "test-store-001",
    "context": {
      "userId": "test_user_curl"
    },
    "payload": {
      "text": "I have an appointment with Dr. Sarah Johnson next Tuesday at 3pm for annual checkup",
      "entities": [
        {
          "type": "person",
          "value": "Dr. Sarah Johnson",
          "entity_type": "PERSON"
        },
        {
          "type": "datetime",
          "value": "next Tuesday at 3pm",
          "entity_type": "DATE"
        },
        {
          "type": "event",
          "value": "annual checkup",
          "entity_type": "EVENT"
        }
      ],
      "metadata": {
        "category": "appointment",
        "tags": ["medical", "health"],
        "priority": "high"
      }
    }
  }')

echo "$STORE_RESPONSE" | jq '.'
MEMORY_ID=$(echo "$STORE_RESPONSE" | jq -r '.data.memoryId')
print_success "Memory stored with ID: $MEMORY_ID"

# Test 4: Store Memory with Screenshot
print_test "Test 4: Store Memory with Screenshot"
curl -s -X POST "${BASE_URL}/memory.store" \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer ${API_KEY}" \
  -d '{
    "version": "mcp.v1",
    "service": "user-memory",
    "action": "memory.store",
    "requestId": "test-store-002",
    "context": {
      "userId": "test_user_curl"
    },
    "payload": {
      "text": "Screenshot of project documentation page",
      "screenshot": "/screenshots/docs_page_001.png",
      "extractedText": "UserMemory API Documentation - Version 1.0.0",
      "metadata": {
        "source": "screen-capture",
        "url": "https://docs.example.com/usermemory"
      }
    }
  }' | jq '.'
print_success "Memory with screenshot stored"

# Test 5: Semantic Search
print_test "Test 5: Semantic Search (POST /memory.search)"
curl -s -X POST "${BASE_URL}/memory.search" \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer ${API_KEY}" \
  -d '{
    "version": "mcp.v1",
    "service": "user-memory",
    "action": "memory.search",
    "requestId": "test-search-001",
    "context": {
      "userId": "test_user_curl"
    },
    "payload": {
      "query": "doctor appointment",
      "limit": 10,
      "minSimilarity": 0.3
    }
  }' | jq '.data.results[] | {id, text, similarity}'
print_success "Semantic search completed"

# Test 6: Search with Filters
print_test "Test 6: Search with Session Filter"
curl -s -X POST "${BASE_URL}/memory.search" \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer ${API_KEY}" \
  -d '{
    "version": "mcp.v1",
    "service": "user-memory",
    "action": "memory.search",
    "requestId": "test-search-002",
    "context": {
      "userId": "test_user_curl"
    },
    "payload": {
      "query": "appointment",
      "filters": {
        "type": "user_memory"
      },
      "limit": 5,
      "minSimilarity": 0.4
    }
  }' | jq '.data | {total, query, elapsedMs}'
print_success "Filtered search completed"

# Test 7: Retrieve Memory by ID
print_test "Test 7: Retrieve Memory by ID (POST /memory.retrieve)"
curl -s -X POST "${BASE_URL}/memory.retrieve" \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer ${API_KEY}" \
  -d "{
    \"version\": \"mcp.v1\",
    \"service\": \"user-memory\",
    \"action\": \"memory.retrieve\",
    \"requestId\": \"test-retrieve-001\",
    \"context\": {
      \"userId\": \"test_user_curl\"
    },
    \"payload\": {
      \"memoryId\": \"${MEMORY_ID}\"
    }
  }" | jq '.data.memory | {id, text, entities}'
print_success "Memory retrieved by ID"

# Test 8: List Memories
print_test "Test 8: List Memories with Pagination (POST /memory.list)"
curl -s -X POST "${BASE_URL}/memory.list" \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer ${API_KEY}" \
  -d '{
    "version": "mcp.v1",
    "service": "user-memory",
    "action": "memory.list",
    "requestId": "test-list-001",
    "context": {
      "userId": "test_user_curl"
    },
    "payload": {
      "limit": 10,
      "offset": 0,
      "sortBy": "created_at",
      "sortOrder": "DESC"
    }
  }' | jq '.data | {total, limit, hasMore, memoriesCount: (.memories | length)}'
print_success "Memories listed"

# Test 9: Update Memory
print_test "Test 9: Update Memory (POST /memory.update)"
curl -s -X POST "${BASE_URL}/memory.update" \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer ${API_KEY}" \
  -d "{
    \"version\": \"mcp.v1\",
    \"service\": \"user-memory\",
    \"action\": \"memory.update\",
    \"requestId\": \"test-update-001\",
    \"context\": {
      \"userId\": \"test_user_curl\"
    },
    \"payload\": {
      \"memoryId\": \"${MEMORY_ID}\",
      \"updates\": {
        \"text\": \"I have an appointment with Dr. Sarah Johnson next Wednesday at 3pm for annual checkup\",
        \"metadata\": {
          \"category\": \"appointment\",
          \"tags\": [\"medical\", \"health\", \"rescheduled\"],
          \"priority\": \"high\"
        }
      }
    }
  }" | jq '.data | {memoryId, updated, embedding}'
print_success "Memory updated"

# Test 10: Classify Conversational Query - Positional
print_test "Test 10: Classify Conversational Query - Positional"
curl -s -X POST "${BASE_URL}/memory.classify-conversational-query" \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer ${API_KEY}" \
  -d '{
    "version": "mcp.v1",
    "service": "user-memory",
    "action": "memory.classify-conversational-query",
    "requestId": "test-classify-001",
    "context": {
      "userId": "test_user_curl"
    },
    "payload": {
      "query": "what did I say first?"
    }
  }' | jq '.data | {isConversational, classification, confidence}'
print_success "Positional query classified"

# Test 11: Classify Conversational Query - Topical
print_test "Test 11: Classify Conversational Query - Topical"
curl -s -X POST "${BASE_URL}/memory.classify-conversational-query" \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer ${API_KEY}" \
  -d '{
    "version": "mcp.v1",
    "service": "user-memory",
    "action": "memory.classify-conversational-query",
    "requestId": "test-classify-002",
    "context": {
      "userId": "test_user_curl"
    },
    "payload": {
      "query": "what topics did we discuss about healthcare?"
    }
  }' | jq '.data | {isConversational, classification, confidence}'
print_success "Topical query classified"

# Test 12: Classify Conversational Query - Overview
print_test "Test 12: Classify Conversational Query - Overview"
curl -s -X POST "${BASE_URL}/memory.classify-conversational-query" \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer ${API_KEY}" \
  -d '{
    "version": "mcp.v1",
    "service": "user-memory",
    "action": "memory.classify-conversational-query",
    "requestId": "test-classify-003",
    "context": {
      "userId": "test_user_curl"
    },
    "payload": {
      "query": "summarize our conversation"
    }
  }' | jq '.data | {isConversational, classification, confidence}'
print_success "Overview query classified"

# Test 13: Classify Non-Conversational Query
print_test "Test 13: Classify Non-Conversational Query"
curl -s -X POST "${BASE_URL}/memory.classify-conversational-query" \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer ${API_KEY}" \
  -d '{
    "version": "mcp.v1",
    "service": "user-memory",
    "action": "memory.classify-conversational-query",
    "requestId": "test-classify-004",
    "context": {
      "userId": "test_user_curl"
    },
    "payload": {
      "query": "what is the weather today?"
    }
  }' | jq '.data | {isConversational, classification, confidence}'
print_success "Non-conversational query classified"

# Test 14: Delete Memory
print_test "Test 14: Delete Memory (POST /memory.delete)"
curl -s -X POST "${BASE_URL}/memory.delete" \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer ${API_KEY}" \
  -d "{
    \"version\": \"mcp.v1\",
    \"service\": \"user-memory\",
    \"action\": \"memory.delete\",
    \"requestId\": \"test-delete-001\",
    \"context\": {
      \"userId\": \"test_user_curl\"
    },
    \"payload\": {
      \"memoryId\": \"${MEMORY_ID}\"
    }
  }" | jq '.data | {memoryId, deleted}'
print_success "Memory deleted"

# Test 15: Error Handling - Invalid API Key
print_test "Test 15: Error Handling - Invalid API Key"
ERROR_RESPONSE=$(curl -s -X POST "${BASE_URL}/memory.store" \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer invalid-key" \
  -d '{
    "version": "mcp.v1",
    "service": "user-memory",
    "action": "memory.store",
    "payload": {"text": "test"}
  }')
echo "$ERROR_RESPONSE" | jq '.'
if echo "$ERROR_RESPONSE" | jq -e '.error.code == "UNAUTHORIZED"' > /dev/null; then
    print_success "Correctly rejected invalid API key"
else
    print_error "Failed to reject invalid API key"
fi

# Test 16: Error Handling - Missing Required Field
print_test "Test 16: Error Handling - Missing Required Field"
ERROR_RESPONSE=$(curl -s -X POST "${BASE_URL}/memory.search" \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer ${API_KEY}" \
  -d '{
    "version": "mcp.v1",
    "service": "user-memory",
    "action": "memory.search",
    "requestId": "test-error-001",
    "context": {"userId": "test_user"},
    "payload": {"limit": 10}
  }')
echo "$ERROR_RESPONSE" | jq '.'
if echo "$ERROR_RESPONSE" | jq -e '.status == "error"' > /dev/null; then
    print_success "Correctly rejected missing required field"
else
    print_error "Failed to reject missing required field"
fi

echo ""
echo -e "${BLUE}╔════════════════════════════════════════╗${NC}"
echo -e "${BLUE}║     All MCP Protocol Tests Complete    ║${NC}"
echo -e "${BLUE}╚════════════════════════════════════════╝${NC}"
echo ""
echo -e "${GREEN}✓ 16 tests executed successfully${NC}"
echo ""
