# UserMemory Microservice

MCP-compliant memory service for Thinkdrop AI with semantic search, entity extraction, and conversational context awareness.

## Quick Start

### Installation

```bash
npm install
```

### Configuration

```bash
cp .env.example .env
# Edit .env with your configuration
```

### Initialize Database

```bash
npm run db:init
```

### Verify Database Structure

```bash
npm run db:verify
```

**Important:** Ensure `memory_entities` table exists for entity-based search. See [DATABASE_FIX.md](./DATABASE_FIX.md) if missing.

### Development

```bash
npm run dev
```

### Production

```bash
npm start
```

### Docker

```bash
docker-compose up -d
```

## API Endpoints

### Health Check
```
GET /service.health
```

### Capabilities
```
GET /service.capabilities
```

### MCP Actions

All actions use POST with MCP envelope:

- `POST /memory.store` - Store new memory
- `POST /memory.search` - Semantic search
- `POST /memory.retrieve` - Get memory by ID
- `POST /memory.update` - Update memory
- `POST /memory.delete` - Delete memory
- `POST /memory.list` - List memories
- `POST /memory.classify-conversational-query` - Classify query type

## Example Request

```bash
curl -X POST http://localhost:3001/memory.store \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer your-api-key" \
  -d '{
    "version": "mcp.v1",
    "service": "user-memory",
    "action": "memory.store",
    "requestId": "req-123",
    "context": {
      "userId": "user_abc"
    },
    "payload": {
      "text": "Meeting with John tomorrow at 3pm",
      "entities": [
        {"type": "person", "value": "John"},
        {"type": "datetime", "value": "tomorrow at 3pm"}
      ]
    }
  }'
```

## Features

- ✅ Semantic search with embeddings (all-MiniLM-L6-v2)
- ✅ Entity extraction and indexing
- ✅ Cross-session memory retrieval
- ✅ Screenshot and OCR support
- ✅ Conversational query classification
- ✅ MCP protocol v1 compliant
- ✅ Docker support
- ✅ Health monitoring and metrics

## Documentation

See [AGENT_SPEC.md](./AGENT_SPEC.md) for complete API documentation and implementation details.

## License

MIT
