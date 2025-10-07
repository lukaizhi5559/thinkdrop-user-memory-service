# Quick Start Guide

## 🚀 Setup (5 minutes)

### 1. Install Dependencies
```bash
npm install
```

### 2. Configure Environment
```bash
cp .env.example .env
```

Edit `.env` and set your API key:
```bash
API_KEY=k7F9qLp3XzR2vH8sT1mN4bC0yW6uJ5eQG4tY9bH2wQ6nM1vS8xR3cL5pZ0kF7uDe
```

### 3. Initialize Database
```bash
npm run db:init
```

### 4. Verify Database Structure
```bash
npm run db:verify
```

**Expected Output:**
```
📋 Tables:
  ✓ memory
  ✓ memory_entities

📊 Memory Table Structure:
  - id: TEXT (PRIMARY KEY)
  - user_id: TEXT
  - source_text: TEXT
  - embedding: FLOAT[384]
  ...

📊 Memory Entities Table Structure:
  - id: TEXT (PRIMARY KEY)
  - memory_id: TEXT
  - entity: TEXT
  - type: TEXT
  - entity_type: TEXT
  - normalized_value: TEXT
  ...
```

### 5. Start Service
```bash
npm run dev
```

Service runs on: **http://localhost:3001**

---

## 🧪 Test the Service

### Health Check
```bash
curl http://localhost:3001/service.health | jq '.'
```

### Run MCP Tests
```bash
chmod +x scripts/test-mcp-curl.sh
./scripts/test-mcp-curl.sh
```

### Run Unit Tests
```bash
npm test
```

---

## 🔧 Database Management

### Migrate Existing Database
If you have an existing `user_memory.duckdb` without `memory_entities` table:

```bash
npm run db:migrate
```

### Verify Database
```bash
npm run db:verify
```

### Reinitialize Database (⚠️ Deletes all data)
```bash
rm -rf data/user_memory.duckdb*
npm run db:init
```

---

## 📝 Quick Test with curl

### Store a Memory
```bash
curl -X POST http://localhost:3001/memory.store \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer k7F9qLp3XzR2vH8sT1mN4bC0yW6uJ5eQG4tY9bH2wQ6nM1vS8xR3cL5pZ0kF7uDe" \
  -d '{
    "version": "mcp.v1",
    "service": "user-memory",
    "action": "memory.store",
    "requestId": "test-001",
    "context": {"userId": "test_user"},
    "payload": {
      "text": "Meeting with Dr. Smith tomorrow at 3pm",
      "entities": [
        {"type": "person", "value": "Dr. Smith", "entity_type": "PERSON"},
        {"type": "datetime", "value": "tomorrow at 3pm", "entity_type": "DATE"}
      ]
    }
  }' | jq '.'
```

### Search Memories
```bash
curl -X POST http://localhost:3001/memory.search \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer k7F9qLp3XzR2vH8sT1mN4bC0yW6uJ5eQG4tY9bH2wQ6nM1vS8xR3cL5pZ0kF7uDe" \
  -d '{
    "version": "mcp.v1",
    "service": "user-memory",
    "action": "memory.search",
    "requestId": "test-002",
    "context": {"userId": "test_user"},
    "payload": {
      "query": "doctor appointment",
      "limit": 10
    }
  }' | jq '.data.results'
```

---

## 🐛 Troubleshooting

### Issue: memory_entities table missing
**Solution:**
```bash
npm run db:migrate
npm run db:verify
```

### Issue: Port 3001 already in use
**Solution:**
```bash
lsof -ti:3001 | xargs kill -9
# Or change PORT in .env
```

### Issue: Embedding model won't load
**Solution:**
```bash
# Clear cache and reinstall
rm -rf node_modules
npm install
```

### Issue: Database locked
**Solution:**
```bash
# Stop all processes using the database
lsof data/user_memory.duckdb | awk '{print $2}' | tail -n +2 | xargs kill -9
```

---

## 📚 Next Steps

- Read [TESTING.md](./TESTING.md) for comprehensive testing guide
- Read [AGENT_SPEC.md](./AGENT_SPEC.md) for complete API documentation
- Run `./scripts/test-mcp-curl.sh` for full MCP protocol tests
- Check [README.md](./README.md) for deployment options

---

## ✅ Verification Checklist

- [ ] Dependencies installed (`npm install`)
- [ ] Environment configured (`.env` file)
- [ ] Database initialized (`npm run db:init`)
- [ ] Database verified (`npm run db:verify`)
  - [ ] `memory` table exists
  - [ ] `memory_entities` table exists
  - [ ] All indexes created
- [ ] Service starts (`npm run dev`)
- [ ] Health check passes (`curl http://localhost:3001/service.health`)
- [ ] MCP tests pass (`./scripts/test-mcp-curl.sh`)

**All checked?** 🎉 You're ready to integrate with Thinkdrop AI!
