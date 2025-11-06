# Fund Performance Analysis System

An AI-powered fund performance analysis system that enables Limited Partners (LPs) to automatically process fund performance PDF documents and query fund metrics using natural language.

![Fund Analysis Chat](files/screenshot/2025-11-06%20082754.png)

## Table of Contents

- [Overview](#overview)
- [Tech Stack](#tech-stack)
- [Features](#features)
- [Setup Instructions](#setup-instructions)
- [Environment Variables](#environment-variables)
- [API Testing Examples](#api-testing-examples)
- [Screenshots](#screenshots)
- [Known Limitations](#known-limitations)
- [Future Improvements](#future-improvements)

## Overview

This system solves the problem of manually analyzing quarterly fund performance reports. Limited Partners receive PDF documents containing capital calls, distributions, and performance metrics. This system:

1. **Automatically processes PDF documents** - Extracts tables (capital calls, distributions, adjustments) and text content
2. **Stores structured data** - Tables go to PostgreSQL, text goes to FAISS vector store
3. **Enables natural language queries** - Ask questions like "What is the current DPI?" or "Explain Paid-In Capital"
4. **Provides accurate answers** - Powered by RAG (Retrieval Augmented Generation) with context-aware conversation

### Key Capabilities

- **Automatic Fund Creation**: PDFs are analyzed to create fund entries automatically
- **Intelligent Metrics Calculation**: DPI, IRR, TVPI, RVPI, MOIC, NAV calculated from transaction data
- **Multi-Fund Comparison**: Compare performance metrics across different funds
- **Context-Aware Chat**: Remembers fund context across conversation turns
- **Semantic Search**: FAISS-powered vector search for relevant document sections

## Tech Stack

### Backend
- **FastAPI** - High-performance Python web framework
- **PostgreSQL** - Relational database for structured data (transactions, funds)
- **FAISS** - Vector database for semantic search
- **Ollama** - Local LLM for embeddings and chat responses
- **Redis** - Caching and session management
- **Celery** - Background task processing
- **SQLAlchemy** - ORM for database operations
- **pdfplumber** - PDF parsing and table extraction

### Frontend
- **Next.js 14** - React framework with App Router
- **TypeScript** - Type-safe development
- **TailwindCSS** - Utility-first CSS framework
- **React Query** - Data fetching and caching
- **Recharts** - Data visualization

### Infrastructure
- **Docker & Docker Compose** - Containerization
- **Nginx** - Reverse proxy and static file serving

## Features

### 1. Document Processing Pipeline
- ✅ PDF upload and automatic processing
- ✅ Table extraction (capital calls, distributions, adjustments)
- ✅ Intelligent table classification using pattern matching
- ✅ Automatic fund creation from PDF metadata
- ✅ Background processing with Celery workers
- ✅ Data validation and cleaning

### 2. Vector Store & RAG System
- ✅ Text chunking with overlap for context preservation
- ✅ Local embeddings using Ollama (nomic-embed-text)
- ✅ FAISS index for fast semantic search
- ✅ Hybrid search (vector + metadata filtering)
- ✅ Context retrieval with source attribution

### 3. Query Engine & Chat System
- ✅ Intent classification (definition vs. calculation vs. retrieval)
- ✅ Context-aware conversation history
- ✅ Fund context extraction across conversation turns
- ✅ Metrics included in conversation history
- ✅ Multi-layer search fallback strategy

### 4. Metrics Calculation
- ✅ **DPI** (Distributions to Paid-In Capital)
- ✅ **IRR** (Internal Rate of Return)
- ✅ **TVPI** (Total Value to Paid-In Capital)
- ✅ **RVPI** (Residual Value to Paid-In Capital)
- ✅ **MOIC** (Multiple on Invested Capital)
- ✅ **NAV** (Net Asset Value)
- ✅ **PIC** (Paid-In Capital with adjustments)

### 5. User Interface
- ✅ Fund dashboard with performance charts
- ✅ Multi-fund comparison table
- ✅ Interactive chat interface with conversation history
- ✅ Document search with highlighting
- ✅ Upload interface with progress tracking
- ✅ Responsive design (mobile-friendly)

### 6. Performance & Optimization
- ✅ Response caching with Redis
- ✅ Gzip compression middleware
- ✅ Rate limiting
- ✅ Database query optimization
- ✅ Pagination for large datasets

## Setup Instructions

### Prerequisites

- Docker and Docker Compose installed
- At least 4GB RAM available
- Ports 3000, 8000, 5432, 6379, 11434 available

### Quick Start (Docker)

1. **Clone the repository**
   ```bash
   git clone <repository-url>
   cd coding-test-3rd
   ```

2. **Configure environment variables**
   ```bash
   cp .env.example .env
   # Edit .env with your settings (Ollama will be used by default)
   ```

3. **Start all services**
   ```bash
   docker-compose up -d
   ```

4. **Wait for Ollama to download models** (first time only)
   ```bash
   docker-compose logs -f backend
   # Wait for "Ollama models ready" message
   ```

5. **Initialize the database**
   ```bash
   docker-compose exec backend python -m app.db.init_db
   ```

6. **Access the application**
   - Frontend: http://localhost:3000
   - Backend API: http://localhost:8000
   - API Docs: http://localhost:8000/docs

### Development Setup (Without Docker)

See [SETUP.md](SETUP.md) for detailed local development instructions.

## Environment Variables

### Required Variables

Create a `.env` file in the root directory:

```bash
# LLM Configuration (Ollama - Local)
OLLAMA_BASE_URL=http://ollama:11434
OLLAMA_MODEL=llama3.2
OLLAMA_EMBEDDING_MODEL=nomic-embed-text

# Database (configured in docker-compose.yml)
DATABASE_URL=postgresql://funduser:fundpass@postgres:5432/funddb

# Redis (configured in docker-compose.yml)
REDIS_URL=redis://redis:6379/0
```

### Optional Variables

```bash
# OpenAI (if you prefer to use OpenAI instead of Ollama)
OPENAI_API_KEY=sk-your-api-key-here
OPENAI_MODEL=gpt-4-turbo-preview
OPENAI_EMBEDDING_MODEL=text-embedding-3-small

# Anthropic (alternative LLM provider)
ANTHROPIC_API_KEY=your-anthropic-key-here
```

## API Testing Examples

### 1. Upload a Fund Document

```bash
curl -X POST "http://localhost:8000/api/documents/upload" \
  -F "file=@files/fund_reports/Velocity_Ventures_Fund.pdf"
```

**Response:**
```json
{
  "document_id": 1,
  "filename": "Velocity_Ventures_Fund.pdf",
  "status": "processing",
  "message": "Document uploaded and processing started"
}
```

### 2. Search Documents

```bash
curl -X POST "http://localhost:8000/api/search/" \
  -H "Content-Type: application/json" \
  -d '{
    "query": "capital calls in 2024",
    "k": 5
  }'
```

**Response:**
```json
{
  "results": [
    {
      "content": "Capital Call - Q1 2024: $25,000,000...",
      "metadata": {
        "document_name": "Velocity_Ventures_Fund.pdf",
        "fund_name": "Velocity Ventures Fund",
        "chunk_index": 3
      },
      "score": 0.89
    }
  ]
}
```

### 3. Chat Query

```bash
curl -X POST "http://localhost:8000/api/chat/query" \
  -H "Content-Type: application/json" \
  -d '{
    "query": "What is the current DPI for Velocity Ventures Fund?",
    "conversation_id": "abc-123"
  }'
```

**Response:**
```json
{
  "answer": "The current DPI (Distributions to Paid-In Capital) for Velocity Ventures Fund is **0.40**...",
  "sources": [
    {
      "content": "Total distributions: $40,000,000...",
      "metadata": {...}
    }
  ],
  "metrics": {
    "paid_in_capital": 100000000,
    "distributed_capital": 40000000,
    "dpi": 0.4,
    "irr": 15.2,
    "tvpi": 1.6
  }
}
```

### 4. Get Fund Metrics

```bash
curl "http://localhost:8000/api/funds/1/metrics"
```

**Response:**
```json
{
  "fund_id": 1,
  "fund_name": "Velocity Ventures Fund",
  "metrics": {
    "paid_in_capital": 100000000,
    "distributed_capital": 40000000,
    "dpi": 0.4,
    "irr": 15.2,
    "tvpi": 1.6,
    "rvpi": 1.2,
    "moic": 1.6,
    "nav": 120000000
  }
}
```

### 5. Compare Multiple Funds

```bash
curl -X POST "http://localhost:8000/api/funds/compare" \
  -H "Content-Type: application/json" \
  -d '{
    "fund_ids": [1, 2, 3]
  }'
```

## Screenshots

### 1. Chat Interface with Context Awareness
![Chat Interface](files/screenshot/2025-11-06%20082754.png)
*Natural language queries with conversation history and context awareness*

### 2. Multi-Fund Comparison
![Fund Comparison](files/screenshot/2025-11-06%20072822.png)
*Compare performance metrics across multiple funds*

### 3. Fund Detail Page with Charts
![Fund Details](files/screenshot/2025-11-06%20075714.png)
*Detailed fund performance with visual charts and transaction history*

### 4. Document Search
![Search Interface](files/screenshot/2025-11-06%20072545.png)
*Semantic search across all fund documents with source highlighting*

### 5. Conversation History
![Conversation Sidebar](files/screenshot/2025-11-06%20083506.png)
*Persistent conversation history with automatic title generation*

## Known Limitations

### 1. PDF Processing
- **Table Detection**: Works best with structured tables. Complex or nested tables may require manual review
- **Multi-page Tables**: Tables spanning multiple pages may be split into separate entries
- **Handwritten Content**: Cannot process handwritten or image-based PDFs (needs OCR)

### 2. Metrics Calculation
- **IRR Calculation**: Requires at least one cash inflow and one outflow. Returns None if insufficient data
- **RVPI Calculation**: May show 0 for funds that have fully distributed (when NAV = 0)
- **Date Parsing**: Assumes dates are in consistent formats (MM/DD/YYYY or YYYY-MM-DD)

### 3. Search & RAG
- **Vector Search Accuracy**: Depends on embedding model quality (local Ollama vs cloud embeddings)
- **Context Window**: Limited to last 10 conversation messages to avoid token limits
- **Fund Name Matching**: Case-insensitive but requires exact name matches (no fuzzy matching)

### 4. Performance
- **First Query**: Slower on first run due to model loading (Ollama warm-up)
- **Large PDFs**: Documents >50 pages may take 30-60 seconds to process
- **Concurrent Uploads**: Limited to 1 document processing at a time (Celery single worker)

### 5. UI/UX
- **Real-time Updates**: Document processing status requires manual refresh
- **Mobile Experience**: Chat interface works but comparison tables require horizontal scrolling
- **Error Messages**: Some backend errors show generic messages instead of user-friendly explanations

## Future Improvements

1. **Enhanced PDF Processing**
   - OCR support for image-based PDFs
   - Better multi-page table handling
   - Automatic table header detection
   - Support for more date formats

2. **Improved Search**
   - Fuzzy fund name matching
   - Multi-fund query support ("Compare DPI for all tech funds")
   - Date range filtering
   - Advanced filters (fund type, vintage year, etc.)

3. **UI Enhancements**
   - Real-time document processing status (WebSocket)
   - Bulk upload support
   - Export functionality (CSV, Excel)
   - Dark mode

4. **Advanced Analytics**
   - Fund performance benchmarking
   - Peer group analysis
   - Trend visualization over time
   - Quartile rankings

5. **Collaboration Features**
   - User authentication and authorization
   - Share conversations and reports
   - Comments and annotations on documents
   - Team workspaces

6. **Data Quality**
   - Automatic anomaly detection in metrics
   - Data validation rules engine
   - Audit trail for all calculations
   - Manual override capabilities

7. **AI Enhancements**
   - Fine-tuned models for PE/VC domain
   - Predictive analytics (projected IRR, etc.)
   - Automatic investment memo generation
   - Smart alerts for significant changes

8. **Integration & APIs**
   - REST API for third-party integrations
   - Webhook support for automation
   - Export to portfolio management systems
   - Integration with DocuSign, Carta, etc.

9. **Enterprise Features**
   - Multi-tenancy support
   - SSO/SAML authentication
   - Advanced security (encryption at rest)
   - Compliance reporting (SOC2, GDPR)

---

## Contributing

Contributions are welcome! Please read [ASSIGNMENT.md](ASSIGNMENT.md) for the original project requirements.

## License

This project is part of a coding challenge. See [ASSIGNMENT.md](ASSIGNMENT.md) for details.

## Support

For issues or questions, please create an issue in the repository.
