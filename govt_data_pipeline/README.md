# Government Data Pipeline

A comprehensive system for scraping, processing, summarizing, and analyzing government documents with advanced AI integration and knowledge graph capabilities.

## System Architecture

![Architecture Diagram](https://mermaid.ink/img/pako:eNp1kk9PwzAMxb9KlBNI7dRfadMd9sNhHJAQBw6-JF6JljYiyYTG-O7YabcJDXpJnPfs92zHB2GsRZGLFsseO8WbyjVgfvSQHlE36Db73GOFJnP8TK2paa4rPRfwUGuv29TAIwx2dI3Vrtd2_GvrGjDG2t4sZM72QHsM6Hw4z9v2YAfs-2hNlxJetkMfuUiIE0yElhXtMcLjhcVx4vqeomfQ89oO2YlHE8k-3gf6_cYGOX1P1OQ4n0eewvUYrgGGEqMiDd_pU47LVqOm4KlmxuPJkHjnZ6PY6Dv0nkydImeqoUDRswzXU-UbVGQdjsHBZzZdrUDkeGDrODFc6a3QHlsUXHMnzuFUjppA8W1G0zg0KT-xexQFO9QJLlluUBww8cYG0I-A6NXDnzhD3r7AoQq_MQvLSKI4GxrpF02q3jSmnLXGXRzpZ73TUDTQaOKTcazYsUXBN4_HXdDw-geVvsvB?type=png)

The system consists of three main processing stages:

1. **Scraping Layer**: Extracts content from government websites
2. **Processing Layer**: Analyzes and structures the data with AI
3. **Storage Layer**: Stores content across specialized databases
4. **Search Layer**: Provides intelligent retrieval capabilities

### Data Flow

```
Government Websites → Scraping → Document Extraction → AI Processing → 
                                    ↓
                      ┌─────────────┼─────────────────┐
                      ↓             ↓                 ↓
                   Supabase      Pinecone           Neo4j
                 (Documents)     (Vectors)     (Knowledge Graph)
                      ↑             ↑                 ↑
                      └─────────────┼─────────────────┘
                                    ↓
                              Hybrid Search
```

## Core Components

The `core.py` module provides the foundation of the pipeline with these key classes:

### 1. ScraperAdapter

Handles browser automation for content extraction:
- Uses `AirflowWebScraper` under the hood for JavaScript rendering
- Provides methods for extracting document links and content
- Manages browser lifecycle to prevent resource leaks

### 2. Content Extraction

Two specialized extractors:
- `XPathExtractor`: Uses XPath expressions for precise HTML targeting
- `CSSExtractor`: Uses CSS selectors for more flexible extraction

### 3. Document Representation

The `Document` class centrally manages:
- Metadata (URL, title, source)
- Content extraction and storage
- Processing status tracking
- Conversion between internal and database representations

### 4. Supabase Integration

`SupabaseManager` handles all database operations:
- Document storage and retrieval
- Source and subsource tracking
- Document status updates
- Full-text search capabilities
- Batch operations for efficiency

## Enhanced Processor

The `enhanced_processor.py` module contains the `EnhancedProcessor` class, which handles AI-powered document processing:

### AI Model Integration

- **Text Generation**: Claude 3 Haiku (Anthropic)
  - Used for: Intelligent document summarization and entity extraction
  - Implementation: `langchain_anthropic.ChatAnthropic`
  - Key feature: Map-reduce processing for long documents

- **Vector Embeddings**: text-embedding-3-small (OpenAI)
  - Used for: Semantic search capabilities
  - Implementation: `langchain_openai.OpenAIEmbeddings`
  - Dimension: 1536

### Processing Capabilities

1. **Smart Summarization**:
   - Automatically adjusts approach based on document length
   - Short documents: Direct single-prompt summarization
   - Long documents: Map-reduce chunking for better handling
   - Output format: Structured with TITLE, FACTS, SENTIMENT, and TAGS

2. **Entity Extraction**:
   - Identifies organizations, people, programs, policies from text
   - Creates standardized canonical names for entity resolution
   - Extracts relationships between entities
   - Filters out generic concepts, dates, and numerical values

3. **Vector Embeddings**:
   - Creates embeddings of document summaries
   - Stores vectors in Pinecone with document metadata
   - Enables semantic similarity search

4. **Knowledge Graph Integration**:
   - Creates entity nodes from extracted entities
   - Establishes relationships between entities
   - Links documents to entities through mentions
   - Enables graph-based document discovery

## Knowledge Graph Manager

The `knowledge_graph_manager.py` module handles Neo4j graph database operations:

- Entity storage and resolution
- Relationship mapping between entities
- Document-entity connections
- Graph traversal for related document discovery

## Optimized Hybrid Search

The `hybrid-search-optimized.py` script implements the hybrid search approach:

### Search Components

1. **Vector Search** (Pinecone):
   - Performs semantic similarity matching
   - Finds documents with similar meaning regardless of wording
   - Uses OpenAI embeddings for high-quality semantic representation

2. **Knowledge Graph Search** (Neo4j):
   - Entity-based document discovery
   - Follows relationships between entities
   - Can discover documents through indirect connections
   - Provides explanatory context for search results

3. **Metadata Retrieval** (Supabase):
   - Single source of truth for document content
   - Provides full summaries for search results
   - Ensures consistent document representation

### Search Process

1. Extract entities from the search query using Claude
2. Perform vector search in Pinecone
3. Perform knowledge graph search in Neo4j
4. Merge results with configurable weighting
5. Fetch full document details from Supabase
6. Return combined results with context explanation

### Result Merging Strategies

- **Weighted**: Combines vector and graph scores with configurable weights
- **Interleaved**: Alternates between vector and graph results
- **Separate**: Keeps results from different sources distinct

## Tech Stack Summary

### AI Models
- **Claude 3 Haiku** (Anthropic): Summarization, entity extraction
- **text-embedding-3-small** (OpenAI): Vector embeddings for semantic search

### Databases
- **Supabase**: Document storage, metadata, full-text search
- **Pinecone**: Vector database for semantic search
- **Neo4j**: Graph database for entity relationships

### Key Libraries
- **LangChain**: AI orchestration for document processing
- **Selenium**: Browser automation for scraping
- **BeautifulSoup4/lxml**: HTML parsing and extraction

## Setup and Maintenance

### Environment Variables

Required API keys and connection details:
```
# Supabase credentials
SUPABASE_URL=https://your-project-id.supabase.co
SUPABASE_KEY=your-service-role-key

# AI model API keys
ANTHROPIC_API_KEY=your-anthropic-api-key
OPENAI_API_KEY=your-openai-api-key

# Pinecone configuration
PINECONE_API_KEY=your-pinecone-api-key
PINECONE_INDEX_NAME=govt-scrape-index
PINECONE_NAMESPACE=govt-content

# Neo4j configuration
NEO4J_URI=neo4j+s://your-instance-id.databases.neo4j.io
NEO4J_USERNAME=neo4j
NEO4J_PASSWORD=your-password
```

### Database Integrity

Key validation scripts:
- `neo4j-graph-test.py`: Tests knowledge graph structure and contents
- `pinecone-document-gap-analyzer.py`: Validates vector-document alignment
- `hybrid-search-optimized.py`: Tests complete search functionality

### Common Maintenance Tasks

1. **Reprocessing documents**:
   ```bash
   python runner.py --process-only
   ```

2. **Creating missing vectors**:
   ```bash
   python pinecone-document-gap-analyzer.py --create
   ```

3. **Exploring the knowledge graph**:
   ```bash
   python neo4j-graph-viz.py --summary
   ```

4. **Testing search functionality**:
   ```bash
   python hybrid-search-optimized.py "your search query"
   ```

## Performance Considerations

- **Vector Search**: Most efficient for similarity matching but lacks context
- **Graph Search**: Provides contextual connections but slower for large-scale retrieval
- **Hybrid Approach**: Best of both worlds with balanced trade-offs

## Potential Enhancements

1. **Temporal Modeling**: Add time-based relationships to track changing policies
2. **Multi-Document Summaries**: Create aggregated views across multiple sources
3. **User Feedback Loop**: Capture search relevance feedback to improve results
4. **Real-time Alerts**: Monitor for new content matching specific criteria
5. **API Layer**: Provide programmatic access to the pipeline's capabilities