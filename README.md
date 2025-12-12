# lineage_ops

Semantic Duplicate Detection and Consolidation for Databricks Unity Catalog

## Overview

`lineage_ops` is a tool designed to detect semantically duplicate tables within a Databricks Unity Catalog environment. It identifies tables that have different names but contain essentially the same data, enabling organizations to consolidate redundant tables into a single Source of Truth (SoT).

![plot](./main_iamge.png)


## Features

- **Lineage-based Analysis**: Uses Unity Catalog's system tables to find tables with common ancestors
- **Schema Embedding**: Converts table schemas (column names, types, comments) into vector representations
- **Similarity Search**: Identifies similar tables using cosine similarity and Locality Sensitive Hashing (LSH)
- **Clustering**: Groups related duplicate tables together
- **Consolidation Recommendations**: Generates actionable recommendations with estimated cost savings
- **Multiple Embedding Providers**: Supports Databricks Foundation Models, OpenAI, and simple hash-based embeddings

## Project Structure

```
lineage_ops/
├── src/
│   ├── lineage_ops/           # Main package
│   │   ├── data_extractor.py      # Extract lineage and schema info
│   │   ├── schema_embedder.py     # Generate schema embeddings
│   │   ├── similarity_analyzer.py # Compute similarities and clustering
│   │   ├── recommendation_generator.py # Generate consolidation recommendations
│   │   └── dedup_detector.py      # Main orchestrator
│   ├── notebook.ipynb         # Interactive analysis notebook
│   ├── dlt_pipeline.ipynb     # DLT pipeline example
│   └── prepare_data/          # Test data setup
│       ├── test_data_setup.ipynb  # Create test tables
│       └── test_data_lineage.sql  # Create lineage through pipelines
├── resources/
│   ├── lineage_ops.job.yml    # Databricks job definition
│   └── lineage_ops.pipeline.yml # DLT pipeline definition
├── tests/
│   └── main_test.py           # Unit tests
├── databricks.yml             # Databricks Asset Bundle config
├── requirements-dev.txt       # Development dependencies
└── setup.py                   # Package setup
```

## Installation

### Using Databricks Asset Bundles

```bash
# Deploy to Databricks workspace
databricks bundle deploy

# Run the analysis job
databricks bundle run lineage_ops_job
```

### Local Development

```bash
# Install dependencies
pip install -r requirements-dev.txt

# Install package in development mode
pip install -e src/
```

## Usage

### Basic Usage

```python
from lineage_ops.dedup_detector import SemanticDuplicateDetector, DedupConfig

# Create configuration
config = DedupConfig(
    catalog_filter="your_catalog",  # Target catalog to analyze
    days_back=30,                   # Lineage query period
    similarity_threshold=0.8,       # Minimum similarity score
    embedding_provider="databricks" # "databricks", "openai", or "simple"
)

# Initialize detector
detector = SemanticDuplicateDetector(spark, config)

# Run full analysis
recommendations_df = detector.run_full_analysis()

# Display results
display(recommendations_df)
```

### Step-by-Step Analysis

```python
from lineage_ops.data_extractor import LineageExtractor, SchemaExtractor
from lineage_ops.schema_embedder import SchemaEmbedder, DatabricksFoundationModelProvider
from lineage_ops.similarity_analyzer import SimilarityAnalyzer
from lineage_ops.recommendation_generator import RecommendationGenerator

# 1. Extract lineage information
lineage_extractor = LineageExtractor(spark)
lineage_df = lineage_extractor.get_table_lineage(days_back=30)
common_ancestors = lineage_extractor.find_common_ancestors(lineage_df)

# 2. Extract schema texts
schema_extractor = SchemaExtractor(spark)
schema_texts_df = schema_extractor.get_table_metadata(catalog_filter="your_catalog")

# 3. Generate embeddings
provider = DatabricksFoundationModelProvider(endpoint_name="databricks-bge-large-en")
embedder = SchemaEmbedder(spark, provider)
embeddings_df = embedder.embed_schema_texts(schema_texts_df)

# 4. Find similar tables
analyzer = SimilarityAnalyzer(spark)
similar_pairs = analyzer.compute_cosine_similarity(embeddings_df, threshold=0.8)

# 5. Generate recommendations
generator = RecommendationGenerator(spark)
recommendations = generator.generate_recommendations_batch(similar_pairs)
summary = generator.generate_summary_report(recommendations)
print(summary)
```

## Modules

### data_extractor.py

Extracts data from Unity Catalog system tables:
- `LineageExtractor`: Retrieves table lineage from `system.access.table_lineage`
- `SchemaExtractor`: Gets table/column metadata from `system.information_schema`

### schema_embedder.py

Converts schema text to vector embeddings:
- `DatabricksFoundationModelProvider`: Uses Databricks Foundation Models
- `OpenAIEmbeddingProvider`: Uses OpenAI API
- `SimpleHashEmbeddingProvider`: Hash-based embedding (no external API required)

### similarity_analyzer.py

Analyzes similarity between tables:
- Cosine similarity computation
- LSH-based approximate nearest neighbor search
- Clustering of similar tables

### recommendation_generator.py

Generates consolidation recommendations:
- Identifies Source of Truth (SoT) tables
- Estimates DBU and storage cost savings
- Produces human-readable recommendations

### dedup_detector.py

Main orchestrator that integrates all components for end-to-end analysis.

## Test Data Setup

The `src/prepare_data/` directory contains scripts to create test tables with lineage:

1. **test_data_setup.ipynb**: Creates bronze layer tables with sample data
2. **test_data_lineage.sql**: Creates silver, gold, legacy, and mart tables with intentional duplicates

### Test Table Structure

- **bronze**: 10 source tables (users, orders, products, etc.)
- **silver**: 10 cleaned tables derived from bronze
- **gold**: 5 aggregated business tables
- **legacy**: 15 duplicate tables (intentional duplicates for testing)
- **mart**: 5 additional data mart tables

## Configuration Options

| Parameter | Description | Default |
|-----------|-------------|---------|
| `catalog_filter` | Target catalog to analyze | `None` (all catalogs) |
| `days_back` | Number of days to look back for lineage | `30` |
| `similarity_threshold` | Minimum cosine similarity score | `0.8` |
| `embedding_provider` | Embedding provider type | `"databricks"` |
| `min_confidence` | Minimum confidence level | `"MEDIUM"` |

## Output

The analysis produces:

1. **Duplicate Candidate Pairs**: Tables identified as potential duplicates with similarity scores
2. **Consolidation Recommendations**: Suggested actions with SoT identification
3. **Cost Savings Estimate**: Projected DBU and storage savings from consolidation
4. **Summary Report**: High-level statistics and confidence distribution

## Requirements

- Databricks Runtime 13.3 LTS or higher
- Unity Catalog enabled
- Access to system tables (`system.access.table_lineage`, `system.information_schema`)
- For Databricks embeddings: Access to Foundation Model endpoints

## License

MIT License
