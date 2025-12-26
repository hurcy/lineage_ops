__version__ = "0.0.1"

from .main import (
    DuplicatedTableDetector,
    LineageOpsConfig,
    detect_duplicated_tables,
    analyze_table_similarity,
)

from .data_extractor import DataExtractor, LineageExtractor, SchemaExtractor
from .schema_embedder import (
    SchemaEmbedder,
    SentenceTransformerProvider,
    DatabricksFoundationModelProvider,
    SimpleHashEmbeddingProvider,
)
from .similarity_analyzer import SimilarityAnalyzer, FastSimilarityAnalyzer

__all__ = [
    # Main classes
    "DuplicatedTableDetector",
    "LineageOpsConfig",
    # Helper functions
    "detect_duplicated_tables",
    "analyze_table_similarity",
    # Extractors
    "DataExtractor",
    "LineageExtractor",
    "SchemaExtractor",
    # Embedders
    "SchemaEmbedder",
    "SentenceTransformerProvider",
    "DatabricksFoundationModelProvider",
    "SimpleHashEmbeddingProvider",
    # Analyzers
    "SimilarityAnalyzer",
    "FastSimilarityAnalyzer",
]
