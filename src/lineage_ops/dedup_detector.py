"""
Semantic Duplicate Detection Main Orchestrator

Integrates all modules to perform end-to-end duplicate detection.
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from typing import Dict, List, Optional, Any
from dataclasses import dataclass

from .data_extractor import LineageExtractor, SchemaExtractor
from .schema_embedder import (
    SchemaEmbedder, 
    EmbeddingProvider,
    DatabricksFoundationModelProvider,
    SimpleHashEmbeddingProvider
)
from .similarity_analyzer import SimilarityAnalyzer, DuplicateCandidateFinder
from .recommendation_generator import RecommendationGenerator


@dataclass
class LineageOpsConfig:
    """Duplicate detection configuration."""
    catalog_filter: Optional[str] = None
    days_back: int = 30
    similarity_threshold: float = 0.8
    ancestor_overlap_threshold: float = 0.5
    use_lsh: bool = True
    embedding_provider: str = "databricks"  # "databricks", "openai", "simple"
    embedding_endpoint: Optional[str] = None
    api_key: Optional[str] = None


class SemanticDuplicateDetector:
    """Class for detecting semantic duplicate tables and generating consolidation recommendations."""
    
    def __init__(
        self, 
        spark: SparkSession,
        config: Optional[LineageOpsConfig] = None
    ):
        """
        Initialize SemanticDuplicateDetector.
        
        Args:
            spark: SparkSession instance
            config: Duplicate detection configuration
        """
        self.spark = spark
        self.config = config or LineageOpsConfig()
        
        # Initialize components
        self.lineage_extractor = LineageExtractor(
            spark, 
            catalog_filter=self.config.catalog_filter
        )
        self.schema_extractor = SchemaExtractor(spark)
        self.embedding_provider = self._create_embedding_provider()
        self.schema_embedder = SchemaEmbedder(spark, self.embedding_provider)
        self.similarity_analyzer = SimilarityAnalyzer(spark)
        self.candidate_finder = DuplicateCandidateFinder(spark)
        self.recommendation_generator = RecommendationGenerator(spark)
        
        # Store intermediate results
        self._lineage_df: Optional[DataFrame] = None
        self._common_ancestors_df: Optional[DataFrame] = None
        self._schema_texts_df: Optional[DataFrame] = None
        self._embeddings_df: Optional[DataFrame] = None
        self._candidates_df: Optional[DataFrame] = None
        self._recommendations_df: Optional[DataFrame] = None
    
    def _create_embedding_provider(self) -> EmbeddingProvider:
        """Create appropriate embedding provider based on configuration."""
        provider_type = self.config.embedding_provider.lower()
        
        if provider_type == "databricks":
            return DatabricksFoundationModelProvider(
                endpoint_name=self.config.embedding_endpoint or "databricks-bge-large-en"
            )
        elif provider_type == "openai":
            if not self.config.api_key:
                raise ValueError("OpenAI provider requires api_key in config")
            from .schema_embedder import OpenAIEmbeddingProvider
            return OpenAIEmbeddingProvider(
                api_key=self.config.api_key,
                base_url=self.config.embedding_endpoint
            )
        elif provider_type == "simple":
            return SimpleHashEmbeddingProvider()
        else:
            raise ValueError(f"Unknown embedding provider: {provider_type}")
    
    def extract_lineage(self) -> DataFrame:
        """
        Extract lineage information.
        
        Returns:
            DataFrame: Table lineage information
        """
        self._lineage_df = self.lineage_extractor.get_table_lineage(
            days_back=self.config.days_back
        )
        return self._lineage_df
    
    def find_common_ancestors(self) -> DataFrame:
        """
        Find table pairs with common ancestors.
        
        Returns:
            DataFrame: Common ancestor information
        """
        if self._lineage_df is None:
            self.extract_lineage()
        
        self._common_ancestors_df = self.lineage_extractor.find_common_ancestors(
            self._lineage_df
        )
        return self._common_ancestors_df
    
    def extract_schema_texts(self, table_names: Optional[List[str]] = None) -> DataFrame:
        """
        Extract table schema texts.
        
        Args:
            table_names: List of table names to analyze (None for auto-detection)
            
        Returns:
            DataFrame: Schema texts
        """
        if table_names is None:
            # Use tables discovered from common ancestor analysis
            if self._common_ancestors_df is None:
                self.find_common_ancestors()
            
            # Collect both columns in one action instead of two separate collects
            table_pairs = self._common_ancestors_df.select("table_a", "table_b").distinct().collect()
            table_names = list(set(
                [row["table_a"] for row in table_pairs] + 
                [row["table_b"] for row in table_pairs]
            ))
            print(f"Tables to analyze: {len(table_names)}, sample: {table_names[:10]}")
        self._schema_texts_df = self.schema_extractor.get_bulk_schema_texts(table_names)
        return self._schema_texts_df
    
    def generate_embeddings(self) -> DataFrame:
        """
        Generate schema embeddings.
        
        Returns:
            DataFrame: Schema embeddings
        """
        if self._schema_texts_df is None:
            self.extract_schema_texts()
        
        self._embeddings_df = self.schema_embedder.embed_schema_texts(
            self._schema_texts_df
        )
        return self._embeddings_df
    
    def find_duplicate_candidates(self) -> DataFrame:
        """
        Find duplicate candidates.
        
        Returns:
            DataFrame: Duplicate candidates
        """
        if self._embeddings_df is None:
            self.generate_embeddings()
        
        if self._common_ancestors_df is None:
            self.find_common_ancestors()
        
        self._candidates_df = self.candidate_finder.find_candidates(
            self._embeddings_df,
            self._common_ancestors_df,
            similarity_threshold=self.config.similarity_threshold,
            ancestor_overlap_threshold=self.config.ancestor_overlap_threshold
        )
        return self._candidates_df
    
    def generate_recommendations(self) -> DataFrame:
        """
        Generate consolidation recommendations.
        
        Returns:
            DataFrame: Consolidation recommendations
        """
        if self._candidates_df is None:
            self.find_duplicate_candidates()
        
        self._recommendations_df = self.recommendation_generator.generate_recommendations_batch(
            self._candidates_df
        )
        return self._recommendations_df
    
    def run_full_analysis(self) -> Dict[str, Any]:
        """
        Run the full analysis pipeline.
        
        Returns:
            Dict: Analysis result summary
        """
        # 1. Extract lineage
        lineage_df = self.extract_lineage()
        lineage_count = lineage_df.count()
        
        # 2. Find common ancestors
        ancestors_df = self.find_common_ancestors()
        ancestor_pairs_count = ancestors_df.count()
        
        # 3. Extract schema texts
        schema_df = self.extract_schema_texts()
        schema_count = schema_df.count()
        
        # 4. Generate embeddings
        embeddings_df = self.generate_embeddings()
        
        # 5. Find duplicate candidates
        candidates_df = self.find_duplicate_candidates()
        candidates_count = candidates_df.count()
        
        # 6. Generate consolidation recommendations
        recommendations_df = self.generate_recommendations()
        
        # 7. Generate summary report
        summary = self.recommendation_generator.generate_summary_report(recommendations_df)
        
        return {
            "lineage_events": lineage_count,
            "common_ancestor_pairs": ancestor_pairs_count,
            "tables_analyzed": schema_count,
            "duplicate_candidates_found": candidates_count,
            "summary": summary,
            "results": {
                "lineage": lineage_df,
                "common_ancestors": ancestors_df,
                "candidates": candidates_df,
                "recommendations": recommendations_df
            }
        }
    
    def save_results(
        self,
        target_catalog: str,
        target_schema: str,
        mode: str = "overwrite"
    ) -> None:
        """
        Save analysis results to Delta tables.
        
        Args:
            target_catalog: Target catalog
            target_schema: Target schema
            mode: Save mode ("overwrite" or "append")
        """
        base_path = f"{target_catalog}.{target_schema}"
        
        if self._common_ancestors_df is not None:
            (
                self._common_ancestors_df
                .write
                .mode(mode)
                .saveAsTable(f"{base_path}.common_ancestors")
            )
        
        if self._candidates_df is not None:
            # Exclude embedding columns when saving
            candidates_to_save = self._candidates_df.drop("embedding_a", "embedding_b")
            (
                candidates_to_save
                .write
                .mode(mode)
                .saveAsTable(f"{base_path}.consolidation_candidates")
            )
        
        if self._recommendations_df is not None:
            (
                self._recommendations_df
                .write
                .mode(mode)
                .saveAsTable(f"{base_path}.consolidation_recommendations")
            )


def detect_semantic_duplicates(
    spark: SparkSession,
    catalog_filter: Optional[str] = None,
    similarity_threshold: float = 0.8,
    days_back: int = 30,
    embedding_provider: str = "databricks"
) -> Dict[str, Any]:
    """
    Helper function for detecting semantic duplicate tables.
    
    Args:
        spark: SparkSession instance
        catalog_filter: Catalog to analyze (None for all)
        similarity_threshold: Similarity threshold
        days_back: Lineage query period
        embedding_provider: Embedding provider ("databricks", "openai", "simple")
        
    Returns:
        Dict: Analysis results
        
    Example:
        >>> from lineage_ops.src.detector import detect_semantic_duplicates
        >>> results = detect_semantic_duplicates(
        ...     spark,
        ...     catalog_filter="prod_catalog",
        ...     similarity_threshold=0.85
        ... )
        >>> results["results"]["recommendations"].display()
    """
    config = LineageOpsConfig(
        catalog_filter=catalog_filter,
        similarity_threshold=similarity_threshold,
        days_back=days_back,
        embedding_provider=embedding_provider
    )
    
    detector = SemanticDuplicateDetector(spark, config)
    return detector.run_full_analysis()
