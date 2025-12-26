"""
Lineage Ops Main Orchestrator

Provides unified DuplicatedTableDetector for:
- Duplicate table detection with consolidation recommendations
- Fast similarity analysis with UMAP visualization for dashboards
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from typing import Dict, List, Optional, Any
from dataclasses import dataclass

from .data_extractor import DataExtractor, LineageExtractor, SchemaExtractor
from .schema_embedder import (
    SchemaEmbedder, 
    EmbeddingProvider,
    DatabricksFoundationModelProvider,
    SentenceTransformerProvider,
    SimpleHashEmbeddingProvider
)
from .similarity_analyzer import SimilarityAnalyzer, DuplicateCandidateFinder, FastSimilarityAnalyzer
from .recommendation_generator import RecommendationGenerator


@dataclass
class LineageOpsConfig:
    """
    Unified configuration for DuplicatedTableDetector.
    
    Combines settings for:
    - Duplicate detection and consolidation recommendations
    - Similarity analysis with UMAP visualization
    """
    # Common settings
    catalog_filter: Optional[str] = None
    days_back: int = 30
    
    # Embedding settings
    embedding_provider: str = "databricks"  # "databricks", "openai", "sentence_transformers", "simple"
    embedding_endpoint: Optional[str] = None
    embedding_model: Optional[str] = None  # For sentence_transformers: model name
    api_key: Optional[str] = None
    
    # Duplicate detection settings
    similarity_threshold: float = 0.8
    ancestor_overlap_threshold: float = 0.5
    use_lsh: bool = True
    
    # Visualization settings
    top_k_similar: int = 5
    use_umap: bool = True


class DuplicatedTableDetector:
    """
    Unified detector for duplicate tables with visualization.
    
    Provides two main capabilities:
    1. Duplicate Detection: Find semantic duplicate tables and generate 
       consolidation recommendations with SoT (Source of Truth) decisions
    2. Similarity Visualization: Generate UMAP 2D coordinates with usage-based 
       coloring for Databricks Dashboard scatter plots
    
    Example:
        >>> from lineage_ops import DuplicatedTableDetector, LineageOpsConfig
        >>> 
        >>> config = LineageOpsConfig(
        ...     catalog_filter="prod",
        ...     embedding_provider="sentence_transformers"
        ... )
        >>> detector = DuplicatedTableDetector(spark, config)
        >>> 
        >>> # Full analysis with visualization
        >>> results = detector.run_full_analysis()
        >>> results["results"]["recommendations"].display()
        >>> results["results"]["visualization"].display()
        >>> 
        >>> # Fast similarity-only analysis
        >>> sim_results = detector.run_similarity_analysis()
        >>> sim_results["viz_df"]  # For dashboard scatter plot
    """
    
    def __init__(
        self, 
        spark: SparkSession,
        config: Optional[LineageOpsConfig] = None
    ):
        """
        Initialize DuplicatedTableDetector.
        
        Args:
            spark: SparkSession instance
            config: Unified configuration
        """
        self.spark = spark
        self.config = config or LineageOpsConfig()
        
        # Initialize extractors
        self.lineage_extractor = LineageExtractor(
            spark, 
            catalog_filter=self.config.catalog_filter
        )
        self.schema_extractor = SchemaExtractor(spark)
        self.data_extractor = DataExtractor(
            spark, 
            catalog_filter=self.config.catalog_filter
        )
        
        # Initialize embedding provider
        self.embedding_provider = self._create_embedding_provider()
        self.schema_embedder = SchemaEmbedder(spark, self.embedding_provider)
        
        # Initialize analyzers
        self.similarity_analyzer = SimilarityAnalyzer(spark)
        self.fast_similarity_analyzer = FastSimilarityAnalyzer(
            use_umap=self.config.use_umap
        )
        self.candidate_finder = DuplicateCandidateFinder(spark)
        self.recommendation_generator = RecommendationGenerator(spark)
        
        # Store intermediate results
        self._lineage_df: Optional[DataFrame] = None
        self._common_ancestors_df: Optional[DataFrame] = None
        self._schema_texts_df: Optional[DataFrame] = None
        self._embeddings_df: Optional[DataFrame] = None
        self._candidates_df: Optional[DataFrame] = None
        self._recommendations_df: Optional[DataFrame] = None
        self._viz_df: Optional[DataFrame] = None
        self._similarity_df = None  # Pandas DataFrame for top-k similar
        self._enriched_pdf = None  # Pandas DataFrame for enriched metadata
    
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
        elif provider_type == "sentence_transformers":
            return SentenceTransformerProvider(
                model_name=self.config.embedding_model or "all-MiniLM-L6-v2"
            )
        elif provider_type == "simple":
            return SimpleHashEmbeddingProvider()
        else:
            raise ValueError(f"Unknown embedding provider: {provider_type}")
    
    # =========================================================================
    # Data Extraction Methods
    # =========================================================================
    
    def extract_lineage(self) -> DataFrame:
        """
        Extract lineage information from system tables.
        
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
        Extract table schema texts for embedding.
        
        Args:
            table_names: List of table names to analyze (None for auto-detection)
            
        Returns:
            DataFrame: Schema texts
        """
        if table_names is None:
            if self._common_ancestors_df is None:
                self.find_common_ancestors()
            
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
        Generate schema embeddings using configured provider.
        
        Returns:
            DataFrame: Schema embeddings
        """
        if self._schema_texts_df is None:
            self.extract_schema_texts()
        
        self._embeddings_df = self.schema_embedder.embed_schema_texts(
            self._schema_texts_df
        )
        return self._embeddings_df
    
    # =========================================================================
    # Duplicate Detection Methods
    # =========================================================================
    
    def find_duplicate_candidates(self) -> DataFrame:
        """
        Find duplicate table candidates based on similarity.
        
        Returns:
            DataFrame: Duplicate candidates with similarity scores
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
        Generate consolidation recommendations for duplicate candidates.
        
        Returns:
            DataFrame: Consolidation recommendations with SoT decisions
        """
        if self._candidates_df is None:
            self.find_duplicate_candidates()
        
        self._recommendations_df = self.recommendation_generator.generate_recommendations_batch(
            self._candidates_df
        )
        return self._recommendations_df
    
    # =========================================================================
    # Visualization Methods
    # =========================================================================
    
    def generate_visualization(self) -> DataFrame:
        """
        Generate 2D UMAP visualization with usage-based coloring.
        
        Creates a DataFrame suitable for Databricks Dashboard scatter plot.
        
        Returns:
            DataFrame: Visualization data with columns:
                - full_table_name
                - vis_x, vis_y (UMAP 2D coordinates)
                - usage_score (for color coding)
                - schema_text (for tooltip)
        """
        import pandas as pd
        import numpy as np
        
        if self._embeddings_df is None:
            self.generate_embeddings()
        
        print("Generating UMAP visualization coordinates...")
        
        embeddings_rows = self._embeddings_df.select(
            "full_table_name", "schema_text", "embedding"
        ).collect()
        
        table_names = [row["full_table_name"] for row in embeddings_rows]
        schema_texts = [row["schema_text"] for row in embeddings_rows]
        
        embeddings = np.array([
            self._extract_embedding_array(row["embedding"]) 
            for row in embeddings_rows
        ])
        
        viz_pdf = self.fast_similarity_analyzer.compute_umap_coordinates(
            embeddings, table_names
        )
        viz_pdf['schema_text'] = schema_texts
        
        usage_df = self._get_usage_scores(table_names)
        viz_pdf = viz_pdf.merge(
            usage_df.toPandas() if isinstance(usage_df, DataFrame) else usage_df,
            on='full_table_name',
            how='left'
        )
        viz_pdf['usage_score'] = viz_pdf['usage_score'].fillna(0).astype(int)
        
        self._viz_df = self.spark.createDataFrame(viz_pdf)
        
        print(f"Generated visualization for {len(table_names)} tables")
        return self._viz_df
    
    def _get_usage_scores(self, table_names: List[str]) -> DataFrame:
        """Get usage scores from audit logs."""
        import pandas as pd
        
        table_df = self.spark.createDataFrame(
            [(name,) for name in table_names],
            ["full_table_name"]
        )
        table_df.createOrReplaceTempView("_viz_target_tables")
        
        try:
            usage_query = f"""
            SELECT 
                request_params['full_name_arg'] AS full_table_name,
                COUNT(*) AS usage_score
            FROM system.access.audit
            WHERE event_time >= current_date() - INTERVAL {self.config.days_back} DAY
                AND action_name IN ('getTable', 'readTable', 'writeTable')
                AND request_params['full_name_arg'] IN (SELECT full_table_name FROM _viz_target_tables)
            GROUP BY request_params['full_name_arg']
            """
            return self.spark.sql(usage_query)
        except Exception:
            return pd.DataFrame({
                'full_table_name': table_names,
                'usage_score': [0] * len(table_names)
            })
    
    def _extract_embedding_array(self, embedding) -> List[float]:
        """Extract embedding as list from various formats."""
        import numpy as np
        
        if embedding is None:
            raise ValueError("Embedding is None")
        
        if hasattr(embedding, 'toArray'):
            return embedding.toArray().tolist()
        if isinstance(embedding, np.ndarray):
            return embedding.tolist()
        if isinstance(embedding, (list, tuple)):
            return list(embedding)
        
        try:
            return list(embedding)
        except TypeError:
            raise ValueError(f"Unsupported embedding type: {type(embedding)}")
    
    # =========================================================================
    # Fast Similarity Analysis (using enriched metadata)
    # =========================================================================
    
    def run_similarity_analysis(
        self,
        output_map_table: Optional[str] = None,
        output_rel_table: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Run fast similarity analysis with UMAP visualization.
        
        Uses enriched metadata (Schema + Lineage + Usage) for hybrid embedding.
        Faster than full analysis, ideal for dashboard visualization.
        
        Args:
            output_map_table: Optional Delta table for visualization map
            output_rel_table: Optional Delta table for similarity relations
            
        Returns:
            Dict with:
                - viz_df: Pandas DataFrame with vis_x, vis_y, usage_score
                - similarity_df: Pandas DataFrame with source, target, score
                - table_count: Number of tables analyzed
        """
        import pandas as pd
        
        print("Step 1/4: Extracting enriched metadata (Schema + Lineage + Usage)...")
        enriched_df = self.data_extractor.get_enriched_metadata(
            days_back=self.config.days_back
        )
        enriched_df = self.data_extractor.create_signature_text(enriched_df)
        
        self._enriched_pdf = enriched_df.toPandas()
        table_count = len(self._enriched_pdf)
        print(f"   Found {table_count} tables")
        
        print("Step 2/4: Generating embeddings...")
        signatures = self._enriched_pdf['signature'].tolist()
        embeddings = self.embedding_provider.embed(signatures)
        self._enriched_pdf['embedding'] = embeddings
        
        print("Step 3/4: Computing UMAP coordinates and similarity...")
        viz_pdf, sim_pdf = self.fast_similarity_analyzer.analyze_pandas_df(
            self._enriched_pdf,
            embedding_col='embedding',
            name_col='full_table_name',
            usage_col='usage_score',
            top_k=self.config.top_k_similar
        )
        
        comment_map = self._enriched_pdf.set_index('full_table_name')['tbl_comment'].to_dict()
        viz_pdf['tbl_comment'] = viz_pdf['full_table_name'].map(comment_map)
        
        self._similarity_df = sim_pdf
        
        print("Step 4/4: Saving results...")
        if output_map_table:
            viz_spark_df = self.spark.createDataFrame(viz_pdf)
            viz_spark_df.write.mode("overwrite").saveAsTable(output_map_table)
            print(f"   Saved visualization map to {output_map_table}")
        
        if output_rel_table:
            rel_spark_df = self.spark.createDataFrame(sim_pdf)
            rel_spark_df.write.mode("overwrite").saveAsTable(output_rel_table)
            print(f"   Saved similarity relations to {output_rel_table}")
        
        print("Done!")
        
        return {
            "viz_df": viz_pdf,
            "similarity_df": sim_pdf,
            "table_count": table_count
        }
    
    # =========================================================================
    # Full Analysis Pipeline
    # =========================================================================
    
    def run_full_analysis(self, include_visualization: bool = True) -> Dict[str, Any]:
        """
        Run the full duplicate detection pipeline with optional visualization.
        
        Steps:
        1. Extract lineage from system tables
        2. Find common ancestors between tables
        3. Extract schema texts for target tables
        4. Generate embeddings
        5. Find duplicate candidates
        6. Generate consolidation recommendations
        7. (Optional) Generate UMAP visualization
        
        Args:
            include_visualization: Whether to generate UMAP visualization data
        
        Returns:
            Dict with analysis results including:
                - lineage_events: Count of lineage events
                - common_ancestor_pairs: Count of table pairs with common ancestors
                - tables_analyzed: Count of tables analyzed
                - duplicate_candidates_found: Count of duplicate candidates
                - summary: Summary statistics
                - results: DataFrames (lineage, common_ancestors, candidates, 
                          recommendations, visualization)
        """
        print("=" * 60)
        print("Running Full Duplicate Detection Analysis")
        print("=" * 60)
        
        print("\n1/7: Extracting lineage...")
        lineage_df = self.extract_lineage()
        lineage_count = lineage_df.count()
        print(f"   Found {lineage_count} lineage events")
        
        print("\n2/7: Finding common ancestors...")
        ancestors_df = self.find_common_ancestors()
        ancestor_pairs_count = ancestors_df.count()
        print(f"   Found {ancestor_pairs_count} table pairs with common ancestors")
        
        print("\n3/7: Extracting schema texts...")
        schema_df = self.extract_schema_texts()
        schema_count = schema_df.count()
        print(f"   Extracted schema for {schema_count} tables")
        
        print("\n4/7: Generating embeddings...")
        self.generate_embeddings()
        print("   Embeddings generated")
        
        print("\n5/7: Finding duplicate candidates...")
        candidates_df = self.find_duplicate_candidates()
        candidates_count = candidates_df.count()
        print(f"   Found {candidates_count} duplicate candidates")
        
        print("\n6/7: Generating recommendations...")
        recommendations_df = self.generate_recommendations()
        print("   Recommendations generated")
        
        summary = self.recommendation_generator.generate_summary_report(recommendations_df)
        
        viz_df = None
        if include_visualization:
            print("\n7/7: Generating visualization...")
            viz_df = self.generate_visualization()
        else:
            print("\n7/7: Skipping visualization")
        
        print("\n" + "=" * 60)
        print("Analysis Complete!")
        print("=" * 60)
        
        results = {
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
        
        if viz_df is not None:
            results["results"]["visualization"] = viz_df
        
        return results
    
    # =========================================================================
    # Save Results
    # =========================================================================
    
    def save_results(
        self,
        target_catalog: str,
        target_schema: str,
        mode: str = "overwrite"
    ) -> None:
        """
        Save analysis results to Delta tables.
        
        Saves the following tables:
        - common_ancestors: Table pairs with common source tables
        - consolidation_candidates: Duplicate candidate pairs
        - consolidation_recommendations: Recommendations with SoT decisions
        - table_viz_map: UMAP visualization coordinates with usage scores
        - table_similarity: Top-K similar table pairs
        
        Args:
            target_catalog: Target catalog name
            target_schema: Target schema name
            mode: Save mode ("overwrite" or "append")
        """
        base_path = f"{target_catalog}.{target_schema}"
        print(f"\nSaving results to {base_path}...")
        
        if self._common_ancestors_df is not None:
            self._common_ancestors_df.write.mode(mode).saveAsTable(f"{base_path}.common_ancestors")
            print(f"  ✓ Saved common_ancestors")
        
        if self._candidates_df is not None:
            candidates_to_save = self._candidates_df.drop("embedding_a", "embedding_b")
            candidates_to_save.write.mode(mode).saveAsTable(f"{base_path}.consolidation_candidates")
            print(f"  ✓ Saved consolidation_candidates")
        
        if self._recommendations_df is not None:
            self._recommendations_df.write.mode(mode).saveAsTable(f"{base_path}.consolidation_recommendations")
            print(f"  ✓ Saved consolidation_recommendations")
        
        if self._viz_df is not None:
            self._viz_df.write.mode(mode).saveAsTable(f"{base_path}.table_viz_map")
            print(f"  ✓ Saved table_viz_map (Dashboard: X=vis_x, Y=vis_y, Color=usage_score)")
        
        if self._similarity_df is not None:
            sim_spark_df = self.spark.createDataFrame(self._similarity_df)
            sim_spark_df.write.mode(mode).saveAsTable(f"{base_path}.table_similarity")
            print(f"  ✓ Saved table_similarity")
        
        print("Done!")


# =============================================================================
# Helper Functions
# =============================================================================

def detect_duplicated_tables(
    spark: SparkSession,
    catalog_filter: Optional[str] = None,
    similarity_threshold: float = 0.8,
    days_back: int = 30,
    embedding_provider: str = "databricks",
    include_visualization: bool = True
) -> Dict[str, Any]:
    """
    Helper function for detecting duplicate tables.
    
    Args:
        spark: SparkSession instance
        catalog_filter: Catalog to analyze (None for all)
        similarity_threshold: Similarity threshold for duplicate detection
        days_back: Lineage query period in days
        embedding_provider: Embedding provider ("databricks", "openai", "sentence_transformers", "simple")
        include_visualization: Whether to generate UMAP visualization
        
    Returns:
        Dict: Analysis results with recommendations and visualization
        
    Example:
        >>> from lineage_ops import detect_duplicated_tables
        >>> results = detect_duplicated_tables(
        ...     spark,
        ...     catalog_filter="prod_catalog",
        ...     similarity_threshold=0.85
        ... )
        >>> results["results"]["recommendations"].display()
        >>> results["results"]["visualization"].display()
    """
    config = LineageOpsConfig(
        catalog_filter=catalog_filter,
        similarity_threshold=similarity_threshold,
        days_back=days_back,
        embedding_provider=embedding_provider
    )
    
    detector = DuplicatedTableDetector(spark, config)
    return detector.run_full_analysis(include_visualization=include_visualization)


def analyze_table_similarity(
    spark: SparkSession,
    catalog_filter: Optional[str] = None,
    days_back: int = 30,
    top_k: int = 5,
    embedding_provider: str = "sentence_transformers",
    output_map_table: Optional[str] = None,
    output_rel_table: Optional[str] = None
) -> Dict[str, Any]:
    """
    Helper function for fast table similarity analysis with UMAP visualization.
    
    Designed for Databricks Dashboard integration.
    
    Args:
        spark: SparkSession instance
        catalog_filter: Catalog to analyze (None for all)
        days_back: Days back for lineage/usage data
        top_k: Number of top similar tables per table
        embedding_provider: Embedding provider (default: sentence_transformers for speed)
        output_map_table: Delta table for visualization map (optional)
        output_rel_table: Delta table for similarity relations (optional)
        
    Returns:
        Dict with viz_df, similarity_df, and table_count
        
    Example:
        >>> from lineage_ops import analyze_table_similarity
        >>> results = analyze_table_similarity(
        ...     spark,
        ...     catalog_filter="my_catalog",
        ...     output_map_table="my_catalog.analytics.table_viz_map"
        ... )
        >>> # Dashboard: Scatter Plot with X=vis_x, Y=vis_y, Color=usage_score
    """
    config = LineageOpsConfig(
        catalog_filter=catalog_filter,
        days_back=days_back,
        top_k_similar=top_k,
        embedding_provider=embedding_provider
    )
    
    detector = DuplicatedTableDetector(spark, config)
    return detector.run_similarity_analysis(
        output_map_table=output_map_table,
        output_rel_table=output_rel_table
    )
