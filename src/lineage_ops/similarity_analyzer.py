"""
Similarity Analysis Module

Clusters tables with high vector similarity and identifies duplicate candidates.
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.window import Window
from pyspark.ml.linalg import Vectors, DenseVector
from pyspark.ml.feature import BucketedRandomProjectionLSH
from pyspark.ml.clustering import KMeans
from typing import List, Tuple, Optional
import numpy as np


class SimilarityAnalyzer:
    """Class for analyzing similarity between table schemas."""
    
    def __init__(self, spark: SparkSession):
        """
        Initialize SimilarityAnalyzer.
        
        Args:
            spark: SparkSession instance
        """
        self.spark = spark
    
    def compute_cosine_similarity(
        self, 
        embeddings_df: DataFrame,
        similarity_threshold: float = 0.8,
        max_tables_for_crossjoin: int = 500
    ) -> DataFrame:
        """
        Compute cosine similarity for all table pairs.
        
        WARNING: This uses crossJoin which is O(n²). For large datasets (>500 tables),
        use compute_similarity_with_lsh() instead.
        
        Args:
            embeddings_df: DataFrame with full_table_name, embedding columns
            similarity_threshold: Similarity threshold (return pairs above this)
            max_tables_for_crossjoin: Maximum tables before warning (default 500)
            
        Returns:
            DataFrame: Similar table pairs with similarity scores
        """
        # Cache DataFrame to avoid multiple scans
        table_count = embeddings_df.count()
        
        if table_count > max_tables_for_crossjoin:
            print(f"WARNING: {table_count} tables detected. crossJoin will create "
                  f"{table_count * (table_count - 1) // 2:,} pairs. "
                  f"Consider using compute_similarity_with_lsh() for better performance.")
        
        # UDF for cosine similarity calculation
        @F.udf(T.FloatType())
        def cosine_similarity(v1: DenseVector, v2: DenseVector) -> float:
            """Compute cosine similarity between two vectors."""
            if v1 is None or v2 is None:
                return 0.0
            a = np.array(v1.toArray())
            b = np.array(v2.toArray())
            norm_a = np.linalg.norm(a)
            norm_b = np.linalg.norm(b)
            if norm_a == 0 or norm_b == 0:
                return 0.0
            return float(np.dot(a, b) / (norm_a * norm_b))
        
        # Self-join to create all pairs (avoid duplicates)
        df1 = embeddings_df.select(
            F.col("full_table_name").alias("table_a"),
            F.col("embedding").alias("embedding_a")
        )
        df2 = embeddings_df.select(
            F.col("full_table_name").alias("table_b"),
            F.col("embedding").alias("embedding_b")
        )
        
        pairs = df1.crossJoin(df2).filter(F.col("table_a") < F.col("table_b"))
        
        # Compute similarity
        similarity_df = (
            pairs
            .withColumn(
                "cosine_similarity",
                cosine_similarity(F.col("embedding_a"), F.col("embedding_b"))
            )
            .filter(F.col("cosine_similarity") >= similarity_threshold)
            .select("table_a", "table_b", "cosine_similarity")
            .orderBy(F.desc("cosine_similarity"))
        )
        
        return similarity_df
    
    def compute_similarity_with_lsh(
        self,
        embeddings_df: DataFrame,
        similarity_threshold: float = 0.8,
        num_hash_tables: int = 5,
        bucket_length: float = 2.0
    ) -> DataFrame:
        """
        Efficiently compute similarity using LSH (Locality Sensitive Hashing).
        
        Used to avoid O(n²) comparisons on large datasets.
        
        Args:
            embeddings_df: DataFrame with full_table_name, embedding columns
            similarity_threshold: Similarity threshold
            num_hash_tables: Number of LSH hash tables
            bucket_length: Bucket length (smaller = more precise)
            
        Returns:
            DataFrame: Similar table pairs with similarity scores
        """
        # Create LSH model
        lsh = BucketedRandomProjectionLSH(
            inputCol="embedding",
            outputCol="hashes",
            numHashTables=num_hash_tables,
            bucketLength=bucket_length
        )
        
        # Fit model
        model = lsh.fit(embeddings_df)
        
        # Find similar items based on Euclidean distance
        # Cosine similarity 0.8 ≈ Euclidean distance 0.63 (for normalized vectors)
        distance_threshold = np.sqrt(2 * (1 - similarity_threshold))
        
        similar_pairs = model.approxSimilarityJoin(
            embeddings_df.select("full_table_name", "embedding"),
            embeddings_df.select(
                F.col("full_table_name").alias("full_table_name_2"),
                F.col("embedding")  # Keep column name as "embedding" for LSH model
            ),
            threshold=distance_threshold,
            distCol="euclidean_distance"
        )
        
        # Clean up results
        @F.udf(T.FloatType())
        def euclidean_to_cosine(dist: float) -> float:
            """Convert Euclidean distance to cosine similarity (assuming normalized vectors)."""
            return 1 - (dist ** 2) / 2
        
        result = (
            similar_pairs
            .filter(F.col("datasetA.full_table_name") < F.col("datasetB.full_table_name_2"))
            .select(
                F.col("datasetA.full_table_name").alias("table_a"),
                F.col("datasetB.full_table_name_2").alias("table_b"),
                euclidean_to_cosine(F.col("euclidean_distance")).alias("cosine_similarity")
            )
            .filter(F.col("cosine_similarity") >= similarity_threshold)
            .orderBy(F.desc("cosine_similarity"))
        )
        
        return result
    
    def cluster_similar_tables(
        self,
        embeddings_df: DataFrame,
        num_clusters: int = None,
        min_cluster_size: int = 2
    ) -> DataFrame:
        """
        Cluster tables based on embeddings.
        
        Args:
            embeddings_df: DataFrame with full_table_name, embedding columns
            num_clusters: Number of clusters (None for auto-determination)
            min_cluster_size: Minimum cluster size (filter smaller)
            
        Returns:
            DataFrame: Tables with cluster assignments
        """
        table_count = embeddings_df.count()
        
        if num_clusters is None:
            # Empirical rule: sqrt(n/2)
            num_clusters = max(2, int(np.sqrt(table_count / 2)))
        
        # KMeans clustering
        kmeans = KMeans(
            featuresCol="embedding",
            predictionCol="cluster_id",
            k=num_clusters,
            seed=42
        )
        
        model = kmeans.fit(embeddings_df)
        clustered = model.transform(embeddings_df)
        
        # Calculate table count per cluster
        cluster_sizes = (
            clustered
            .groupBy("cluster_id")
            .agg(F.count("*").alias("cluster_size"))
        )
        
        # Keep only clusters with minimum size
        result = (
            clustered
            .join(cluster_sizes, on="cluster_id")
            .filter(F.col("cluster_size") >= min_cluster_size)
            .select(
                "full_table_name",
                "cluster_id",
                "cluster_size",
                "embedding"
            )
            .orderBy("cluster_id", "full_table_name")
        )
        
        return result
    
    def compute_column_overlap(
        self,
        table_a_columns: DataFrame,
        table_b_columns: DataFrame
    ) -> Tuple[float, List[str], List[str]]:
        """
        Compute column overlap between two tables.
        
        Args:
            table_a_columns: Table A columns DataFrame
            table_b_columns: Table B columns DataFrame
            
        Returns:
            Tuple: (overlap ratio, common columns list, different columns list)
        """
        cols_a = set(
            row["COLUMN_NAME"].lower() 
            for row in table_a_columns.select("COLUMN_NAME").collect()
        )
        cols_b = set(
            row["COLUMN_NAME"].lower() 
            for row in table_b_columns.select("COLUMN_NAME").collect()
        )
        
        common = cols_a & cols_b
        diff_a = cols_a - cols_b
        diff_b = cols_b - cols_a
        
        overlap_ratio = len(common) / max(len(cols_a), len(cols_b)) if cols_a or cols_b else 0
        
        return overlap_ratio, list(common), list(diff_a | diff_b)


class DuplicateCandidateFinder:
    """Class for comprehensively finding duplicate table candidates."""
    
    def __init__(self, spark: SparkSession):
        """
        Initialize DuplicateCandidateFinder.
        
        Args:
            spark: SparkSession instance
        """
        self.spark = spark
        self.similarity_analyzer = SimilarityAnalyzer(spark)
    
    def find_candidates(
        self,
        embeddings_df: DataFrame,
        lineage_common_ancestors_df: DataFrame,
        similarity_threshold: float = 0.8,
        ancestor_overlap_threshold: float = 0.5
    ) -> DataFrame:
        """
        Find duplicate candidates by combining structural and semantic analysis.
        
        Args:
            embeddings_df: Schema embeddings DataFrame
            lineage_common_ancestors_df: Common ancestor info DataFrame
            similarity_threshold: Schema similarity threshold
            ancestor_overlap_threshold: Source overlap threshold
            
        Returns:
            DataFrame: Duplicate candidate table pairs with details
        """
        # Compute schema similarity
        schema_similarity = self.similarity_analyzer.compute_cosine_similarity(
            embeddings_df,
            similarity_threshold=similarity_threshold
        )
        
        # Filter common ancestor info
        ancestor_candidates = (
            lineage_common_ancestors_df
            .filter(F.col("source_overlap_ratio") >= ancestor_overlap_threshold)
            .select(
                "table_a",
                "table_b",
                "common_source_count",
                "source_overlap_ratio"
            )
        )
        
        # Combine both analysis results
        combined = (
            schema_similarity
            .join(
                ancestor_candidates,
                on=["table_a", "table_b"],
                how="full_outer"
            )
            .withColumn(
                "has_schema_similarity",
                F.col("cosine_similarity").isNotNull()
            )
            .withColumn(
                "has_common_ancestors",
                F.col("common_source_count").isNotNull()
            )
            .withColumn(
                "combined_score",
                # Combined score from both scores
                F.coalesce(F.col("cosine_similarity"), F.lit(0.0)) * 0.6 +
                F.coalesce(F.col("source_overlap_ratio"), F.lit(0.0)) * 0.4
            )
            .withColumn(
                "confidence_level",
                F.when(
                    F.col("has_schema_similarity") & F.col("has_common_ancestors"),
                    F.lit("HIGH")
                ).when(
                    F.col("has_schema_similarity") | 
                    (F.col("source_overlap_ratio") >= 0.8),
                    F.lit("MEDIUM")
                ).otherwise(F.lit("LOW"))
            )
            .filter(
                F.col("has_schema_similarity") | F.col("has_common_ancestors")
            )
            .orderBy(F.desc("combined_score"))
        )
        
        return combined
    
    def enrich_with_metadata(
        self,
        candidates_df: DataFrame,
        table_metadata_df: DataFrame
    ) -> DataFrame:
        """
        Add table metadata to duplicate candidates.
        
        Args:
            candidates_df: Duplicate candidates DataFrame
            table_metadata_df: Table metadata DataFrame
            
        Returns:
            DataFrame: Duplicate candidates with metadata added
        """
        # Join table A metadata
        enriched = (
            candidates_df
            .join(
                table_metadata_df.select(
                    F.col("full_table_name").alias("table_a"),
                    F.col("TABLE_OWNER").alias("owner_a"),
                    F.col("TABLE_TYPE").alias("type_a")
                ),
                on="table_a",
                how="left"
            )
            .join(
                table_metadata_df.select(
                    F.col("full_table_name").alias("table_b"),
                    F.col("TABLE_OWNER").alias("owner_b"),
                    F.col("TABLE_TYPE").alias("type_b")
                ),
                on="table_b",
                how="left"
            )
            .withColumn(
                "same_owner",
                F.col("owner_a") == F.col("owner_b")
            )
        )
        
        return enriched
