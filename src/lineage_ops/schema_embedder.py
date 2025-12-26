"""
Schema Embedding Module

Converts table column names, data types, and comments into text and then to embedding vectors.
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.ml.linalg import Vectors, VectorUDT
from typing import List, Optional, Callable
import numpy as np
from abc import ABC, abstractmethod


class EmbeddingProvider(ABC):
    """Abstract class for embedding providers."""
    
    @abstractmethod
    def embed(self, texts: List[str]) -> List[List[float]]:
        """
        Convert text list to embedding vectors.
        
        Args:
            texts: List of texts to embed
            
        Returns:
            List[List[float]]: List of embedding vectors
        """
        pass
    
    @property
    @abstractmethod
    def dimension(self) -> int:
        """Return the dimension of embedding vectors."""
        pass


class DatabricksFoundationModelProvider(EmbeddingProvider):
    """
    Embedding provider using Databricks Foundation Model API.
    
    Note: Requires Databricks Runtime 14.x+ and Foundation Model API access
    """
    
    def __init__(
        self, 
        endpoint_name: str = "databricks-bge-large-en",
        batch_size: int = 16
    ):
        """
        Initialize Databricks Foundation Model embedding provider.
        
        Args:
            endpoint_name: Foundation Model endpoint name
            batch_size: Batch size
        """
        self.endpoint_name = endpoint_name
        self.batch_size = batch_size
        self._dimension = 1024  # BGE-large default dimension
    
    def embed(self, texts: List[str]) -> List[List[float]]:
        """
        Embed texts using Databricks Foundation Model API.
        
        Args:
            texts: List of texts to embed
            
        Returns:
            List[List[float]]: List of embedding vectors
        """
        from mlflow.deployments import get_deploy_client
        
        client = get_deploy_client("databricks")
        
        all_embeddings = []
        for i in range(0, len(texts), self.batch_size):
            batch = texts[i:i + self.batch_size]
            response = client.predict(
                endpoint=self.endpoint_name,
                inputs={"input": batch}
            )
            batch_embeddings = [item["embedding"] for item in response["data"]]
            all_embeddings.extend(batch_embeddings)
        
        return all_embeddings
    
    @property
    def dimension(self) -> int:
        return self._dimension


class OpenAIEmbeddingProvider(EmbeddingProvider):
    """
    Embedding provider using OpenAI API.
    
    Note: Requires Azure OpenAI or OpenAI API key
    """
    
    def __init__(
        self,
        api_key: str,
        model: str = "text-embedding-3-small",
        batch_size: int = 100,
        base_url: Optional[str] = None
    ):
        """
        Initialize OpenAI embedding provider.
        
        Args:
            api_key: OpenAI API key
            model: Embedding model to use
            batch_size: Batch size
            base_url: Azure OpenAI endpoint URL
        """
        self.api_key = api_key
        self.model = model
        self.batch_size = batch_size
        self.base_url = base_url
        
        # Dimensions per model
        self._dimensions = {
            "text-embedding-3-small": 1536,
            "text-embedding-3-large": 3072,
            "text-embedding-ada-002": 1536
        }
        self._dimension = self._dimensions.get(model, 1536)
    
    def embed(self, texts: List[str]) -> List[List[float]]:
        """
        Embed texts using OpenAI API.
        
        Args:
            texts: List of texts to embed
            
        Returns:
            List[List[float]]: List of embedding vectors
        """
        from openai import OpenAI
        
        client = OpenAI(api_key=self.api_key, base_url=self.base_url)
        
        all_embeddings = []
        for i in range(0, len(texts), self.batch_size):
            batch = texts[i:i + self.batch_size]
            response = client.embeddings.create(
                input=batch,
                model=self.model
            )
            batch_embeddings = [item.embedding for item in response.data]
            all_embeddings.extend(batch_embeddings)
        
        return all_embeddings
    
    @property
    def dimension(self) -> int:
        return self._dimension


class SimpleHashEmbeddingProvider(EmbeddingProvider):
    """
    Simple hash-based embedding provider (for testing/development).
    
    Does not provide actual semantic similarity, use for testing purposes only.
    """
    
    def __init__(self, dimension: int = 128):
        """
        Initialize hash-based embedding provider.
        
        Args:
            dimension: Embedding vector dimension
        """
        self._dim = dimension
    
    def embed(self, texts: List[str]) -> List[List[float]]:
        """
        Embed texts using hash-based method.
        
        Args:
            texts: List of texts to embed
            
        Returns:
            List[List[float]]: List of embedding vectors
        """
        embeddings = []
        for text in texts:
            # Use text hash as seed for reproducible vectors
            np.random.seed(hash(text) % (2**32))
            vector = np.random.randn(self._dim)
            # L2 normalization
            vector = vector / np.linalg.norm(vector)
            embeddings.append(vector.tolist())
        return embeddings
    
    @property
    def dimension(self) -> int:
        return self._dim


class SentenceTransformerProvider(EmbeddingProvider):
    """
    Embedding provider using SentenceTransformers library.
    
    Fast local embedding without external API calls.
    Recommended for speed and cost optimization.
    
    Note: Requires sentence-transformers package: %pip install sentence-transformers
    """
    
    def __init__(
        self,
        model_name: str = "all-MiniLM-L6-v2",
        batch_size: int = 32,
        show_progress: bool = True
    ):
        """
        Initialize SentenceTransformer embedding provider.
        
        Args:
            model_name: Model name from HuggingFace (default: all-MiniLM-L6-v2)
                - 'all-MiniLM-L6-v2': Fast, 384 dim, good quality
                - 'all-mpnet-base-v2': Slower, 768 dim, better quality
                - 'paraphrase-multilingual-MiniLM-L12-v2': Multilingual support
            batch_size: Batch size for encoding
            show_progress: Show progress bar during encoding
        """
        self.model_name = model_name
        self.batch_size = batch_size
        self.show_progress = show_progress
        self._model = None
        
        # Dimensions per model
        self._dimensions = {
            "all-MiniLM-L6-v2": 384,
            "all-mpnet-base-v2": 768,
            "paraphrase-multilingual-MiniLM-L12-v2": 384,
            "all-distilroberta-v1": 768,
        }
        self._dimension = self._dimensions.get(model_name, 384)
    
    def _get_model(self):
        """Lazy load model to avoid import issues."""
        if self._model is None:
            from sentence_transformers import SentenceTransformer
            self._model = SentenceTransformer(self.model_name)
            # Update dimension from actual model
            self._dimension = self._model.get_sentence_embedding_dimension()
        return self._model
    
    def embed(self, texts: List[str]) -> List[List[float]]:
        """
        Embed texts using SentenceTransformer.
        
        Args:
            texts: List of texts to embed
            
        Returns:
            List[List[float]]: List of embedding vectors
        """
        model = self._get_model()
        embeddings = model.encode(
            texts,
            batch_size=self.batch_size,
            show_progress_bar=self.show_progress,
            convert_to_numpy=True
        )
        return embeddings.tolist()
    
    @property
    def dimension(self) -> int:
        return self._dimension


class SchemaEmbedder:
    """Class for converting table schemas to embedding vectors."""
    
    def __init__(
        self, 
        spark: SparkSession,
        embedding_provider: EmbeddingProvider
    ):
        """
        Initialize SchemaEmbedder.
        
        Args:
            spark: SparkSession instance
            embedding_provider: Embedding provider instance
        """
        self.spark = spark
        self.embedding_provider = embedding_provider
    
    def embed_schema_texts(self, schema_texts_df: DataFrame) -> DataFrame:
        """
        Embed schema texts DataFrame.
        
        Args:
            schema_texts_df: DataFrame with full_table_name, schema_text columns
            
        Returns:
            DataFrame: DataFrame with full_table_name, schema_text, embedding columns
        """
        # Collect data to driver (embedding API calls are performed on driver)
        # Note: Batch processing needed for large-scale data
        rows = schema_texts_df.select("full_table_name", "schema_text").collect()
        
        table_names = [row["full_table_name"] for row in rows]
        schema_texts = [row["schema_text"] for row in rows]
        
        # Generate embeddings
        embeddings = self.embedding_provider.embed(schema_texts)
        
        # Create result DataFrame
        result_data = [
            (name, text, Vectors.dense(emb))
            for name, text, emb in zip(table_names, schema_texts, embeddings)
        ]
        
        schema = T.StructType([
            T.StructField("full_table_name", T.StringType(), False),
            T.StructField("schema_text", T.StringType(), False),
            T.StructField("embedding", VectorUDT(), False)
        ])
        
        return self.spark.createDataFrame(result_data, schema)
    
    def embed_schema_texts_distributed(
        self, 
        schema_texts_df: DataFrame,
        batch_size: int = 100
    ) -> DataFrame:
        """
        Embed schema texts with distributed processing.
        
        Suitable for large-scale tables, uses Pandas UDF.
        
        Args:
            schema_texts_df: DataFrame with full_table_name, schema_text columns
            batch_size: Batch size
            
        Returns:
            DataFrame: DataFrame with full_table_name, schema_text, embedding columns
        """
        from pyspark.sql.functions import pandas_udf
        import pandas as pd
        
        embedding_dim = self.embedding_provider.dimension
        provider = self.embedding_provider
        
        # Assign batch numbers
        schema_texts_df = schema_texts_df.withColumn(
            "batch_id",
            F.floor(F.monotonically_increasing_id() / batch_size)
        )
        
        # Define Pandas UDF
        @pandas_udf(T.ArrayType(T.FloatType()))
        def embed_batch(texts: pd.Series) -> pd.Series:
            """Embed text batch."""
            text_list = texts.tolist()
            embeddings = provider.embed(text_list)
            return pd.Series(embeddings)
        
        # Apply embedding
        result_df = schema_texts_df.withColumn(
            "embedding_array",
            embed_batch(F.col("schema_text"))
        )
        
        # Convert Array to Vector
        to_vector_udf = F.udf(
            lambda arr: Vectors.dense(arr) if arr else None,
            VectorUDT()
        )
        
        result_df = (
            result_df
            .withColumn("embedding", to_vector_udf(F.col("embedding_array")))
            .drop("embedding_array", "batch_id")
        )
        
        return result_df


class ColumnEmbedder:
    """Embeds individual columns to detect column-level duplicates."""
    
    def __init__(
        self,
        spark: SparkSession,
        embedding_provider: EmbeddingProvider
    ):
        """
        Initialize ColumnEmbedder.
        
        Args:
            spark: SparkSession instance
            embedding_provider: Embedding provider instance
        """
        self.spark = spark
        self.embedding_provider = embedding_provider
    
    def create_column_text(self, column_name: str, data_type: str, comment: str = None) -> str:
        """
        Convert column info to text for embedding.
        
        Args:
            column_name: Column name
            data_type: Data type
            comment: Column description
            
        Returns:
            str: Text representation of column
        """
        parts = [f"Column: {column_name}", f"Type: {data_type}"]
        if comment:
            parts.append(f"Description: {comment}")
        return " | ".join(parts)
    
    def embed_columns(self, columns_df: DataFrame) -> DataFrame:
        """
        Embed column info DataFrame.
        
        Args:
            columns_df: DataFrame with full_table_name, COLUMN_NAME, DATA_TYPE, COMMENT columns
            
        Returns:
            DataFrame: DataFrame with embeddings added
        """
        # Generate column text
        columns_with_text = (
            columns_df
            .withColumn(
                "column_text",
                F.concat(
                    F.lit("Column: "),
                    F.col("COLUMN_NAME"),
                    F.lit(" | Type: "),
                    F.col("DATA_TYPE"),
                    F.when(
                        F.col("COMMENT").isNotNull(),
                        F.concat(F.lit(" | Description: "), F.col("COMMENT"))
                    ).otherwise(F.lit(""))
                )
            )
        )
        
        # Perform embedding on driver
        rows = columns_with_text.collect()
        texts = [row["column_text"] for row in rows]
        embeddings = self.embedding_provider.embed(texts)
        
        # Build result data
        result_data = []
        for row, emb in zip(rows, embeddings):
            result_data.append((
                row["full_table_name"],
                row["COLUMN_NAME"],
                row["DATA_TYPE"],
                row["COMMENT"],
                row["column_text"],
                Vectors.dense(emb)
            ))
        
        schema = T.StructType([
            T.StructField("full_table_name", T.StringType(), False),
            T.StructField("column_name", T.StringType(), False),
            T.StructField("data_type", T.StringType(), False),
            T.StructField("comment", T.StringType(), True),
            T.StructField("column_text", T.StringType(), False),
            T.StructField("embedding", VectorUDT(), False)
        ])
        
        return self.spark.createDataFrame(result_data, schema)
