"""
Data Extraction Module

Extracts lineage and schema information from Unity Catalog system tables.
Provides enriched metadata with Schema + Lineage + Usage for embedding.
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql import Window
from typing import Optional, List


class DataExtractor:
    """
    Unified data extractor that joins schema, lineage, and usage data.
    
    Creates enriched metadata suitable for hybrid embedding (Schema + Lineage + Usage).
    """
    
    def __init__(self, spark: SparkSession, catalog_filter: Optional[str] = None):
        """
        Initialize DataExtractor.
        
        Args:
            spark: SparkSession instance
            catalog_filter: Filter for specific catalog (e.g., 'prod_catalog')
        """
        self.spark = spark
        self.catalog_filter = catalog_filter
    
    def get_enriched_metadata(self, days_back: int = 30) -> DataFrame:
        """
        Extract and join metadata, lineage, and usage data for all tables.
        
        Creates a unified DataFrame with:
        - Table metadata (name, comment)
        - Column information (aggregated as text)
        - Lineage context (parent tables as text)
        - Usage score (access count from audit logs)
        
        Args:
            days_back: Number of past days to query for lineage/usage
            
        Returns:
            DataFrame: Enriched metadata with columns:
                - full_table_name: catalog.schema.table
                - tbl_comment: Table comment
                - col_list: Comma-separated column names
                - col_comments: Space-separated column comments
                - lineage_context: Space-separated parent table names
                - usage_score: Access count from audit logs
        """
        catalog_condition = ""
        if self.catalog_filter:
            catalog_condition = f"AND TABLE_CATALOG = '{self.catalog_filter}'"
        
        # 1. Get table metadata
        tables_query = f"""
        SELECT 
            CONCAT(TABLE_CATALOG, '.', TABLE_SCHEMA, '.', TABLE_NAME) AS full_table_name,
            TABLE_CATALOG,
            TABLE_SCHEMA,
            TABLE_NAME,
            COMMENT AS tbl_comment
        FROM system.information_schema.tables
        WHERE TABLE_TYPE != 'VIEW'
            {catalog_condition}
        """
        tables = self.spark.sql(tables_query)
        tables.createOrReplaceTempView("_enriched_tables")
        
        # 2. Aggregate column information per table
        cols_query = f"""
        SELECT 
            CONCAT(TABLE_CATALOG, '.', TABLE_SCHEMA, '.', TABLE_NAME) AS full_table_name,
            CONCAT_WS(', ', COLLECT_LIST(COLUMN_NAME)) AS col_list,
            CONCAT_WS(' ', COLLECT_LIST(COALESCE(COMMENT, ''))) AS col_comments
        FROM system.information_schema.columns
        WHERE 1=1 {catalog_condition}
        GROUP BY TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME
        """
        columns_agg = self.spark.sql(cols_query)
        
        # 3. Aggregate lineage (parent tables) per target table
        lineage_query = f"""
        SELECT 
            target_table_full_name AS full_table_name,
            CONCAT_WS(' ', COLLECT_SET(source_table_full_name)) AS lineage_context
        FROM system.access.table_lineage
        WHERE event_time >= current_date() - INTERVAL {days_back} DAY
            AND source_table_full_name IS NOT NULL
            AND target_table_full_name IS NOT NULL
        GROUP BY target_table_full_name
        """
        try:
            lineage = self.spark.sql(lineage_query)
        except Exception:
            # Fallback if lineage table is not accessible
            lineage = self.spark.createDataFrame([], "full_table_name STRING, lineage_context STRING")
        
        # 4. Aggregate usage from audit logs
        audit_query = f"""
        SELECT 
            request_params['full_name_arg'] AS full_table_name,
            COUNT(*) AS usage_score
        FROM system.access.audit
        WHERE event_time >= current_date() - INTERVAL {days_back} DAY
            AND action_name IN ('getTable', 'readTable', 'writeTable')
            AND request_params['full_name_arg'] IS NOT NULL
        GROUP BY request_params['full_name_arg']
        """
        try:
            usage = self.spark.sql(audit_query)
        except Exception:
            # Fallback if audit table is not accessible
            usage = self.spark.createDataFrame([], "full_table_name STRING, usage_score LONG")
        
        # 5. Join all data
        enriched = (
            tables.alias("t")
            .join(columns_agg.alias("c"), "full_table_name", "left")
            .join(lineage.alias("l"), "full_table_name", "left")
            .join(usage.alias("u"), "full_table_name", "left")
            .select(
                "t.full_table_name",
                F.coalesce("t.tbl_comment", F.lit("")).alias("tbl_comment"),
                F.coalesce("c.col_list", F.lit("")).alias("col_list"),
                F.coalesce("c.col_comments", F.lit("")).alias("col_comments"),
                F.coalesce("l.lineage_context", F.lit("")).alias("lineage_context"),
                F.coalesce("u.usage_score", F.lit(0)).alias("usage_score")
            )
        )
        
        return enriched
    
    def create_signature_text(self, enriched_df: DataFrame) -> DataFrame:
        """
        Create signature text for embedding from enriched metadata.
        
        The signature combines table name, comment, columns, and lineage context
        into a single text field suitable for embedding.
        
        Args:
            enriched_df: DataFrame from get_enriched_metadata()
            
        Returns:
            DataFrame: With additional 'signature' column for embedding
        """
        return enriched_df.withColumn(
            "signature",
            F.concat(
                F.lit("Table: "), F.col("full_table_name"),
                F.lit(" Comment: "), F.col("tbl_comment"),
                F.lit(" Columns: "), F.col("col_list"),
                F.lit(" Lineage Parents: "), F.col("lineage_context")
            )
        )


class LineageExtractor:
    """Class for extracting table lineage information."""
    
    def __init__(self, spark: SparkSession, catalog_filter: Optional[str] = None):
        """
        Initialize LineageExtractor.
        
        Args:
            spark: SparkSession instance
            catalog_filter: Filter for specific catalog (e.g., 'prod_catalog')
        """
        self.spark = spark
        self.catalog_filter = catalog_filter
    
    def get_table_lineage(self, days_back: int = 30) -> DataFrame:
        """
        Query table lineage information.
        
        Args:
            days_back: Number of past days to query (default 30)
            
        Returns:
            DataFrame: Table lineage information
        """
        query = f"""
        SELECT DISTINCT
            source_table_full_name,
            source_table_catalog,
            source_table_schema,
            source_table_name,
            target_table_full_name,
            entity_type,
            entity_id,
            MIN(event_time) AS first_seen,
            MAX(event_time) AS last_seen,
            COUNT(*) AS event_count
        FROM system.access.table_lineage
        WHERE event_time >= current_date() - INTERVAL {days_back} DAY
            AND source_table_full_name IS NOT NULL
            AND target_table_full_name IS NOT NULL
        GROUP BY
            source_table_full_name,
            source_table_catalog,
            source_table_schema,
            source_table_name,
            target_table_full_name,
            entity_type,
            entity_id
        """
        
        df = self.spark.sql(query)
        
        if self.catalog_filter:
            df = df.filter(
                (F.col("source_table_catalog") == self.catalog_filter) |
                (F.split(F.col("target_table_full_name"), r"\.")[0] == self.catalog_filter)
            )
        
        return df
    
    def find_common_ancestors(self, lineage_df: DataFrame) -> DataFrame:
        """
        Find target table pairs with common ancestors (Common Parent).
        
        Identifies different target tables derived from the same source tables.
        
        Args:
            lineage_df: Table lineage DataFrame
            
        Returns:
            DataFrame: Table pairs with common ancestors and shared source info
        """
        # Create source table sets for each target table
        target_sources = (
            lineage_df
            .groupBy("target_table_full_name")
            .agg(
                F.collect_set("source_table_full_name").alias("source_tables"),
                F.count("*").alias("total_lineage_events")
            )
        )
        
        # Self-join to create target table pairs (with condition to avoid duplicates)
        df1 = target_sources.alias("t1")
        df2 = target_sources.alias("t2")
        
        paired = (
            df1.join(
                df2,
                F.col("t1.target_table_full_name") < F.col("t2.target_table_full_name")
            )
            .select(
                F.col("t1.target_table_full_name").alias("table_a"),
                F.col("t2.target_table_full_name").alias("table_b"),
                F.col("t1.source_tables").alias("sources_a"),
                F.col("t2.source_tables").alias("sources_b")
            )
        )
        
        # Calculate common sources
        common_ancestors = (
            paired
            .withColumn(
                "common_sources",
                F.array_intersect("sources_a", "sources_b")
            )
            .withColumn(
                "common_source_count",
                F.size("common_sources")
            )
            .withColumn(
                "source_overlap_ratio",
                F.size("common_sources") / 
                F.least(F.size("sources_a"), F.size("sources_b"))
            )
            .filter(F.col("common_source_count") > 0)
            .orderBy(F.desc("source_overlap_ratio"), F.desc("common_source_count"))
        )
        
        return common_ancestors


class SchemaExtractor:
    """Class for extracting table schema information."""
    
    def __init__(self, spark: SparkSession):
        """
        Initialize SchemaExtractor.
        
        Args:
            spark: SparkSession instance
        """
        self.spark = spark
    
    def get_table_metadata(self, catalog: str, schema: str = None) -> DataFrame:
        """
        Query table metadata.
        
        Args:
            catalog: Catalog name
            schema: Schema name (optional)
            
        Returns:
            DataFrame: Table metadata
        """
        schema_filter = f"AND TABLE_SCHEMA = '{schema}'" if schema else ""
        
        query = f"""
        SELECT 
            TABLE_CATALOG,
            TABLE_SCHEMA,
            TABLE_NAME,
            TABLE_TYPE,
            TABLE_OWNER,
            STORAGE_PATH,
            CONCAT(TABLE_CATALOG, '.', TABLE_SCHEMA, '.', TABLE_NAME) AS full_table_name
        FROM {catalog}.information_schema.tables
        WHERE TABLE_TYPE != 'VIEW'
            {schema_filter}
        """
        
        return self.spark.sql(query)
    
    def get_column_metadata(self, table_full_name: str) -> DataFrame:
        """
        Query column metadata for a specific table.
        
        Args:
            table_full_name: Full table name (catalog.schema.table)
            
        Returns:
            DataFrame: Column metadata
        """
        parts = table_full_name.split(".")
        if len(parts) != 3:
            raise ValueError(f"Invalid table name format: {table_full_name}")
        
        catalog, schema, table = parts
        
        query = f"""
        SELECT 
            COLUMN_NAME,
            DATA_TYPE,
            IS_NULLABLE,
            COLUMN_DEFAULT,
            COMMENT
        FROM {catalog}.information_schema.columns
        WHERE TABLE_CATALOG = '{catalog}'
            AND TABLE_SCHEMA = '{schema}'
            AND TABLE_NAME = '{table}'
        ORDER BY ORDINAL_POSITION
        """
        
        return self.spark.sql(query)
    
    def get_schema_text_representation(self, table_full_name: str) -> str:
        """
        Convert table schema to text for embedding.
        
        Args:
            table_full_name: Full table name
            
        Returns:
            str: Text representation of schema
        """
        columns_df = self.get_column_metadata(table_full_name)
        columns = columns_df.collect()
        
        schema_parts = [f"Table: {table_full_name}"]
        schema_parts.append("Columns:")
        
        for col in columns:
            col_desc = f"  - {col['COLUMN_NAME']} ({col['DATA_TYPE']})"
            if col['COMMENT']:
                col_desc += f": {col['COMMENT']}"
            schema_parts.append(col_desc)
        
        return "\n".join(schema_parts)
    
    def get_bulk_schema_texts(self, table_names: list) -> DataFrame:
        """
        Generate schema texts for multiple tables in bulk.
        
        Args:
            table_names: List of full table names
            
        Returns:
            DataFrame: Schema texts per table
        """
        # Create temporary view from table list
        table_df = self.spark.createDataFrame(
            [(name,) for name in table_names],
            ["full_table_name"]
        )
        table_df.createOrReplaceTempView("target_tables")
        
        # Bulk query column info for all target tables
        # Group tables by catalog for efficient querying
        catalogs = set(name.split(".")[0] for name in table_names)
        
        # Build single UNION ALL query for all catalogs (more efficient than sequential unions)
        union_queries = []
        for catalog in catalogs:
            query = f"""
            SELECT 
                CONCAT(c.TABLE_CATALOG, '.', c.TABLE_SCHEMA, '.', c.TABLE_NAME) AS full_table_name,
                c.COLUMN_NAME,
                c.DATA_TYPE,
                c.ORDINAL_POSITION,
                c.COMMENT
            FROM {catalog}.information_schema.columns c
            INNER JOIN target_tables t 
                ON CONCAT(c.TABLE_CATALOG, '.', c.TABLE_SCHEMA, '.', c.TABLE_NAME) = t.full_table_name
            """
            union_queries.append(f"({query})")
        
        if not union_queries:
            return self.spark.createDataFrame([], "full_table_name STRING, schema_text STRING")
        
        # Execute as single query with UNION ALL
        combined_query = " UNION ALL ".join(union_queries)
        all_columns = self.spark.sql(combined_query)
        
        # Generate schema text
        schema_texts = (
            all_columns
            .withColumn(
                "col_description",
                F.concat(
                    F.lit("  - "),
                    F.col("COLUMN_NAME"),
                    F.lit(" ("),
                    F.col("DATA_TYPE"),
                    F.lit(")"),
                    F.when(
                        F.col("COMMENT").isNotNull(),
                        F.concat(F.lit(": "), F.col("COMMENT"))
                    ).otherwise(F.lit(""))
                )
            )
            .orderBy("full_table_name", "ORDINAL_POSITION")
            .groupBy("full_table_name")
            .agg(
                F.concat_ws(
                    "\n",
                    F.concat(F.lit("Table: "), F.first("full_table_name")),
                    F.lit("Columns:"),
                    F.concat_ws("\n", F.collect_list("col_description"))
                ).alias("schema_text")
            )
        )
        
        return schema_texts
