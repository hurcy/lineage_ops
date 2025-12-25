"""
Recommendation and Cost Estimation Module

Generates consolidation recommendations for duplicate tables and estimates cost savings.
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.window import Window
from typing import Dict, List, Optional, Any
from dataclasses import dataclass
from datetime import datetime
import json


@dataclass
class TableUsageStats:
    """Table usage statistics."""
    table_name: str
    read_count: int
    write_count: int
    last_read: Optional[datetime]
    last_write: Optional[datetime]
    unique_users: int
    job_count: int


@dataclass
class CostEstimation:
    """Cost savings estimation."""
    duplicate_table: str
    sot_table: str
    estimated_dbu_savings_per_month: float
    storage_savings_gb: float
    pipeline_count_to_remove: int
    confidence: str
    notes: str


class UsageAnalyzer:
    """Class for analyzing table usage."""
    
    def __init__(self, spark: SparkSession):
        """
        Initialize UsageAnalyzer.
        
        Args:
            spark: SparkSession instance
        """
        self.spark = spark
    
    def get_table_usage_stats(
        self, 
        table_names: List[str],
        days_back: int = 90
    ) -> DataFrame:
        """
        Query usage statistics per table.
        
        Args:
            table_names: List of table names to analyze
            days_back: Number of past days to query
            
        Returns:
            DataFrame: Table usage statistics
        """
        # Create temporary view from table list
        table_df = self.spark.createDataFrame(
            [(name,) for name in table_names],
            ["full_table_name"]
        )
        table_df.createOrReplaceTempView("target_tables_usage")
        
        # Collect read/write statistics from lineage table
        usage_query = f"""
        WITH read_stats AS (
            SELECT 
                source_table_full_name AS table_name,
                COUNT(*) AS read_count,
                MAX(event_time) AS last_read,
                COUNT(DISTINCT entity_id) AS reader_jobs
            FROM system.access.table_lineage
            WHERE event_time >= current_date() - INTERVAL {days_back} DAY
                AND source_table_full_name IN (SELECT full_table_name FROM target_tables_usage)
            GROUP BY source_table_full_name
        ),
        write_stats AS (
            SELECT 
                target_table_full_name AS table_name,
                COUNT(*) AS write_count,
                MAX(event_time) AS last_write,
                COUNT(DISTINCT entity_id) AS writer_jobs
            FROM system.access.table_lineage
            WHERE event_time >= current_date() - INTERVAL {days_back} DAY
                AND target_table_full_name IN (SELECT full_table_name FROM target_tables_usage)
            GROUP BY target_table_full_name
        )
        SELECT 
            t.full_table_name,
            COALESCE(r.read_count, 0) AS read_count,
            COALESCE(w.write_count, 0) AS write_count,
            r.last_read,
            w.last_write,
            COALESCE(r.reader_jobs, 0) + COALESCE(w.writer_jobs, 0) AS total_job_count
        FROM target_tables_usage t
        LEFT JOIN read_stats r ON t.full_table_name = r.table_name
        LEFT JOIN write_stats w ON t.full_table_name = w.table_name
        """
        
        return self.spark.sql(usage_query)
    
    def get_table_creation_info(self, table_names: List[str]) -> DataFrame:
        """
        Query table creation information.
        
        Args:
            table_names: List of table names to analyze
            
        Returns:
            DataFrame: Table creation information
        """
        # Extract table creation info from audit log
        table_df = self.spark.createDataFrame(
            [(name,) for name in table_names],
            ["full_table_name"]
        )
        table_df.createOrReplaceTempView("target_tables_creation")
        
        creation_query = """
        SELECT 
            request_params['full_name_arg'] AS table_name,
            MIN(event_time) AS created_at,
            FIRST(user_identity.email) AS created_by
        FROM system.access.audit
        WHERE action_name = 'createTable'
            AND request_params['full_name_arg'] IN (SELECT full_table_name FROM target_tables_creation)
        GROUP BY request_params['full_name_arg']
        """
        
        try:
            return self.spark.sql(creation_query)
        except Exception:
            # Return empty DataFrame if audit table is inaccessible
            return self.spark.createDataFrame(
                [],
                "table_name STRING, created_at TIMESTAMP, created_by STRING"
            )


class CostEstimator:
    """Class for estimating cost savings."""
    
    # Default cost assumptions (USD)
    DEFAULT_DBU_COST_PER_HOUR = 0.55  # Standard DBU
    DEFAULT_STORAGE_COST_PER_GB_MONTH = 0.023  # S3 Standard
    AVERAGE_JOB_DURATION_HOURS = 0.5
    
    def __init__(
        self, 
        spark: SparkSession,
        dbu_cost_per_hour: float = None,
        storage_cost_per_gb_month: float = None
    ):
        """
        Initialize CostEstimator.
        
        Args:
            spark: SparkSession instance
            dbu_cost_per_hour: DBU cost per hour
            storage_cost_per_gb_month: Storage cost per GB per month
        """
        self.spark = spark
        self.dbu_cost = dbu_cost_per_hour or self.DEFAULT_DBU_COST_PER_HOUR
        self.storage_cost = storage_cost_per_gb_month or self.DEFAULT_STORAGE_COST_PER_GB_MONTH
    
    def estimate_pipeline_cost(
        self,
        table_name: str,
        usage_stats_df: DataFrame
    ) -> Dict[str, float]:
        """
        Estimate pipeline costs related to a table.
        
        Args:
            table_name: Table name
            usage_stats_df: Usage statistics DataFrame
            
        Returns:
            Dict: Cost estimation information
        """
        stats = usage_stats_df.filter(
            F.col("full_table_name") == table_name
        ).first()
        
        if not stats:
            return {
                "monthly_dbu_cost": 0.0,
                "job_count": 0,
                "write_frequency": 0
            }
        
        # Estimate DBU cost based on monthly write operations
        # Assumption: Each write operation takes avg 0.5 hours, uses 4 DBUs
        writes_per_month = stats["write_count"] * (30 / 90)  # Convert 90-day data to monthly
        estimated_dbu_per_month = writes_per_month * self.AVERAGE_JOB_DURATION_HOURS * 4
        monthly_dbu_cost = estimated_dbu_per_month * self.dbu_cost
        
        return {
            "monthly_dbu_cost": monthly_dbu_cost,
            "job_count": stats["total_job_count"],
            "write_frequency": stats["write_count"]
        }
    
    def estimate_storage_size(self, table_full_name: str) -> float:
        """
        Estimate table storage size (GB).
        
        Args:
            table_full_name: Full table name
            
        Returns:
            float: Estimated storage size (GB)
        """
        try:
            # Use DESCRIBE DETAIL for Delta tables
            detail = self.spark.sql(f"DESCRIBE DETAIL {table_full_name}")
            size_bytes = detail.select("sizeInBytes").first()[0]
            return size_bytes / (1024 ** 3) if size_bytes else 0.0
        except Exception:
            # Return 0 if size query fails
            return 0.0
    
    def estimate_savings(
        self,
        duplicate_table: str,
        sot_table: str,
        usage_stats_df: DataFrame
    ) -> CostEstimation:
        """
        Estimate savings from removing duplicate table.
        
        Args:
            duplicate_table: Duplicate table to remove
            sot_table: SoT table to keep
            usage_stats_df: Usage statistics DataFrame
            
        Returns:
            CostEstimation: Cost savings estimation
        """
        # Estimate pipeline cost
        dup_cost = self.estimate_pipeline_cost(duplicate_table, usage_stats_df)
        
        # Estimate storage size
        storage_gb = self.estimate_storage_size(duplicate_table)
        storage_savings = storage_gb * self.storage_cost
        
        # Determine confidence level
        if dup_cost["job_count"] > 0 and storage_gb > 0:
            confidence = "HIGH"
        elif dup_cost["job_count"] > 0 or storage_gb > 0:
            confidence = "MEDIUM"
        else:
            confidence = "LOW"
        
        notes = []
        if dup_cost["job_count"] == 0:
            notes.append("No pipeline activity detected")
        if storage_gb == 0:
            notes.append("Storage size could not be determined")
        
        return CostEstimation(
            duplicate_table=duplicate_table,
            sot_table=sot_table,
            estimated_dbu_savings_per_month=dup_cost["monthly_dbu_cost"],
            storage_savings_gb=storage_gb,
            pipeline_count_to_remove=dup_cost["job_count"],
            confidence=confidence,
            notes="; ".join(notes) if notes else "Estimation based on available data"
        )


class RecommendationGenerator:
    """Class for generating consolidation recommendations."""
    
    def __init__(
        self, 
        spark: SparkSession,
        llm_endpoint: Optional[str] = None
    ):
        """
        Initialize RecommendationGenerator.
        
        Args:
            spark: SparkSession instance
            llm_endpoint: LLM endpoint (optional, for AI comment generation)
        """
        self.spark = spark
        self.llm_endpoint = llm_endpoint
        self.usage_analyzer = UsageAnalyzer(spark)
        self.cost_estimator = CostEstimator(spark)
    
    def determine_sot(
        self,
        table_a: str,
        table_b: str,
        usage_stats_df: DataFrame,
        creation_info_df: DataFrame
    ) -> Dict[str, Any]:
        """
        Determine the SoT (Source of Truth) between two tables.
        
        Decision criteria:
        1. Table with more reads
        2. Table updated more recently
        3. Older table (stability)
        
        Args:
            table_a: First table
            table_b: Second table
            usage_stats_df: Usage statistics DataFrame
            creation_info_df: Creation info DataFrame
            
        Returns:
            Dict: SoT decision result
        """
        stats_a = usage_stats_df.filter(
            F.col("full_table_name") == table_a
        ).first()
        stats_b = usage_stats_df.filter(
            F.col("full_table_name") == table_b
        ).first()
        
        score_a = 0
        score_b = 0
        reasons = []
        
        # Compare read frequency
        if stats_a and stats_b:
            if stats_a["read_count"] > stats_b["read_count"]:
                score_a += 2
                reasons.append(f"{table_a} has more reads ({stats_a['read_count']} vs {stats_b['read_count']})")
            elif stats_b["read_count"] > stats_a["read_count"]:
                score_b += 2
                reasons.append(f"{table_b} has more reads ({stats_b['read_count']} vs {stats_a['read_count']})")
            
            # Compare recent updates
            if stats_a["last_write"] and stats_b["last_write"]:
                if stats_a["last_write"] > stats_b["last_write"]:
                    score_a += 1
                    reasons.append(f"{table_a} was updated more recently")
                else:
                    score_b += 1
                    reasons.append(f"{table_b} was updated more recently")
        
        # Compare creation date (older is more stable)
        creation_a = creation_info_df.filter(
            F.col("table_name") == table_a
        ).first()
        creation_b = creation_info_df.filter(
            F.col("table_name") == table_b
        ).first()
        
        if creation_a and creation_b:
            if creation_a["created_at"] < creation_b["created_at"]:
                score_a += 1
                reasons.append(f"{table_a} is older (established)")
            else:
                score_b += 1
                reasons.append(f"{table_b} is older (established)")
        
        # Make decision
        if score_a > score_b:
            sot = table_a
            duplicate = table_b
        elif score_b > score_a:
            sot = table_b
            duplicate = table_a
        else:
            # Tie-breaker: alphabetical order
            sot = min(table_a, table_b)
            duplicate = max(table_a, table_b)
            reasons.append("Tie-breaker: alphabetical order")
        
        return {
            "sot_table": sot,
            "duplicate_table": duplicate,
            "reasons": reasons,
            "confidence": "HIGH" if abs(score_a - score_b) >= 2 else "MEDIUM" if abs(score_a - score_b) >= 1 else "LOW"
        }
    
    def generate_recommendation(
        self,
        table_a: str,
        table_b: str,
        similarity_score: float,
        common_ancestors: Optional[List[str]] = None
    ) -> Dict[str, Any]:
        """
        Generate consolidation recommendation for a duplicate table pair.
        
        Args:
            table_a: First table
            table_b: Second table
            similarity_score: Schema similarity score
            common_ancestors: List of common source tables
            
        Returns:
            Dict: Consolidation recommendation
        """
        # Collect usage statistics
        usage_stats = self.usage_analyzer.get_table_usage_stats([table_a, table_b])
        creation_info = self.usage_analyzer.get_table_creation_info([table_a, table_b])
        
        # Determine SoT
        sot_decision = self.determine_sot(table_a, table_b, usage_stats, creation_info)
        
        # Estimate cost savings
        cost_estimation = self.cost_estimator.estimate_savings(
            sot_decision["duplicate_table"],
            sot_decision["sot_table"],
            usage_stats
        )
        
        # Generate comment
        comment = self._generate_comment(
            sot_decision,
            similarity_score,
            common_ancestors,
            cost_estimation
        )
        
        return {
            "table_a": table_a,
            "table_b": table_b,
            "similarity_score": similarity_score,
            "sot_table": sot_decision["sot_table"],
            "duplicate_table": sot_decision["duplicate_table"],
            "recommendation": comment,
            "decision_reasons": sot_decision["reasons"],
            "decision_confidence": sot_decision["confidence"],
            "cost_estimation": {
                "monthly_dbu_savings_usd": cost_estimation.estimated_dbu_savings_per_month,
                "storage_savings_gb": cost_estimation.storage_savings_gb,
                "pipelines_to_remove": cost_estimation.pipeline_count_to_remove
            },
            "common_ancestors": common_ancestors or []
        }
    
    def _generate_comment(
        self,
        sot_decision: Dict,
        similarity_score: float,
        common_ancestors: Optional[List[str]],
        cost_estimation: CostEstimation
    ) -> str:
        """
        Generate consolidation recommendation comment.
        
        Args:
            sot_decision: SoT decision info
            similarity_score: Similarity score
            common_ancestors: Common source tables
            cost_estimation: Cost estimation
            
        Returns:
            str: Consolidation recommendation comment
        """
        parts = []
        
        # Similarity info
        similarity_pct = int(similarity_score * 100)
        parts.append(
            f"Tables {sot_decision['sot_table']} and {sot_decision['duplicate_table']} "
            f"have {similarity_pct}% similar column structure."
        )
        
        # Common source info
        if common_ancestors:
            parts.append(
                f"Both tables are derived from the same source ({', '.join(common_ancestors[:3])}"
                f"{'...' if len(common_ancestors) > 3 else ''})."
            )
        
        # Decision rationale
        if sot_decision["reasons"]:
            parts.append(f"Decision rationale: {'; '.join(sot_decision['reasons'][:3])}")
        
        # Recommendation
        parts.append(
            f"Recommendation: Remove '{sot_decision['duplicate_table']}' and "
            f"use '{sot_decision['sot_table']}' as the Source of Truth (SoT)."
        )
        
        # Cost savings
        if cost_estimation.estimated_dbu_savings_per_month > 0:
            parts.append(
                f"Estimated monthly DBU savings: ${cost_estimation.estimated_dbu_savings_per_month:.2f}, "
                f"Pipelines to remove: {cost_estimation.pipeline_count_to_remove}"
            )
        
        return " ".join(parts)
    
    def generate_recommendations_batch(
        self,
        candidates_df: DataFrame
    ) -> DataFrame:
        """
        Generate consolidation recommendations for all duplicate candidates.
        
        Args:
            candidates_df: Duplicate candidates DataFrame
            
        Returns:
            DataFrame: DataFrame with consolidation recommendations
        """
        # Collect candidates once and extract all unique tables in one pass
        candidates_rows = candidates_df.collect()
        all_tables = list(set(
            [row["table_a"] for row in candidates_rows] + 
            [row["table_b"] for row in candidates_rows]
        ))
        
        # Batch collect statistics and convert to dictionaries for O(1) lookup
        usage_stats_df = self.usage_analyzer.get_table_usage_stats(all_tables)
        creation_info_df = self.usage_analyzer.get_table_creation_info(all_tables)
        
        # Convert DataFrames to dictionaries once (avoid N filter operations)
        usage_stats_dict = {
            row["full_table_name"]: row.asDict() 
            for row in usage_stats_df.collect()
        }
        creation_info_dict = {
            row["table_name"]: row.asDict() 
            for row in creation_info_df.collect()
        }
        
        # Batch compute storage sizes for all duplicate candidates
        # (will be determined after SoT decision, so we pre-compute for all tables)
        storage_sizes = {}
        for table in all_tables:
            storage_sizes[table] = self.cost_estimator.estimate_storage_size(table)
        
        # Generate recommendations for each candidate (using dict lookups, no Spark actions)
        recommendations = []
        for row in candidates_rows:
            sot_decision = self._determine_sot_from_dict(
                row["table_a"],
                row["table_b"],
                usage_stats_dict,
                creation_info_dict
            )
            
            cost_estimation = self._estimate_savings_from_dict(
                sot_decision["duplicate_table"],
                sot_decision["sot_table"],
                usage_stats_dict,
                storage_sizes
            )
            
            common_ancestors = getattr(row, "common_sources", None) or []
            if common_ancestors:
                common_ancestors = list(common_ancestors)
            
            similarity = getattr(row, "cosine_similarity", None) or getattr(row, "combined_score", None) or 0
            comment = self._generate_comment(
                sot_decision,
                similarity,
                common_ancestors,
                cost_estimation
            )
            
            recommendations.append({
                "table_a": row["table_a"],
                "table_b": row["table_b"],
                "sot_table": sot_decision["sot_table"],
                "duplicate_table": sot_decision["duplicate_table"],
                "similarity_score": float(getattr(row, "cosine_similarity", None) or 0),
                "combined_score": float(getattr(row, "combined_score", None) or 0),
                "confidence_level": getattr(row, "confidence_level", None) or sot_decision["confidence"],
                "recommendation": comment,
                "monthly_dbu_savings_usd": cost_estimation.estimated_dbu_savings_per_month,
                "storage_savings_gb": cost_estimation.storage_savings_gb,
                "pipelines_to_remove": cost_estimation.pipeline_count_to_remove
            })
        
        # Create DataFrame
        schema = T.StructType([
            T.StructField("table_a", T.StringType(), False),
            T.StructField("table_b", T.StringType(), False),
            T.StructField("sot_table", T.StringType(), False),
            T.StructField("duplicate_table", T.StringType(), False),
            T.StructField("similarity_score", T.FloatType(), True),
            T.StructField("combined_score", T.FloatType(), True),
            T.StructField("confidence_level", T.StringType(), True),
            T.StructField("recommendation", T.StringType(), True),
            T.StructField("monthly_dbu_savings_usd", T.FloatType(), True),
            T.StructField("storage_savings_gb", T.FloatType(), True),
            T.StructField("pipelines_to_remove", T.IntegerType(), True)
        ])
        
        return self.spark.createDataFrame(recommendations, schema)
    
    def _determine_sot_from_dict(
        self,
        table_a: str,
        table_b: str,
        usage_stats_dict: Dict[str, Dict],
        creation_info_dict: Dict[str, Dict]
    ) -> Dict[str, Any]:
        """
        Determine the SoT using pre-collected dictionaries (no Spark actions).
        """
        stats_a = usage_stats_dict.get(table_a)
        stats_b = usage_stats_dict.get(table_b)
        
        score_a = 0
        score_b = 0
        reasons = []
        
        # Compare read frequency
        if stats_a and stats_b:
            read_a = stats_a.get("read_count", 0) or 0
            read_b = stats_b.get("read_count", 0) or 0
            if read_a > read_b:
                score_a += 2
                reasons.append(f"{table_a} has more reads ({read_a} vs {read_b})")
            elif read_b > read_a:
                score_b += 2
                reasons.append(f"{table_b} has more reads ({read_b} vs {read_a})")
            
            # Compare recent updates
            last_write_a = stats_a.get("last_write")
            last_write_b = stats_b.get("last_write")
            if last_write_a and last_write_b:
                if last_write_a > last_write_b:
                    score_a += 1
                    reasons.append(f"{table_a} was updated more recently")
                else:
                    score_b += 1
                    reasons.append(f"{table_b} was updated more recently")
        
        # Compare creation date (older is more stable)
        creation_a = creation_info_dict.get(table_a)
        creation_b = creation_info_dict.get(table_b)
        
        if creation_a and creation_b:
            created_at_a = creation_a.get("created_at")
            created_at_b = creation_b.get("created_at")
            if created_at_a and created_at_b:
                if created_at_a < created_at_b:
                    score_a += 1
                    reasons.append(f"{table_a} is older (established)")
                else:
                    score_b += 1
                    reasons.append(f"{table_b} is older (established)")
        
        # Make decision
        if score_a > score_b:
            sot = table_a
            duplicate = table_b
        elif score_b > score_a:
            sot = table_b
            duplicate = table_a
        else:
            # Tie-breaker: alphabetical order
            sot = min(table_a, table_b)
            duplicate = max(table_a, table_b)
            reasons.append("Tie-breaker: alphabetical order")
        
        return {
            "sot_table": sot,
            "duplicate_table": duplicate,
            "reasons": reasons,
            "confidence": "HIGH" if abs(score_a - score_b) >= 2 else "MEDIUM" if abs(score_a - score_b) >= 1 else "LOW"
        }
    
    def _estimate_savings_from_dict(
        self,
        duplicate_table: str,
        sot_table: str,
        usage_stats_dict: Dict[str, Dict],
        storage_sizes: Dict[str, float]
    ) -> CostEstimation:
        """
        Estimate savings using pre-collected dictionaries (no Spark actions).
        """
        stats = usage_stats_dict.get(duplicate_table)
        
        if not stats:
            monthly_dbu_cost = 0.0
            job_count = 0
        else:
            # Estimate DBU cost based on monthly write operations
            writes_per_month = (stats.get("write_count", 0) or 0) * (30 / 90)
            estimated_dbu_per_month = writes_per_month * self.cost_estimator.AVERAGE_JOB_DURATION_HOURS * 4
            monthly_dbu_cost = estimated_dbu_per_month * self.cost_estimator.dbu_cost
            job_count = stats.get("total_job_count", 0) or 0
        
        storage_gb = storage_sizes.get(duplicate_table, 0.0)
        
        # Determine confidence level
        if job_count > 0 and storage_gb > 0:
            confidence = "HIGH"
        elif job_count > 0 or storage_gb > 0:
            confidence = "MEDIUM"
        else:
            confidence = "LOW"
        
        notes = []
        if job_count == 0:
            notes.append("No pipeline activity detected")
        if storage_gb == 0:
            notes.append("Storage size could not be determined")
        
        return CostEstimation(
            duplicate_table=duplicate_table,
            sot_table=sot_table,
            estimated_dbu_savings_per_month=monthly_dbu_cost,
            storage_savings_gb=storage_gb,
            pipeline_count_to_remove=job_count,
            confidence=confidence,
            notes="; ".join(notes) if notes else "Estimation based on available data"
        )
    
    def generate_summary_report(self, recommendations_df: DataFrame) -> Dict[str, Any]:
        """
        Generate summary report for all analysis results.
        
        Args:
            recommendations_df: Consolidation recommendations DataFrame
            
        Returns:
            Dict: Summary report
        """
        # Aggregate statistics
        stats = recommendations_df.agg(
            F.count("*").alias("total_duplicate_pairs"),
            F.sum("monthly_dbu_savings_usd").alias("total_monthly_dbu_savings"),
            F.sum("storage_savings_gb").alias("total_storage_savings"),
            F.sum("pipelines_to_remove").alias("total_pipelines_to_remove"),
            F.avg("similarity_score").alias("avg_similarity_score")
        ).first()
        
        # Confidence level distribution
        confidence_dist = (
            recommendations_df
            .groupBy("confidence_level")
            .count()
            .collect()
        )
        
        return {
            "summary": {
                "total_duplicate_pairs_found": stats["total_duplicate_pairs"],
                "estimated_monthly_dbu_savings_usd": float(stats["total_monthly_dbu_savings"] or 0),
                "estimated_storage_savings_gb": float(stats["total_storage_savings"] or 0),
                "total_pipelines_to_remove": int(stats["total_pipelines_to_remove"] or 0),
                "average_similarity_score": float(stats["avg_similarity_score"] or 0)
            },
            "confidence_distribution": {
                row["confidence_level"]: row["count"]
                for row in confidence_dist
            },
            "generated_at": datetime.now().isoformat()
        }
