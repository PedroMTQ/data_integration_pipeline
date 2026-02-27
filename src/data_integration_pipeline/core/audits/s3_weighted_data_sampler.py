import numpy as np
import pyarrow as pa
import heapq
from typing import List, Iterable, Union
from collections import defaultdict
from data_integration_pipeline.io.file_reader import S3FileReader
from data_integration_pipeline.io.logger import logger
import polars as pl


class S3WeightedParquetSampler:
    """
    A high-performance streaming utility for Global Weighted (A-Res) or Standard Reservoir (Algorithm R) sampling.

    This class provides a memory-efficient way to extract representative samples from potentially infinite data streams. It maintains a constant memory footprint regardless of the total stream size (e.g., 100M+ records).

    Mathematical Foundation:
    ------------------------
    1. Weighted Mode (A-Res):
       Triggered when a `weights` dictionary is provided. It uses the formula: score = random(0,1) ^ (1/weight). This biases the sample towards higher-weight groups without completely excluding rare ones.

    2. Standard Mode (Algorithm R):
       Triggered when `weights` is empty or None. All records are treated with equal probability (1/N), acting as a neutral statistical "slice."

    Key Features:
    -------------
    * Memory Efficiency: Maintains a fixed-size reservoir (default 1000 records) regardless of stream size.
    * Weighted Sampling: Supports group-based weighting to prioritize certain records.
    * Weight Filtering: If weights are active, setting `default_weight=0` will automatically skip/filter records from keys not defined in the weights dictionary.

    Parameters:
    -----------
    weight_column (str):
        The column/key used to identify groups (e.g., 'State').
    target_total_rows (int):
        The maximum capacity of the sample reservoir (default: 1000).
    weights (dict[str, float]):
        Optional mapping of group values to priority weights.
        Higher weights increase inclusion probability.
    default_weight (float):
        Weight applied to records with keys missing from the weights dict.
        Use 0.0 to exclude unknown keys.
    """

    MAX_WEIGHT = 100.0

    def __init__(
        self,
        data: Iterable[pa.Table],
        weight_column: str,
        weights: dict = {},
        target_total_rows: int = 1000,
        default_weight: float = 1.0,
        batch_size: int = 1000,
    ):
        self.data: Iterable[pa.Table] = data
        self.weight_column = weight_column
        self.weights = weights
        self.default_weight = default_weight
        self.target_total_rows = target_total_rows
        self.batch_size = batch_size
        # Heap stores: (score, tie_breaker, file_index, row_index)
        self.heap = []
        self.counter = 0
        self._is_sampled = False
        self.raw_counts_list = []

    def _validate_weights(self, weights: dict[str, float]) -> dict[str, float]:
        """Cleans and caps weight values for mathematical stability."""
        cleaned = {}
        for k, w in weights.items():
            if w < 0:
                raise ValueError(f"Weight for {k} must be non-negative.")
            if w > self.MAX_WEIGHT:
                logger.warning(f"Capping weight {w} for {k} to {self.MAX_WEIGHT}")
                w = self.MAX_WEIGHT
            cleaned[k] = w
        return cleaned

    def _sample_from_s3(self):
        """Streams and samples using Polars for vectorized math and distribution."""
        if self._is_sampled:
            return
        current_file_row_offset = 0
        for table in self.data:
            df = pl.from_arrow(table).with_row_index("_local_idx")

            batch_counts = df.select(pl.col(self.weight_column).cast(pl.Utf8).value_counts()).unnest(self.weight_column)
            self.raw_counts_list.append(batch_counts)

            df = df.with_columns([pl.col(self.weight_column).replace_strict(self.weights, default=self.default_weight).cast(pl.Float64).alias("_w")]).filter(pl.col("_w") > 0)
            if df.is_empty():
                current_file_row_offset += table.num_rows
                continue

            num_valid = len(df)
            scores = np.random.rand(num_valid) ** (1.0 / df["_w"].to_numpy())


            indices = df["_local_idx"].to_numpy()
            vals = df[self.weight_column].to_list()

            for i in range(num_valid):
                self.counter += 1  # Global counter
                global_row_idx = current_file_row_offset + indices[i]

                if len(self.heap) < self.target_total_rows:
                    heapq.heappush(self.heap, (scores[i], self.counter, s3_path, global_row_idx, vals[i]))
                elif scores[i] > self.heap[0][0]:
                    heapq.heapreplace(self.heap, (scores[i], self.counter, s3_path, global_row_idx, vals[i]))

            current_file_row_offset += table.num_rows
        self._is_sampled = True

    def get_data(self) -> Iterable[pa.Table]:
        """
        Gathers winning rows from S3 by streaming batches.
        Avoids loading full files into memory by filtering per-batch.
        """
        self._sample_from_s3()
        if not self.heap:
            return
        # 1. Group winners by path
        winners_by_path = defaultdict(list)
        for _, _, path, r_idx, _ in self.heap:
            winners_by_path[path].append(r_idx)

        for path, row_indexes in winners_by_path.items():
            # Sort indexes to make streaming extraction easier
            row_indexes.sort()

            logger.info(f"Gathering {len(row_indexes)} rows from {path}")

            with S3FileReader(path, bucket_name=self.bucket_name, as_table=True) as reader:
                current_offset = 0
                collected_batches = []

                for batch_table in reader:
                    batch_len = len(batch_table)
                    batch_end = current_offset + batch_len

                    # Find which winning indexes fall within this specific batch
                    # This is much faster than checking every index in a loop
                    batch_winners = [idx - current_offset for idx in row_indexes if current_offset <= idx < batch_end]

                    if batch_winners:
                        # Use Polars for a zero-copy 'gather' on just this batch
                        # This avoids converting the whole batch to a dict
                        chunk = pl.from_arrow(batch_table)[batch_winners].to_arrow()
                        collected_batches.append(chunk)

                    current_offset = batch_end

                    # Optimization: If we found all winners for this file, stop reading early
                    if current_offset > row_indexes[-1]:
                        break

                if not collected_batches:
                    continue

                # Merge the tiny collected chunks into one table for this file
                file_sample = pa.concat_tables(collected_batches)

                # Yield in the requested batch_size (zero-copy slice)
                for i in range(0, file_sample.num_rows, self.batch_size):
                    yield file_sample.slice(i, self.batch_size)

    def get_sample_data_distribution(self) -> dict[str, int]:
        """Returns distribution using the scalar values cached in the heap."""
        if not self._is_sampled:
            self._sample_from_s3()
        dist = {}
        for _, _, _, _, val in self.heap:
            key = str(val) if val is not None else "None"
            dist[key] = dist.get(key, 0) + 1
        return dict(sorted(dist.items(), key=lambda x: x[1], reverse=True))

    def get_raw_data_distribution(self) -> dict[str, int]:
        """
        Aggregates the raw distribution across all S3 batches using
        Polars' high-speed groupby-sum.
        """
        if not self._is_sampled:
            self._sample_from_s3()

        if not self.raw_counts_list:
            return {}

        # 1. Combine all batch DataFrames into one
        # Each batch looks like: [weight_column, "count"]
        combined_df = pl.concat(self.raw_counts_list)

        # 2. Final aggregation: Group by the value and sum the 'count' column
        final_dist_df = combined_df.group_by(self.weight_column).agg(pl.col("count").sum()).sort("count", descending=True)

        # 3. Convert to dictionary efficiently
        # iter_rows() on two columns returns (value, count) tuples
        return dict(final_dist_df.iter_rows())

    def get_total_sampled_records(self) -> int:
        """Returns the total number of records in the current sample."""
        if not self._is_sampled:
            self._sample_from_s3()
        return len(self.heap)

    def get_total_raw_records(self) -> int:
        """Returns the total number of records processed from the raw stream."""
        if not self._is_sampled:
            self._sample_from_s3()
        return self.counter

    def get_filtered_data(self, columns_filter: list[str], *args, **kwargs) -> Iterable[pa.Table]:
        if columns_filter:
            logger.debug(f"Columns filter: {columns_filter}")
        arrow_table: pa.Table
        for arrow_table in self.get_data(*args, **kwargs):
            if columns_filter:
                logger.debug(f"Table size before filtering: {arrow_table.shape}")
                arrow_table = arrow_table.select(columns_filter)
                logger.debug(f"Table size after filtering: {arrow_table.shape}")
            yield arrow_table


if __name__ == "__main__":
    s3_path = "silver/business_entity_registry/business_entity_registry.parquet"

    s3_sampler = S3WeightedParquetSampler(
        s3_path=s3_path,
        bucket_name="data",
        weight_column="city",
        weights={"NEW YORK": 50.0},
        default_weight=1,
        target_total_rows=10,
    )

    # Consume the generator to trigger the sampling
    print("get_total_raw_records", s3_sampler.get_total_raw_records())
    print("get_raw_data_distribution", s3_sampler.get_raw_data_distribution())
    print("get_total_sampled_records", s3_sampler.get_total_sampled_records())
    print("get_sample_data_distribution", s3_sampler.get_sample_data_distribution())
    for i in s3_sampler.get_data():
        print(i)
    for i in s3_sampler.get_filtered_data(columns_filter=["city", "company_name"]):
        print(i)
