package com.iceberg.spark.IcebergSpark.transformer;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class CalculateAvgTransformer implements Transformer {
  @Override
  public Dataset<Row> apply(Dataset<Row> rowDataset) {
    return rowDataset.groupBy().avg("column_name").as("average_value");
  }
}
