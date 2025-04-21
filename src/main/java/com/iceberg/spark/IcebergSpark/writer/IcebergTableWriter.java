package com.iceberg.spark.IcebergSpark.writer;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class IcebergTableWriter implements Writer {

  private final SparkSession session;

  public IcebergTableWriter(SparkSession session) {
    this.session = session;
  }

  @Override
  public void accept(Dataset<Row> rowDataset) {}
}
