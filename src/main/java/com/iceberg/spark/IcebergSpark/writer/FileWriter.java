package com.iceberg.spark.IcebergSpark.writer;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class FileWriter implements Writer {

  private final String filePath;

  private final SparkSession session;

  public FileWriter(SparkSession session, String filePath) {
    this.session = session;
    this.filePath = filePath;
  }

  @Override
  public void accept(Dataset<Row> rowDataset) {}
}
