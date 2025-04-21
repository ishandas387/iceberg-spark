package com.iceberg.spark.IcebergSpark.reader;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.springframework.stereotype.Component;

import java.util.function.Supplier;

public class ParquetReader implements Reader {

  private final SparkSession session;

  private final String fileInputPath;

  public ParquetReader(SparkSession session, String fileInputPath) {
    this.session = session;
    this.fileInputPath = fileInputPath;
  }

  @Override
  public Dataset<Row> get() {
    if (fileInputPath == null) {
      return null;
    }
    return session.read().parquet(fileInputPath);
  }
}
