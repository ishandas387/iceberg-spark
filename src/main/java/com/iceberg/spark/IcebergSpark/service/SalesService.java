package com.iceberg.spark.IcebergSpark.service;

import com.iceberg.spark.IcebergSpark.reader.Reader;
import com.iceberg.spark.IcebergSpark.transformer.Transformer;
import com.iceberg.spark.IcebergSpark.writer.Writer;
import org.apache.spark.sql.SparkSession;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;

public class SalesService {

  private final SparkSession sparkSession;

  private final String input;
  private final String output;
  private final Reader reader;
  private final Transformer transformer;
  private final Writer writer;

  public SalesService(
      SparkSession sparkSession,
      String input,
      String output,
      Reader reader,
      Transformer transformer,
      Writer writer) {
    this.sparkSession = sparkSession;
    this.input = input;
    this.output = output;
    this.reader = reader;
    this.transformer = transformer;
    this.writer = writer;
  }

  public void processSalesData(String inputPath, String outputPath) {
    Map<String, String> m = new HashMap<String, String>();
    // m.computeIfAbsent()
  }
}
