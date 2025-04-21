package com.iceberg.spark.IcebergSpark.writer;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.function.Consumer;

public interface Writer extends Consumer<Dataset<Row>> {}
