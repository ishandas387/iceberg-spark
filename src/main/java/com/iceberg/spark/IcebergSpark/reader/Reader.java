package com.iceberg.spark.IcebergSpark.reader;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.function.Supplier;

public interface Reader extends Supplier<Dataset<Row>> {}
