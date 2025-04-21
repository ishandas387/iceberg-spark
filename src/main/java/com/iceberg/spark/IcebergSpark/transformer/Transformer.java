package com.iceberg.spark.IcebergSpark.transformer;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.function.Function;

public interface Transformer extends Function<Dataset<Row>, Dataset<Row>> {}
