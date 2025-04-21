package com.iceberg.spark.IcebergSpark.service;

import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class IcebergWriter {

  @Autowired private SparkSession sparkSession;

  @Autowired private Catalog catalog;

  public void writeData(String dataPath, String tableName) {
    // Read data (e.g., from CSV, Parquet) using Spark API
    Dataset<Row> data = sparkSession.read().format("csv").load(dataPath);

    // Define table schema (replace with your actual schema)
    Schema schema =
        new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.required(2, "name", Types.StringType.get()));

    TableIdentifier tf = TableIdentifier.of("local", "student1");
    // Write data to Iceberg table
    Table table = catalog.loadTable(tf);
    if (table == null) {
      table = catalog.createTable(tf, schema);
    }
    // table.wrappend().save(data);
    // data.write().format("iceberg").mode("append").saveAsTable(catalog, tf);
    // data.write().append().save(data);
  }
}
