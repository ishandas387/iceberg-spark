package com.iceberg.spark.IcebergSpark.configuration;

import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.sql.SparkSession;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class Config {
  private static final Logger LOGGER = LogManager.getLogger(Config.class);

  @Bean
  public SparkSession sparkSession() {
    LOGGER.info("create bean configuration for SparkSession");
    return SparkSession.builder().appName("My Spark Iceberg App")
            .master("local")
            .config("spark.sql.warehouse.dir", "file:///tmp/iceberg")
            .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog.local.type", "hadoop")
            .config("spark.sql.catalog.local.warehouse", "file:///tmp/iceberg")
            .config("spark.sql.defaultCatalog", "local")
            .config("spark.sql.defaultSchema", "local")
            .getOrCreate();
  }

  @Bean
  public Catalog catalog(SparkSession sparkSession) {
    // Configure catalog based on your needs (e.g., HadoopCatalog, FileSystemCatalog)
    return new HadoopCatalog(sparkSession.sparkContext().hadoopConfiguration(), "file:///tmp/iceberg");
  }
}
