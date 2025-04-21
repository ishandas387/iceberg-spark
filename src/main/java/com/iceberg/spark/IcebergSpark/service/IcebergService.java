package com.iceberg.spark.IcebergSpark.service;

import com.iceberg.spark.IcebergSpark.reader.ParquetReader;
import com.iceberg.spark.IcebergSpark.reader.Reader;
import com.iceberg.spark.IcebergSpark.writer.Writer;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Table;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hadoop.HadoopFileIO;
import org.apache.iceberg.jdbc.JdbcCatalog;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

@Service
public class IcebergService {

  public static final String STUDENT_TABLE = "student_table1";
  Reader parquetReader;

  Writer icebergWriter;

  @Autowired SparkSession session;

  @Autowired
  Catalog catalog;

  private static final Logger LOGGER = Logger.getLogger(IcebergService.class.getName());

  public void testIcebergCatalog() {

    String catalogLocation = "file:///path/to/iceberg/catalog";
    // Catalog catalog = setupFileSystemCatalog(catalogLocation);
    //HadoopCatalog catalog = new HadoopCatalog(new Configuration(), "file:///tmp/iceberg");
    LOGGER.info("catalog name {} "+ catalog.name());
    TableIdentifier tf = TableIdentifier.of("school", STUDENT_TABLE);
    Schema studentSchema =
        new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.required(2, "name", Types.StringType.get()),
            Types.NestedField.required(3, "country", Types.StringType.get()),
            Types.NestedField.required(4, "admission_ts", Types.TimestampType.withZone())
        );

    Table table;
   /* if (catalog.tableExists(tf)) {
      //LOGGER.info("Table exists load");
      catalog.dropTable(tf);
    }*/

    if (catalog.tableExists(tf)) {
      LOGGER.info("Table exists load");
      table = catalog.loadTable(tf);
      // update spec

        PartitionSpec currentSpec = table.spec();
        LOGGER.info("Current partition specification: " + currentSpec);

        // Define the new partition specification for identity partitioning on the 'category' column
        PartitionSpec newSpec = PartitionSpec.builderFor(table.schema())
                .identity("name")
                .build();

        // Apply the partition evolution
        for(int i = 0; i < currentSpec.fields().size(); i++) {
            LOGGER.info("Removing field "+currentSpec.fields().get(i).name());
            table.updateSpec().removeField(currentSpec.fields().get(i).name()).commit();
        }
        table.updateSpec()
                .addField("name")         // Add the 'name' column for identity partitioning
                .commit();
    } else {
      LOGGER.info("Table creating");
      PartitionSpec spec = PartitionSpec.builderFor(studentSchema)
             // .identity("country")
             // .bucket("country", 2)
              .day("admission_ts")
              .build();
      table = catalog.createTable(tf, studentSchema, spec);
    }

    //LOGGER.info(table.toString());

    LOGGER.info("Load dummy data to table {}" + table.name());

    // Define the Spark DataFrame schema


    StructType schema = DataTypes.createStructType(new StructField[]{
            DataTypes.createStructField("id", DataTypes.IntegerType, false),
            DataTypes.createStructField("name", DataTypes.StringType, false),
            DataTypes.createStructField("country", DataTypes.StringType, false),
            DataTypes.createStructField("admission_ts", DataTypes.TimestampType, false)
    });
    // Create a list of student data as Row objects
    List<Row> rows = Arrays.asList(
            RowFactory.create(1, "Alice1", "Ireland", Timestamp.valueOf("2025-04-20 10:00:00")),
            RowFactory.create(2, "Bob1" , "Germany", Timestamp.valueOf("2025-04-20 10:00:00")),
            RowFactory.create(3, "Charlie1", "U.K", Timestamp.valueOf("2025-04-21 10:00:00")),
            RowFactory.create(4, "Yang1", "China", Timestamp.valueOf("2025-04-21 10:00:00"))
    );

      System.out.println("Data:");
     rows.forEach(System.out::println);

      System.out.println("Schema:");
      schema.printTreeString();

    // Create a Spark DataFrame from the data
    Dataset<Row> df = session.createDataFrame(rows, schema);

    df.show();
    // Write the DataFrame to the Iceberg table

    LOGGER.info("Table that spark session can see");
    session.sql("SHOW CATALOGS").show();
    //session.sql("SHOW TABLES IN local.school").show();


    df.write()
            .format("iceberg")
            .mode("append") // Use "overwrite" if you want to replace the data
            .save("school."+ STUDENT_TABLE);

  }

  private void addConfigurations(Map<String, String> properties) {
    properties.put(CatalogProperties.CATALOG_IMPL, JdbcCatalog.class.getName());
    properties.put(CatalogProperties.URI, "jdbc:h2:mem:/testdb");
    properties.put(JdbcCatalog.PROPERTY_PREFIX + "user", "sa");
    properties.put(JdbcCatalog.PROPERTY_PREFIX + "password", "password");
    properties.put(CatalogProperties.WAREHOUSE_LOCATION, "src/resources/warehouse");
    properties.put(CatalogProperties.FILE_IO_IMPL, HadoopFileIO.class.getName());
  }

  public void loadEtl() {
    String resourcePath = "userdata1.parquet"; // Path relative to the resources folder
    ClassLoader classLoader = getClass().getClassLoader();
    java.net.URL url = classLoader.getResource(resourcePath);

    parquetReader = new ParquetReader(session, url.getPath());
    parquetReader.get().show();
  }

  public void summarizeTable() {
    Dataset<Row> df = session.sql("select * from school." + STUDENT_TABLE);
    df.show();

  }

  /*    private Catalog setupFileSystemCatalog(String catalogLocation) {
      return CatalogUtil.loadCatalog("catalog",
              "org.apache.iceberg.catalog.FileSystemCatalog",
              catalogLocation);

  }*/
}
