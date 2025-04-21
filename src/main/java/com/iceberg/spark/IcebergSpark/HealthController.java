package com.iceberg.spark.IcebergSpark;

import com.iceberg.spark.IcebergSpark.service.IcebergService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/iceberg")
public class HealthController {

  @Autowired IcebergService service;

  @GetMapping("/alive")
  public String health() {
    return "alive";
  }

  @GetMapping("/catalog")
  public String createCatalog() {
    service.testIcebergCatalog();
    return "OK";
  }

  @GetMapping("/etl")
  public String etlLoad() {
    service.loadEtl();
    return "OK";
  }

  @GetMapping("/summary")
  public String summarize() {
    service.summarizeTable();
    return "OK";
  }

  @GetMapping("/df")
  public String createdf() {
    service.summarizeTable();
    return "OK";
  }
}
