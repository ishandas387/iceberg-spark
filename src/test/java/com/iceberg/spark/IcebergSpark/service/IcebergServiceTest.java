package com.iceberg.spark.IcebergSpark.service;

import org.junit.jupiter.api.Test;

class IcebergServiceTest {

    IcebergService service = new IcebergService();

    @Test
    void testIcebergCatalog() {
        service.testIcebergCatalog();
    }
}