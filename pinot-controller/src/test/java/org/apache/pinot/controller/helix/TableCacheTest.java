/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.controller.helix;

import com.google.common.base.Preconditions;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.pinot.common.utils.helix.TableCache;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.DimensionFieldSpec;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.apache.pinot.util.TestUtils;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.*;


public class TableCacheTest extends ControllerTest {

  @BeforeClass
  public void setUp()
      throws Exception {
    startZk();
    startController();
    addFakeBrokerInstancesToAutoJoinHelixCluster(1, true);
    addFakeServerInstancesToAutoJoinHelixCluster(1, true);
  }

  @Test
  public void testTableCache()
      throws Exception {
    TableCache tableCache = new TableCache(_propertyStore, true);

    assertTrue(tableCache.isCaseInsensitive());
    assertNull(tableCache.getActualTableName("testTable"));
    assertNull(tableCache.getColumnNameMap("testTable"));
    assertNull(tableCache.getTableConfig("testTable_OFFLINE"));
    assertNull(tableCache.getSchema("testTable"));

    // Add a table config
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName("testTable").build();
    _helixResourceManager.addTable(tableConfig);
    // Wait for at most 10 seconds for the callback to add the table config to the cache
    TestUtils.waitForCondition(aVoid -> tableCache.getTableConfig("testTable_OFFLINE") != null, 10_000L,
        "Failed to add the table config to the cache");
    assertEquals(tableCache.getActualTableName("TeStTaBlE"), "testTable");
    assertEquals(tableCache.getActualTableName("TeStTaBlE_oFfLiNe"), "testTable_OFFLINE");
    assertNull(tableCache.getActualTableName("testTable_REALTIME"));
    assertNull(tableCache.getColumnNameMap("testTable"));
    assertEquals(tableCache.getTableConfig("testTable_OFFLINE"), tableConfig);
    assertNull(tableCache.getSchema("testTable"));

    // Update the table config
    tableConfig.getIndexingConfig().setCreateInvertedIndexDuringSegmentGeneration(true);
    _helixResourceManager.updateTableConfig(tableConfig);
    // Wait for at most 10 seconds for the callback to update the table config in the cache
    // NOTE: Table config should never be null during the transitioning
    TestUtils.waitForCondition(
        aVoid -> Preconditions.checkNotNull(tableCache.getTableConfig("testTable_OFFLINE")).equals(tableConfig),
        10_000L, "Failed to update the table config in the cache");
    assertEquals(tableCache.getActualTableName("TeStTaBlE"), "testTable");
    assertEquals(tableCache.getActualTableName("TeStTaBlE_oFfLiNe"), "testTable_OFFLINE");
    assertNull(tableCache.getActualTableName("testTable_REALTIME"));
    assertNull(tableCache.getColumnNameMap("testTable"));
    assertEquals(tableCache.getTableConfig("testTable_OFFLINE"), tableConfig);
    assertNull(tableCache.getSchema("testTable"));

    // Add a schema
    Schema schema =
        new Schema.SchemaBuilder().setSchemaName("testTable").addSingleValueDimension("testColumn", DataType.INT)
            .build();
    _helixResourceManager.addSchema(schema, false);
    // Wait for at most 10 seconds for the callback to add the schema to the cache
    TestUtils.waitForCondition(aVoid -> tableCache.getSchema("testTable") != null, 10_000L,
        "Failed to add the schema to the cache");
    assertEquals(tableCache.getActualTableName("TeStTaBlE"), "testTable");
    assertEquals(tableCache.getActualTableName("TeStTaBlE_oFfLiNe"), "testTable_OFFLINE");
    assertNull(tableCache.getActualTableName("testTable_REALTIME"));
    assertEquals(tableCache.getColumnNameMap("testTable"), Collections.singletonMap("testcolumn", "testColumn"));
    assertEquals(tableCache.getTableConfig("testTable_OFFLINE"), tableConfig);
    assertEquals(tableCache.getSchema("testTable"), schema);

    // Update the schema
    schema.addField(new DimensionFieldSpec("newColumn", DataType.LONG, true));
    _helixResourceManager.updateSchema(schema, false);
    // Wait for at most 10 seconds for the callback to update the schema in the cache
    // NOTE: schema should never be null during the transitioning
    TestUtils.waitForCondition(aVoid -> Preconditions.checkNotNull(tableCache.getSchema("testTable")).equals(schema),
        10_000L, "Failed to update the schema in the cache");
    assertEquals(tableCache.getActualTableName("TeStTaBlE"), "testTable");
    assertEquals(tableCache.getActualTableName("TeStTaBlE_oFfLiNe"), "testTable_OFFLINE");
    assertNull(tableCache.getActualTableName("testTable_REALTIME"));
    Map<String, String> expectedColumnMap = new HashMap<>();
    expectedColumnMap.put("testcolumn", "testColumn");
    expectedColumnMap.put("newcolumn", "newColumn");
    assertEquals(tableCache.getColumnNameMap("testTable"), expectedColumnMap);
    assertEquals(tableCache.getTableConfig("testTable_OFFLINE"), tableConfig);
    assertEquals(tableCache.getSchema("testTable"), schema);

    // Create a new case-sensitive TableCache which should load the existing table config and schema
    TableCache caseSensitiveTableCache = new TableCache(_propertyStore, false);
    assertFalse(caseSensitiveTableCache.isCaseInsensitive());
    // Getting actual table name or column name map should throw exception for case-sensitive TableCache
    try {
      caseSensitiveTableCache.getActualTableName("testTable");
      fail();
    } catch (Exception e) {
      // Expected
    }
    try {
      caseSensitiveTableCache.getColumnNameMap("testTable");
      fail();
    } catch (Exception e) {
      // Expected
    }
    assertEquals(tableCache.getTableConfig("testTable_OFFLINE"), tableConfig);
    assertEquals(tableCache.getSchema("testTable"), schema);

    // Remove the table config
    _helixResourceManager.deleteOfflineTable("testTable");
    // Wait for at most 10 seconds for the callback to remove the table config from the cache
    TestUtils.waitForCondition(aVoid -> tableCache.getTableConfig("testTable_OFFLINE") == null, 10_000L,
        "Failed to remove the table config from the cache");
    // Case-insensitive table name are handled based on the table config instead of the schema
    assertNull(tableCache.getActualTableName("testTable"));
    assertEquals(tableCache.getColumnNameMap("testTable"), expectedColumnMap);
    assertNull(tableCache.getTableConfig("testTable_OFFLINE"));
    assertEquals(tableCache.getSchema("testTable"), schema);

    // Remove the schema
    _helixResourceManager.deleteSchema(schema);
    // Wait for at most 10 seconds for the callback to remove the schema from the cache
    TestUtils.waitForCondition(aVoid -> tableCache.getSchema("testTable") == null, 10_000L,
        "Failed to remove the schema from the cache");
    assertNull(tableCache.getActualTableName("testTable"));
    assertNull(tableCache.getColumnNameMap("testTable"));
    assertNull(tableCache.getTableConfig("testTable_OFFLINE"));
    assertNull(tableCache.getSchema("testTable"));
  }

  @AfterClass
  public void tearDown() {
    stopController();
    stopZk();
  }
}
