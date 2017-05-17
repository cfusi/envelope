/**
 * Copyright © 2016-2017 Cloudera, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.cloudera.labs.envelope.utils.hbase;

import com.cloudera.labs.envelope.utils.JVMUtils;
import com.cloudera.labs.envelope.utils.RowUtils;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigObject;
import com.typesafe.config.ConfigValue;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class HBaseUtils {

  private static final Logger LOG = LoggerFactory.getLogger(HBaseUtils.class);

  public static final String ZK_QUORUM_PROPERTY = "zookeeper";
  public static final String TABLE_NAME_PROPERTY = "table.name";
  public static final String ROWKEY_PROPERTY = "mapping.rowkey";
  public static final String COLUMNS_PROPERTY = "mapping.columns";
  public static final String SERDE_PROPERTY = "mapping.serde";
  public static final String HBASE_PASSTHRU_PREFIX = "hbase.conf";
  public static final String HBASE_BATCH_SIZE = "batch.size";
  public static final String KEY_SEPARATOR = "mapping.rowkey.separator";

  public static final int DEFAULT_HBASE_BATCH_SIZE = 1000;
  public static final String DEFAULT_SERDE_PROPERTY = "default";
  public static final String DEFAULT_KEY_SEPARATOR = ":";

  private static final List<String> REQUIRED_CONFIGS = Lists.newArrayList(
      TABLE_NAME_PROPERTY,
      ROWKEY_PROPERTY,
      COLUMNS_PROPERTY
  );

  public static boolean validateConfig(Config config) {
    boolean isValid = true;

    // Does config have all required sections?
    // TODO localize required properties to individual serdes
    for (String property : REQUIRED_CONFIGS) {
      if (!config.hasPath(property)) {
        LOG.error("'{}' not specified in config", property);
        isValid = false;
      }
    }

    // Specific checks for columns
    if (config.hasPath(COLUMNS_PROPERTY)) {
      ConfigObject columnConfig = config.getConfig(COLUMNS_PROPERTY).root();
      Set<String> columns = columnConfig.keySet();
      for (String column : columns) {
        if (!config.hasPath(COLUMNS_PROPERTY + "." + column + ".col")) {
          LOG.error("'col' not specified in column {}", column);
          isValid = false;
        }
        if (!config.hasPath(COLUMNS_PROPERTY + "." + column + ".type")) {
          LOG.error("'type' not specified in column {}", column);
          isValid = false;
        }
        if (!config.hasPath(COLUMNS_PROPERTY + "." + column + ".cf")) {
          LOG.error("'cf' not specified in column {}", column);
          isValid = false;
        }
      }
    }

    return isValid;
  }

  public synchronized static Connection getConnection(Config config) throws IOException {
    LOG.info("Opening connection to HBase");
    LOG.debug("Creating connection object...");
    Configuration configuration = HBaseUtils.getHBaseConfiguration(config);

    // new Connection
    Connection connection = ConnectionFactory.createConnection(configuration);

    if (connection == null) {
      LOG.error("Could not open connection to HBase with {}", configuration.get(HBaseUtils.ZK_QUORUM_PROPERTY));
      throw new IllegalArgumentException("Could not connect to HBase with supplied ZK quorum");
    }

    JVMUtils.closeAtShutdown(connection);
    return connection;
  }

  public static Configuration getHBaseConfiguration(Config config) throws IOException {
    Configuration hbaseConfiguration = HBaseConfiguration.create();
    if (config.hasPath(ZK_QUORUM_PROPERTY)) {
      String zkQuorum = config.getString(ZK_QUORUM_PROPERTY);
      hbaseConfiguration.set(HConstants.ZOOKEEPER_QUORUM, zkQuorum);
    }
    LOG.debug("HBase:: Using ZK quorum: {}", hbaseConfiguration.get(HConstants.ZOOKEEPER_QUORUM));
    LOG.debug("HBase:: Using security: {}", hbaseConfiguration.get("hadoop.security.authentication"));

    // Add any other pass-through options starting with HBASE_PASSTHRU_PREFIX
    if (config.hasPath(HBASE_PASSTHRU_PREFIX)) {
      Config hbaseConfigs = config.getConfig(HBASE_PASSTHRU_PREFIX);
      for (Map.Entry<String, ConfigValue> entry : hbaseConfigs.entrySet()) {
        String param = entry.getKey();
        String value = null;
        switch (entry.getValue().valueType()) {
          case STRING:
            value = (String) entry.getValue().unwrapped();
            break;
          default:
            LOG.warn("Only string parameters currently " +
                "supported, auto-converting to String [{}]", param);
            value = entry.getValue().unwrapped().toString();
        }
        if (value != null) {
          hbaseConfiguration.set(param, value);
        }
      }
    }
    return hbaseConfiguration;
  }

  public static int batchSizeFor(Config config) {
    if (config.hasPath(HBASE_BATCH_SIZE)) {
      return config.getInt(HBASE_BATCH_SIZE);
    } else {
      return DEFAULT_HBASE_BATCH_SIZE;
    }
  }

  public static TableName tableInfoFor(Config config) {
    String name = config.getString(TABLE_NAME_PROPERTY);

    return TableName.valueOf(name);
  }

  // HBaseSerde util
  public static HBaseSerde getSerde(Config config) {
    HBaseSerde serde;
    if (config.hasPath(SERDE_PROPERTY)) {
      String serdeImpl = config.getString(SERDE_PROPERTY);
      if (serdeImpl.equals(DEFAULT_SERDE_PROPERTY)) {
        return new DefaultHBaseSerde();
      } else {
        try {
          Class<?> clazz = Class.forName(serdeImpl);
          Constructor<?> constructor = clazz.getConstructor();
          return (HBaseSerde) constructor.newInstance();
        } catch (Exception e) {
          LOG.error("Could not construct custom HBaseSerde instance  [" + serdeImpl + "]: " + e);
          throw new RuntimeException(e);
        }
      }
    } else {
      serde =  new DefaultHBaseSerde();
    }
    serde.configure(config);

    return serde;
  }

  // HBaseSerde util
  public static byte[] rowKeySeparatorFor(Config config) {
    byte[] separator = DEFAULT_KEY_SEPARATOR.getBytes();
    if (config.hasPath(KEY_SEPARATOR)) {
      separator = config.getString(KEY_SEPARATOR).getBytes();
    }

    return separator;
  }

  // HBaseSerde util
  public static List<String> rowKeyFor(Config config) {
    return config.getStringList(ROWKEY_PROPERTY);
  }

  // HBaseSerde util
  public static Map<String, HBaseSerde.ColumnDef> columnsFor(Config config) {
    Map<String, HBaseSerde.ColumnDef> columnDefs = Maps.newHashMap();
    Config columns = config.getConfig(COLUMNS_PROPERTY);

    for (ConfigValue value : columns.root().values()) {
      Config column = value.atPath("c");
      columnDefs.put(column.getString("c.col"), new HBaseSerde.ColumnDef(column.getString("c.cf"),
          column.getString("c.col"), column.getString("c.type")));
    }

    return columnDefs;
  }

  // HBaseSerde util
  public static StructType buildSchema(Map<String, HBaseSerde.ColumnDef> columnDefinitions) {
    List<String> fieldNames = Lists.newArrayList();
    List<String> fieldTypes = Lists.newArrayList();
    for (Map.Entry<String, HBaseSerde.ColumnDef> columnDef : columnDefinitions.entrySet()) {
      fieldNames.add(columnDef.getValue().name);
      fieldTypes.add(columnDef.getValue().type);
    }

    return RowUtils.structTypeFor(fieldNames, fieldTypes);
  }

}
