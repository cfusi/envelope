/**
 * Licensed to Cloudera, Inc. under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Cloudera, Inc. licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.cloudera.labs.envelope.derive;

import com.cloudera.labs.envelope.load.ProvidesAlias;
import com.cloudera.labs.envelope.spark.Contexts;
import com.cloudera.labs.envelope.utils.ConfigUtils;
import com.cloudera.labs.envelope.utils.RowUtils;
import com.typesafe.config.Config;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

public class CsvDeriver implements Deriver, ProvidesAlias {
    private static final Logger LOG = LoggerFactory.getLogger(CsvDeriver.class);

    public static final String DATASET_CONFIG = "dataset";
    public static final String CSV_COLUMN = "csv-column";

    // CSV optional parameters
    public static final String CSV_HEADER_CONFIG = "header";
    public static final String CSV_SEPARATOR_CONFIG = "separator";
    public static final String CSV_ENCODING_CONFIG = "encoding";
    public static final String CSV_QUOTE_CONFIG = "quote";
    public static final String CSV_ESCAPE_CONFIG = "escape";
    public static final String CSV_COMMENT_CONFIG = "comment";
    public static final String CSV_INFER_SCHEMA_CONFIG = "infer-schema";
    public static final String CSV_IGNORE_LEADING_CONFIG = "ignore-leading-ws";
    public static final String CSV_IGNORE_TRAILING_CONFIG = "ignore-trailing-ws";
    public static final String CSV_NULL_VALUE_CONFIG = "null-value";
    public static final String CSV_NAN_VALUE_CONFIG = "nan-value";
    public static final String CSV_POS_INF_CONFIG = "positive-infinity";
    public static final String CSV_NEG_INF_CONFIG = "negative-infinity";
    public static final String CSV_DATE_CONFIG = "date-format";
    public static final String CSV_TIMESTAMP_CONFIG = "timestamp-format";
    public static final String CSV_MAX_COLUMNS_CONFIG = "max-columns";
    public static final String CSV_MAX_CHARS_COLUMN_CONFIG = "max-chars-per-column";
    public static final String CSV_MAX_MALFORMED_LOG_CONFIG = "max-malformed-logged";
    public static final String CSV_MODE_CONFIG = "mode";

    // Schema optional parameters
    public static final String FIELD_NAMES_CONFIG = "field.names";
    public static final String FIELD_TYPES_CONFIG = "field.types";

    private Config config;
    private ConfigUtils.OptionMap options;
    private StructType schema;

    private String dataset = "";
    private String csvColumn = "";

    @Override
    public void configure(Config config) {
        this.config = config;

        if (config.hasPath(DATASET_CONFIG)) {
            dataset = config.getString(DATASET_CONFIG);
        }

        if (config.hasPath(CSV_COLUMN)) {
            csvColumn = config.getString(CSV_COLUMN);
        } else {
            throw new RuntimeException("CsvDeriver: the String CSV column dataset must be specified");
        }

        List<String> names = config.getStringList(FIELD_NAMES_CONFIG);
        List<String> types = config.getStringList(FIELD_TYPES_CONFIG);

        this.schema = RowUtils.structTypeFor(names, types);

        options = new ConfigUtils.OptionMap(config)
                .resolve("sep", CSV_SEPARATOR_CONFIG)
                .resolve("encoding", CSV_ENCODING_CONFIG)
                .resolve("quote", CSV_QUOTE_CONFIG)
                .resolve("escape", CSV_ESCAPE_CONFIG)
                .resolve("comment", CSV_COMMENT_CONFIG)
                .resolve("header", CSV_HEADER_CONFIG)
                .resolve("inferSchema", CSV_INFER_SCHEMA_CONFIG)
                .resolve("ignoreLeadingWhiteSpace", CSV_IGNORE_LEADING_CONFIG)
                .resolve("ignoreTrailingWhiteSpace", CSV_IGNORE_TRAILING_CONFIG)
                .resolve("nullValue", CSV_NULL_VALUE_CONFIG)
                .resolve("nanValue", CSV_NAN_VALUE_CONFIG)
                .resolve("positiveInf", CSV_POS_INF_CONFIG)
                .resolve("negativeInf", CSV_NEG_INF_CONFIG)
                .resolve("dateFormat", CSV_DATE_CONFIG)
                .resolve("timestampFormat", CSV_TIMESTAMP_CONFIG)
                .resolve("maxColumns", CSV_MAX_COLUMNS_CONFIG)
                .resolve("maxCharsPerColumn", CSV_MAX_CHARS_COLUMN_CONFIG)
                .resolve("maxMalformedLogPerPartition", CSV_MAX_MALFORMED_LOG_CONFIG)
                .resolve("mode", CSV_MODE_CONFIG);

    }

    @Override
    public Dataset<Row> derive(Map<String, Dataset<Row>> dependencies) throws Exception {
        Dataset<Row> result;
        Dataset<String> input;

        if (dependencies.size() == 1) {
            LOG.debug("derive :dependency = " + dependencies.values().iterator().next());
            input = dependencies.values().iterator().next().select(csvColumn).withColumnRenamed(csvColumn, "value").as(Encoders.STRING());
            LOG.debug("derive : after transformation. Final input dataset schema: " + input.schema().treeString());
        } else if ( !dataset.isEmpty() ) {
            input = dependencies.get(dataset).withColumnRenamed(csvColumn, "value").as(Encoders.STRING());
        } else {
            throw new RuntimeException("If there are more than one dependencies, you need to specify the property " + DATASET_CONFIG);
        }

        if (null != schema) {
            LOG.debug("derive : schema = " + schema.treeString());
            result =  Contexts.getSparkSession().read().schema(schema).options(options).csv(input);
            LOG.debug("derive : result schema = " + result.schema().treeString());
        } else {
            result =  Contexts.getSparkSession().read().options(options).csv(input);
        }

        return result;
    }

    @Override
    public String getAlias() {
        return "csv";
    }
}
