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
package com.cloudera.labs.envelope.input.translate;

import java.util.List;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructType;
import org.kitesdk.morphline.api.MorphlineCompilationException;
import org.kitesdk.morphline.api.Record;
import org.kitesdk.morphline.base.Fields;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.labs.envelope.load.ProvidesAlias;
import com.cloudera.labs.envelope.utils.MorphlineUtils;
import com.cloudera.labs.envelope.utils.RowUtils;
import com.cloudera.labs.envelope.utils.TranslatorUtils;
import com.google.common.collect.Lists;
import com.typesafe.config.Config;

/**
 * Morphline
 */
public class MorphlineTranslator<K, V> implements Translator<K, V>, ProvidesAlias {

  // TODO : Move default parameter values to reference.conf? How? Each Translator is separate/nested?
  // This most likely should use Config.withFallback() on a per instance basis.
  public static final String ENCODING_KEY = "encoding.key";
  public static final String ENCODING_MSG = "encoding.message";
  public static final String MORPHLINE = "morphline.file";
  public static final String MORPHLINE_ID = "morphline.id";
  public static final String PRODUCTION_MODE = "production.mode";
  public static final String FIELD_NAMES = "field.names";
  public static final String FIELD_TYPES = "field.types";

  private static final Logger LOG = LoggerFactory.getLogger(MorphlineTranslator.class);
  private static final String TRANSLATOR_KEY = "_attachment_key";
  private static final String TRANSLATOR_KEY_CHARSET = "_attachment_key_charset";

  private String keyEncoding;
  private String messageEncoding;
  private String morphlineFile;
  private String morphlineId;
  private StructType schema;
  private MorphlineUtils.Pipeline pipeline;
  private boolean doesAppendRaw;

  @Override
  public void configure(Config config) {
    LOG.trace("Configuring Morphline Translator");

    // Define the encoding values, if necessary
    this.keyEncoding = config.getString(ENCODING_KEY);
    this.messageEncoding = config.getString(ENCODING_MSG);

    // Set up the Morphline configuration, the file must be located on the local file system
    this.morphlineFile = config.getString(MORPHLINE);
    this.morphlineId = config.getString(MORPHLINE_ID);

    if (this.morphlineFile == null || this.morphlineFile.trim().length() == 0) {
      throw new MorphlineCompilationException("Missing or empty Morphline File configuration parameter", null);
    }

    // Construct the StructType schema for the Rows
    List<String> fieldNames = config.getStringList(FIELD_NAMES);
    List<String> fieldTypes = config.getStringList(FIELD_TYPES);
    this.doesAppendRaw = TranslatorUtils.doesAppendRaw(config);
    if (this.doesAppendRaw) {
      fieldNames.add(TranslatorUtils.getAppendRawKeyFieldName(config));
      fieldTypes.add("binary");
      fieldNames.add(TranslatorUtils.getAppendRawValueFieldName(config));
      fieldTypes.add("binary");
    }
    this.schema = RowUtils.structTypeFor(fieldNames, fieldTypes);
  }

  @Override
  public StructType getSchema() {
    return this.schema;
  }

  @Override
  public Iterable<Row> translate(K key, V value) throws Exception {
    LOG.debug("Translating {}[{}]", key, value);

    // Get the Morphline Command pipeline
    if (null == this.pipeline) {
      this.pipeline = MorphlineUtils.getPipeline(this.morphlineFile, this.morphlineId);

      // If null, then instantiate the pipeline
      if (null == this.pipeline) {
        this.pipeline = MorphlineUtils.setPipeline(this.morphlineFile, this.morphlineId, new MorphlineUtils.Collector(),true);
      }
    }

    // Construct the input Record
    Record inputRecord = new Record();

    // Set up the message as _attachment_body (standard Morphline convention)
    if (value instanceof String) {
      inputRecord.put(Fields.ATTACHMENT_BODY, ((String) value).getBytes(this.messageEncoding));
    } else {
      inputRecord.put(Fields.ATTACHMENT_BODY, value);
    }
    inputRecord.put(Fields.ATTACHMENT_CHARSET, this.messageEncoding);

    // Add the key as a custom Record field
    if (null != key) {
      inputRecord.put(TRANSLATOR_KEY, key);
      inputRecord.put(TRANSLATOR_KEY_CHARSET, this.keyEncoding);
    }

    // TODO : Consider using the MorphlineContext exception handler
    // Execute the pipeline (runtime errors are not caught)
    List<Record> outputRecords = MorphlineUtils.executePipeline(this.pipeline, inputRecord);

    // Convert output to Rows
    List<Row> outputRows = Lists.newArrayListWithCapacity(outputRecords.size());
    for (Record output: outputRecords) {
      Row outputRow = MorphlineUtils.convertToRow(this.schema, output);
      
      if (this.doesAppendRaw) {
        outputRow = RowUtils.append(outputRow, key);
        if (value instanceof String) {
          outputRow = RowUtils.append(outputRow, ((String) value).getBytes(this.messageEncoding));
        }
        else {
          outputRow = RowUtils.append(outputRow, value);
        }
      }
      
      outputRows.add(outputRow);
    }

    return outputRows;
  }

  @Override
  public String getAlias() {
    return "morphline";
  }
}
