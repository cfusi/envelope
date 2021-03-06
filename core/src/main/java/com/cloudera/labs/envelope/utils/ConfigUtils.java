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
package com.cloudera.labs.envelope.utils;

import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValue;
import com.typesafe.config.ConfigValueFactory;
import com.typesafe.config.ConfigValueType;

public class ConfigUtils {

  public static Config configFromPath(String path) {
    File configFile = new File(path);
    return ConfigFactory.parseFile(configFile);
  }

  public static Config configFromResource(String resource) {
    try (Reader reader = new InputStreamReader(ConfigUtils.class.getResourceAsStream(resource))) {
      return ConfigFactory.parseReader(reader);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public static Config applySubstitutions(Config config) {
    return ConfigFactory.defaultOverrides()
        .withFallback(config)
        .resolve();
  }

  public static Config applySubstitutions(Config config, String substitutionsString) {
    String[] substitutions = substitutionsString.split(Pattern.quote(","));

    for (String substitution : substitutions) {
      Config substitutionConfig = ConfigFactory.parseString(substitution);
      config = config.withFallback(substitutionConfig);
    }

    return applySubstitutions(config);
  }

  public static void assertConfig(Config config, String key) {
    if (!config.hasPath(key)) {
      throw new RuntimeException("Missing required property [" + key + "]");
    }
  }

  @SuppressWarnings("serial")
  public static class OptionMap extends HashMap<String, String> {
    private Config config;

    public OptionMap(Config config) {
      this.config = config;
    }

    public OptionMap resolve(String option, String parameter) {
      if (config.hasPath(parameter)) {
        this.put(option, config.getString(parameter));
      }
      return this;
    }
  }
  
  public static Config findReplaceStringValues(Config config, String findRegex, Object replace) {
    for (Map.Entry<String, ConfigValue> valueEntry : config.entrySet()) {
      ConfigValueType valueType = valueEntry.getValue().valueType();
      if (valueType.equals(ConfigValueType.OBJECT)) {
        config = ConfigUtils.findReplaceStringValues(config.getConfig(valueEntry.getKey()), findRegex, replace);
      }
      else if (valueType.equals(ConfigValueType.LIST)) {
        @SuppressWarnings("unchecked")
        List<Object> valueList = (List<Object>)valueEntry.getValue().unwrapped();
        if (valueList.size() > 0) {
          if (valueList.get(0) instanceof String) {
            for (int i = 0; i < valueList.size(); i++) {
              String found = (String)valueList.get(0);
              String replaced = found.replaceAll(findRegex, replace.toString());
              valueList.set(i, replaced);
            }
          }
        }
        config = config.withValue(valueEntry.getKey(), ConfigValueFactory.fromAnyRef(valueList));
      }
      else if (valueType.equals(ConfigValueType.STRING)) {
        String found = (String)valueEntry.getValue().unwrapped();
        String replaced = found.replaceAll(findRegex, replace.toString());
        config = config.withValue(valueEntry.getKey(), ConfigValueFactory.fromAnyRef(replaced));
      }
    }
    
    return config;
  }

}
