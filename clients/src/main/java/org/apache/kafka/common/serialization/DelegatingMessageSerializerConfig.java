/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to You under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package org.apache.kafka.common.serialization;

import java.util.Map;
import java.util.Set;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;

/**
 * The consumer configuration keys
 */
public class DelegatingMessageSerializerConfig extends AbstractConfig {
    private static final ConfigDef CONFIG;

    /*
     * NOTE: DO NOT CHANGE EITHER CONFIG STRINGS OR THEIR JAVA VARIABLE NAMES AS
     * THESE ARE PART OF THE PUBLIC API AND CHANGE WILL BREAK USER CODE.
     */

    /** <code>headers.deserializer</code> */
    public static final String HEADERS_SERIALIZER_CLASS_CONFIG = "headers.serializer";
    public static final String HEADERS_SERIALIZER_CLASS_DOC = "Serializer class for headers that implements the <code>Derializer</code> interface.";

    /** <code>value.deserializer</code> */
    public static final String VALUE_SERIALIZER_CLASS_CONFIG = "value.serializer";
    public static final String VALUE_SERIALIZER_CLASS_DOC = "Serializer class for value that implements the <code>Serializer</code> interface.";

    static {
        CONFIG = new ConfigDef()
                                .define(HEADERS_SERIALIZER_CLASS_CONFIG,
                                        Type.CLASS,
                                        Importance.HIGH,
                                        HEADERS_SERIALIZER_CLASS_DOC)
                                .define(VALUE_SERIALIZER_CLASS_CONFIG,
                                        Type.CLASS,
                                        Importance.HIGH,
                                        VALUE_SERIALIZER_CLASS_DOC);
    }

    DelegatingMessageSerializerConfig(Map<?, ?> props) {
        super(CONFIG, props);
    }

    public static Set<String> configNames() {
        return CONFIG.names();
    }

    public static void main(String[] args) {
        System.out.println(CONFIG.toHtmlTable());
    }

}
