package com.omnis.kafka;

import java.time.Instant;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TimeZone;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClientConfig;

public class KafkaUtils {
    
    private KafkaUtils() {
    }
    
    public static void addKafkaConfigs(Map<String, Object> pParams, Properties properties) {
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, ParamUtils.getString(pParams, "Server", ""));        
        
        List<List<String>> config = ParamUtils.getListOfListOfString(pParams, "Config", null);
        if (config != null) {
            String securityProtocol = null;
            String saslMechanism = null;
            String saslUserName = null;
            String saslPassword = null;
    
            for (List<String> configItem : config) {
                if (configItem.size() != 2) {
                    throw new IllegalArgumentException("Key 'Config' entries must have exactly 2 elements [property, value], but found " + configItem.size());
                }

                String item = configItem.get(0);
                String value = configItem.get(1);

                if ("security.protocol".equals(item)) {
                    securityProtocol = value;
                } else if ("sasl.mechanism".equals(item)) {
                    saslMechanism = value;
                } else if ("sasl.username".equals(item)) {
                    saslUserName = value;
                } else if ("sasl.password".equals(item)) {
                    saslPassword = value;
                } else {
                    properties.setProperty(item, value);
                }
            }
    
            if ("SASL_PLAINTEXT".equals(securityProtocol)) {
                if (saslMechanism != null && saslUserName != null && saslPassword != null) {                
                    properties.setProperty("security.protocol", securityProtocol);
                    properties.setProperty("sasl.mechanism", saslMechanism);
    
                    String jaasConfig = 
                            String.format(
                                    "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"%s\" password=\"%s\";", 
                                    saslUserName, 
                                    saslPassword);
    
                    properties.setProperty("sasl.jaas.config", jaasConfig);
                } else {
                    throw new IllegalArgumentException("SASL configuration is incomplete.");                    
                }
            }
        }
    }

    // HTTP Basic-Auth credentials for the Schema Registry, mirroring MCWKafka's
    // SchemaRegistryClient::setCredentials(). Usable both as a standalone
    // CachedSchemaRegistryClient's configs map and merged into a Properties bag
    // (KafkaAvroSerializer/Deserializer read the same keys from there).
    public static Map<String, Object> getSchemaRegistryAuthConfigs(Map<String, Object> pParams) {
        Map<String, Object> configs = new java.util.HashMap<>();

        String user = ParamUtils.getString(pParams, "SchemaRegistryUser", "");
        if (!user.isEmpty()) {
            String password = ParamUtils.getString(pParams, "SchemaRegistryPassword", "");
            configs.put(SchemaRegistryClientConfig.BASIC_AUTH_CREDENTIALS_SOURCE, "USER_INFO");
            configs.put(SchemaRegistryClientConfig.USER_INFO_CONFIG, user + ":" + password);
        }

        return configs;
    }

    public static ArrayList<Header> getHeadersFromParams(Map<String, Object> pParams) {
        ArrayList<Header> headers = null;

        Object headersObj = pParams.get("Headers");
        if (headersObj instanceof Map<?, ?> headersMap) {
            headers = new ArrayList<>();

            for (Map.Entry<?, ?> header : headersMap.entrySet()) {
                String headerKey = (String) header.getKey();
                byte[] headerValue = ((String) header.getValue()).getBytes();
                headers.add(new RecordHeader(headerKey, headerValue));
            }
        }

        return headers;
    }
    
    public static void setProduceResult(Map<String, Object> data, RecordMetadata metadata) {
        data.put(Constants.SUCCESS, true);

        data.put("Topic", metadata.topic());
        data.put("Partition", metadata.partition());
        if (metadata.hasOffset())
            data.put("Offset", metadata.offset());
        if (metadata.hasTimestamp())
            data.put("Timestamp", LocalDateTime.ofInstant(Instant.ofEpochMilli(metadata.timestamp()), TimeZone.getDefault().toZoneId()).toString());
    }
    
    public static Map<String, Object> genericRecordToMap(GenericRecord record) {
        Map<String, Object> map = new java.util.HashMap<>();
        for (Schema.Field field : record.getSchema().getFields()) {
            map.put(field.name(), toResponseValue(record.get(field.name())));
        }
        return map;
    }

    // Converts a raw Kafka key/value (String, GenericRecord, or a nested Avro field)
    // into something SendResponse can serialize — GenericRecord becomes a Map.
    public static Object toResponseValue(Object value) {
        if (value instanceof Utf8) {
            return value.toString();
        }
        if (value instanceof GenericRecord genericRecord) {
            return genericRecordToMap(genericRecord);
        }
        if (value instanceof java.util.Collection<?> collection) {
            ArrayList<Object> list = new ArrayList<>();
            for (Object item : collection) {
                list.add(toResponseValue(item));
            }
            return list;
        }
        if (value instanceof java.util.Map<?, ?> map) {
            Map<String, Object> result = new java.util.HashMap<>();
            for (Map.Entry<?, ?> entry : map.entrySet()) {
                result.put(String.valueOf(entry.getKey()), toResponseValue(entry.getValue()));
            }
            return result;
        }
        return value;
    }

    public static String getAllExceptionMessages(Throwable throwable) {
        if (throwable == null) {
            return "";
        }
        
        StringBuilder messageBuilder = new StringBuilder();
        Throwable current = throwable;
        
        while (current != null) {
            if (messageBuilder.length() > 0) {
                messageBuilder.append("; ");
            }
            
            // Use class name if the detail message is null
            String msg = current.getMessage();
            if (msg == null || msg.isEmpty()) {
                msg = current.getClass().getName();
            }
            
            messageBuilder.append(msg);
            current = current.getCause(); // Move to the next exception in the tree
        }
        
        return messageBuilder.toString();
    }    
}
