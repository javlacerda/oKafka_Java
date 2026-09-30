package com.omnis.kafka;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class App {
    public static final String TOPIC = "omnis_simple";
    public static final String TOPIC_SCHEMA = "omnis_schema";
    public static final String GROUP_ID = "omnis-consumer-group";
    public static final String CLIENT_ID = "omnis-consumer";
    //    public static final String SERVER = "datakfk.dev.prozis.tech:9095";
    public static final String SERVER = "localhost:9092";
    //    public static final String SCHEMA_REGISTRY_SERVER = "http://datakfk.dev.prozis.tech:8081";
    public static final String SCHEMA_REGISTRY_SERVER = "http://localhost:8081";
    public static final String SCHEMA_STR =
            """
            {
              "type": "record",
              "name": "user",
              "fields": [
                {
                  "name": "Id",
                  "type": "int"
                },
                {
                  "name": "Name",
                  "type": "string"
                },
                {
                  "name": "Email",
                  "type": "string"
                },
                {
                  "name": "Age",
                  "type": "int"
                },
                {
                  "name": "Active",
                  "type": "boolean"
                },
                {
                  "name": "DateCreated",
                  "type": "string"
                }
              ]
            }
            """;

    public static void main(String[] args) {
        App app = new App();

        app.produce();
        app.consume();
        app.produceSchema();
        app.consumeSchema();
    }

    private void produce() {
        Map<String, Object> params = buildParamsProducer();
        params.put("Topic", TOPIC);
        params.put("Key", UUID.randomUUID().toString());
        params.put("Message", "Simple text message from Omnis via Java: " + LocalDateTime.now());
        
        OKafkaProducer kafkaProducer = new OKafkaProducer();
        try {
            kafkaProducer.connect(params);
            var ret = kafkaProducer.produce(params);
            System.out.println("###: " + ret.getData());
        }
        catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    private void consume() {
        Map<String, Object> params = buildParamsConsumer();
        params.put("Topic", TOPIC);
        params.put("ClientId", CLIENT_ID);
        params.put("GroupId", GROUP_ID);

        OKafkaConsumer kafkaConsumer = new OKafkaConsumer();
        try {
            kafkaConsumer.connect(params);

            Map<String, Object> consumeParams = new HashMap<>();
            consumeParams.put("Timeout", 10);
            var ret = kafkaConsumer.consume(consumeParams);
            System.out.println("###: " + ret.getData());

            kafkaConsumer.close(params);
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    private void produceSchema() {
        Map<String, Object> params = buildParamsProducer();
        params.put("Topic", TOPIC_SCHEMA);
        params.put("SchemaRegistryUrl", SCHEMA_REGISTRY_SERVER);
        params.put("Schema", SCHEMA_STR);
        params.put("Key", UUID.randomUUID().toString());

        Map<String, Object> message = new HashMap<>();
        message.put("Id", 12345);
        message.put("Name", "Record 12345");
        message.put("Email", "12345@record.com");
        message.put("Age", 52);
        message.put("Active", true);
        message.put("DateCreated", LocalDate.now().toString());
        params.put("Message", message);

        OKafkaProducerSchema kafkaProducer = new OKafkaProducerSchema();
        try {
            kafkaProducer.connect(params);
            var ret = kafkaProducer.produce(params);
            System.out.println("###: " + ret.getData());
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    private void consumeSchema() {
        Map<String, Object> params = buildParamsConsumer();
        params.put("Topic", TOPIC_SCHEMA);
        params.put("SchemaRegistryUrl", SCHEMA_REGISTRY_SERVER);
        params.put("ClientId", CLIENT_ID);
        params.put("GroupId", GROUP_ID);

        OKafkaConsumerSchema kafkaConsumer = new OKafkaConsumerSchema();
        try {
            kafkaConsumer.connect(params);

            Map<String, Object> consumeParams = new HashMap<>();
            consumeParams.put("Timeout", 10);
            var ret = kafkaConsumer.consume(consumeParams);
            System.out.println("###: " + ret.getData());

            kafkaConsumer.close(params);
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    private Map<String, Object> buildParamsProducer() {
        Map<String, Object> params = new HashMap<>();

        params.put("Server", SERVER);
        params.put("Partition", Double.valueOf(0));

        List<List<String>> config = new ArrayList<>();
        config.add(Arrays.asList("linger.ms", "0"));
        //        config.add(Arrays.asList("security.protocol", "SASL_PLAINTEXT"));
        //        config.add(Arrays.asList("sasl.mechanism", "SCRAM-SHA-256"));
        //        config.add(Arrays.asList("sasl.username", "erpcore-user"));
        //        config.add(Arrays.asList("sasl.password", ""));
        params.put("Config", config);

        return params;
    }

    private Map<String, Object> buildParamsConsumer() {
        Map<String, Object> params = new HashMap<>();

        params.put("Server", SERVER);
        params.put("Partition", Double.valueOf(0));

        List<List<String>> config = new ArrayList<>();
        config.add(Arrays.asList("enable.auto.commit", "false"));
        config.add(Arrays.asList("auto.offset.reset", "earliest"));
        //        config.add(Arrays.asList("security.protocol", "SASL_PLAINTEXT"));
        //        config.add(Arrays.asList("sasl.mechanism", "SCRAM-SHA-256"));
        //        config.add(Arrays.asList("sasl.username", "erpcore-user"));
        //        config.add(Arrays.asList("sasl.password", ""));
        params.put("Config", config);

        return params;
    }
}
