package com.omnis.kafka;

import java.util.Map;
import java.util.Properties;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import net.omnis.OmnisCalls.Response;

public class OKafkaConsumerSchema extends OKafkaConsumerBase<GenericRecord> {

    @Override
    protected KafkaConsumer<Object, GenericRecord> createConsumer(Map<String, Object> pParams) {
        String clientId = ParamUtils.getString(pParams, "ClientId", "");
        String groupId = ParamUtils.getString(pParams, "GroupId", "");
        String schemaRegistryUrl = ParamUtils.getString(pParams, "SchemaRegistryUrl", "");

        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, clientId);
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);

        if (pParams.containsKey("KeySchemaId")) {
            properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName());
        } else {
            properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        }
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName());
        properties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 1);

        properties.setProperty(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
        properties.setProperty(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, "false");
        properties.putAll(KafkaUtils.getSchemaRegistryAuthConfigs(pParams));

        KafkaUtils.addKafkaConfigs(pParams, properties);
        return new KafkaConsumer<>(properties);
    }

    @Override
    public Response connect(Map<String, Object> pParams) {
        return super.connect(pParams);
    }

    @Override
    public Response consume(Map<String, Object> pParams) {
        return super.consume(pParams);
    }

    @Override
    public Response commit(Map<String, Object> pParams) {
        return super.commit(pParams);
    }

    @Override
    public Response close(Map<String, Object> pParams) {
        return super.close(pParams);
    }
}
