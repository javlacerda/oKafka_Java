package com.omnis.kafka;

import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import net.omnis.OmnisCalls.Response;

public class OKafkaConsumer extends OKafkaConsumerBase<String> {

    @Override
    protected KafkaConsumer<Object, String> createConsumer(Map<String, Object> pParams) {
        String clientId = ParamUtils.getString(pParams, "ClientId", "");
        String groupId = ParamUtils.getString(pParams, "GroupId", "");

        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, clientId);
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 1);

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
