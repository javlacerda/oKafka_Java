package com.omnis.kafka;

import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

import net.omnis.OmnisCalls.Response;

public class OKafkaProducer extends OKafkaProducerBase<String> {

    @Override
    protected KafkaProducer<Object, String> createProducer(Map<String, Object> pParams) {
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        applyTransactionalId(properties);

        KafkaUtils.addKafkaConfigs(pParams, properties);
        return new KafkaProducer<>(properties);
    }

    @Override
    protected Object buildKey(Map<String, Object> pParams) {
        return ParamUtils.getString(pParams, "Key", null);
    }

    @Override
    protected String buildValue(Map<String, Object> pParams) throws Exception {
        String message = ParamUtils.getString(pParams, "Message", null);
        if (message == null)
            throw new Exception("No message provided");

        return message;
    }

    @Override
    public Response connect(Map<String, Object> pParams) {
        return super.connect(pParams);
    }

    @Override
    public Response produce(Map<String, Object> pParams) {
        return super.produce(pParams);
    }

    @Override
    public Response produceFromList(Map<String, Object> pParams) {
        return super.produceFromList(pParams);
    }

    @Override
    public Response close(Map<String, Object> pParams) {
        return super.close(pParams);
    }

    @Override
    public Response setTransactionalId(Map<String, Object> pParams) {
        return super.setTransactionalId(pParams);
    }

    @Override
    public Response beginTransaction(Map<String, Object> pParams) {
        return super.beginTransaction(pParams);
    }

    @Override
    public Response commitTransaction(Map<String, Object> pParams) {
        return super.commitTransaction(pParams);
    }

    @Override
    public Response abortTransaction(Map<String, Object> pParams) {
        return super.abortTransaction(pParams);
    }
}
