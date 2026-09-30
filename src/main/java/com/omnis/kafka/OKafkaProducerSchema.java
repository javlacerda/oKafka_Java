package com.omnis.kafka;

import java.util.Map;
import java.util.Properties;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import net.omnis.OmnisCalls.Response;

public class OKafkaProducerSchema extends OKafkaProducerBase<GenericRecord> {

    private String m_schemaStr = "";
    private int m_schemaId = 0;
    private String m_keySchemaStr = "";
    private int m_keySchemaId = 0;
    private String m_schemaRegistryUrl = "";
    private CachedSchemaRegistryClient m_registryClient = null;

    @Override
    protected KafkaProducer<Object, GenericRecord> createProducer(Map<String, Object> pParams) {
        Properties properties = new Properties();

        m_schemaId = ParamUtils.getInt(pParams, "SchemaId", 0);
        m_schemaStr = ParamUtils.getString(pParams, "Schema", "");
        m_keySchemaId = ParamUtils.getInt(pParams, "KeySchemaId", 0);
        m_keySchemaStr = ParamUtils.getString(pParams, "KeySchema", "");
        m_schemaRegistryUrl = ParamUtils.getString(pParams, "SchemaRegistryUrl", "");
        m_registryClient = new CachedSchemaRegistryClient(m_schemaRegistryUrl, 100, KafkaUtils.getSchemaRegistryAuthConfigs(pParams));

        if (m_keySchemaId != 0 || !m_keySchemaStr.isEmpty()) {
            properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
        } else {
            properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        }

        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
        properties.setProperty(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, m_schemaRegistryUrl);
        properties.putAll(KafkaUtils.getSchemaRegistryAuthConfigs(pParams));
        applyTransactionalId(properties);

        KafkaUtils.addKafkaConfigs(pParams, properties);
        return new KafkaProducer<>(properties);
    }

    @Override
    protected Object buildKey(Map<String, Object> pParams) throws Exception {
        if (m_keySchemaId == 0 && m_keySchemaStr.isEmpty())
            return ParamUtils.getString(pParams, "Key", null);

        Object key = pParams.get("Key");
        if (key == null)
            throw new Exception("No key provided");

        // Key schemas are often a plain primitive type (e.g. "string", "int"),
        // unlike value schemas which are always a record — only wrap into a
        // GenericRecord when the schema actually is one.
        Schema avroSchema = resolveAvroSchema(m_keySchemaId, m_keySchemaStr);
        if (avroSchema.getType() != Schema.Type.RECORD)
            return key;

        if (!(key instanceof Map<?, ?> keyMap))
            throw new Exception("KeySchema is a record type, so 'Key' must be a Map");

        GenericRecord genericRecord = new GenericData.Record(avroSchema);
        for (Map.Entry<?, ?> entry : keyMap.entrySet()) {
            genericRecord.put(String.valueOf(entry.getKey()), entry.getValue());
        }

        return genericRecord;
    }

    @Override
    protected GenericRecord buildValue(Map<String, Object> pParams) throws Exception {
        Map<String, Object> message = ParamUtils.getMap(pParams, "Message", null);
        if (message == null)
            throw new Exception("No message provided");

        Schema avroSchema = resolveAvroSchema(m_schemaId, m_schemaStr);
        GenericRecord genericRecord = new GenericData.Record(avroSchema);

        for (Map.Entry<String, Object> entry : message.entrySet()) {
            genericRecord.put(entry.getKey(), entry.getValue());
        }

        return genericRecord;
    }

    private Schema resolveAvroSchema(int schemaId, String schemaStr) throws Exception {
        if (schemaId != 0) {
            ParsedSchema parsedSchema = m_registryClient.getSchemaById(schemaId);
            return (Schema) parsedSchema.rawSchema();
        }

        return new Schema.Parser().parse(schemaStr);
    }

    @Override
    protected void closeProducer() throws Exception {
        super.closeProducer();

        if (m_registryClient != null) {
            CachedSchemaRegistryClient client = m_registryClient;
            m_registryClient = null;
            client.close();
        }
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
