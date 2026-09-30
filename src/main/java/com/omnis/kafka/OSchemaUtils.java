package com.omnis.kafka;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;

import net.omnis.OmnisCalls.OModule;
import net.omnis.OmnisCalls.Response;
import net.omnis.OmnisCalls.SendResponse;

public class OSchemaUtils extends OModule {

    public Response getSubjects(Map<String, Object> pParams) {
        Map<String, Object> data = new HashMap<>();

        try {
            String schemaRegistryUrl = ParamUtils.getString(pParams, "SchemaRegistryUrl", "");

            try (SchemaRegistryClient registryClient = new CachedSchemaRegistryClient(schemaRegistryUrl, 100, KafkaUtils.getSchemaRegistryAuthConfigs(pParams))) {
                Collection<String> subjects = registryClient.getAllSubjects();
                data.put("Subjects", new ArrayList<>(subjects));
                data.put(Constants.SUCCESS, true);
            }
        } catch (Exception ex) {
            data.put(Constants.SUCCESS, false);
            data.put("ErrorMessage", KafkaUtils.getAllExceptionMessages(ex));
        }

        return new SendResponse(data);
    }

    public Response registerSchema(Map<String, Object> pParams) {
        Map<String, Object> data = new HashMap<>();

        try {
            String schemaRegistryUrl = ParamUtils.getString(pParams, "SchemaRegistryUrl", "");
            String subject = ParamUtils.getString(pParams, "Subject", "");
            String schemaStr = ParamUtils.getString(pParams, "Schema", "");

            ParsedSchema avroSchema = new AvroSchema(schemaStr);

            try (SchemaRegistryClient registryClient = new CachedSchemaRegistryClient(schemaRegistryUrl, 100, KafkaUtils.getSchemaRegistryAuthConfigs(pParams))) {
                int schemaId = registryClient.register(subject, avroSchema);
                data.put("SchemaId", schemaId);
                data.put(Constants.SUCCESS, true);
            }
        } catch (Exception ex) {
            data.put(Constants.SUCCESS, false);
            data.put("ErrorMessage", KafkaUtils.getAllExceptionMessages(ex));
        }

        return new SendResponse(data);
    }

    public Response getSchemaBySubject(Map<String, Object> pParams) {
        Map<String, Object> data = new HashMap<>();

        try {
            String schemaRegistryUrl = ParamUtils.getString(pParams, "SchemaRegistryUrl", "");
            String subject = ParamUtils.getString(pParams, "Subject", "");

            try (SchemaRegistryClient registryClient = new CachedSchemaRegistryClient(schemaRegistryUrl, 100, KafkaUtils.getSchemaRegistryAuthConfigs(pParams))) {
                SchemaMetadata metadata = registryClient.getLatestSchemaMetadata(subject);

                data.put("Id", metadata.getId());
                data.put("Version", metadata.getVersion());
                data.put("Schema", metadata.getSchema());
                data.put("Type", metadata.getSchemaType());
                data.put(Constants.SUCCESS, true);
            }
        } catch (Exception ex) {
            data.put(Constants.SUCCESS, false);
            data.put("ErrorMessage", KafkaUtils.getAllExceptionMessages(ex));
        }

        return new SendResponse(data);
    }

    public Response getSchemaById(Map<String, Object> pParams) {
        Map<String, Object> data = new HashMap<>();

        try {
            String schemaRegistryUrl = ParamUtils.getString(pParams, "SchemaRegistryUrl", "");
            int schemaId = ParamUtils.getInt(pParams, "Id", 0);

            try (SchemaRegistryClient registryClient = new CachedSchemaRegistryClient(schemaRegistryUrl, 100, KafkaUtils.getSchemaRegistryAuthConfigs(pParams))) {
                ParsedSchema parsedSchema = registryClient.getSchemaById(schemaId);

                // GET /schemas/ids/{id} only returns the schema itself, not its subject/version,
                // so Version and Type are left blank here too (matching the C++ SchemaInfo defaults).
                data.put("Id", schemaId);
                data.put("Version", 0);
                data.put("Schema", parsedSchema.canonicalString());
                data.put("Type", "");
                data.put(Constants.SUCCESS, true);
            }
        } catch (Exception ex) {
            data.put(Constants.SUCCESS, false);
            data.put("ErrorMessage", KafkaUtils.getAllExceptionMessages(ex));
        }

        return new SendResponse(data);
    }
}
