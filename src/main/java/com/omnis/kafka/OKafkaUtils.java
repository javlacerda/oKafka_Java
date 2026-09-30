package com.omnis.kafka;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListTopicsOptions;

import net.omnis.OmnisCalls.OModule;
import net.omnis.OmnisCalls.Response;
import net.omnis.OmnisCalls.SendResponse;

public class OKafkaUtils extends OModule {

    public Response getTopics(Map<String, Object> pParams) {
        Map<String, Object> data = new HashMap<>();

        try {
            Properties properties = new Properties();
            KafkaUtils.addKafkaConfigs(pParams, properties);

            boolean listInternal = ParamUtils.getBoolean(pParams, "ListInternal", false);
            int timeout = ParamUtils.getInt(pParams, "Timeout", 10);

            try (AdminClient adminClient = AdminClient.create(properties)) {
                Set<String> topicNames =
                        adminClient.listTopics(new ListTopicsOptions().listInternal(listInternal))
                            .names()
                            .get(timeout, TimeUnit.SECONDS);

                data.put("Topics", new ArrayList<>(topicNames));
                data.put(Constants.SUCCESS, true);
            }
        } catch (Exception ex) {
            data.put(Constants.SUCCESS, false);
            data.put("ErrorMessage", KafkaUtils.getAllExceptionMessages(ex));
        }

        return new SendResponse(data);
    }
}
