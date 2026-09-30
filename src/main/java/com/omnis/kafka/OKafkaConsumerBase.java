package com.omnis.kafka;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.TimeZone;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;

import net.omnis.OmnisCalls.OModule;
import net.omnis.OmnisCalls.Response;
import net.omnis.OmnisCalls.SendResponse;

public abstract class OKafkaConsumerBase<V> extends OModule {

    protected KafkaConsumer<Object, V> m_consumer = null;

    public Response connect(Map<String, Object> pParams) {
        Map<String, Object> data = new HashMap<>();

        closeConsumer();

        try {
            m_consumer = createConsumer(pParams);
            String topic = ParamUtils.getString(pParams, "Topic", "");

            if (pParams.containsKey("Partition")) {
                int partition = ParamUtils.getInt(pParams, "Partition", 0);
                m_consumer.assign(Arrays.asList(new TopicPartition(topic, partition)));
            } else {
                m_consumer.subscribe(Arrays.asList(topic));
            }

            data.put(Constants.SUCCESS, true);
        } catch (Exception ex) {
            data.put(Constants.SUCCESS, false);
            data.put("ErrorMessage", KafkaUtils.getAllExceptionMessages(ex));
        }

        return new SendResponse(data);
    }

    public Response consume(Map<String, Object> pParams) {
        Map<String, Object> data = new HashMap<>();

        try {
            if (m_consumer != null) {
                ConsumerRecords<Object, V> records = m_consumer.poll(Duration.ofSeconds(ParamUtils.getInt(pParams, "Timeout", 60)));
                data.put(Constants.SUCCESS, true);

                if (records != null && records.count() > 0) {
                    ConsumerRecord<Object, V> record = records.iterator().next();
                    data.put("HasMessage", true);
                    data.put("Key", KafkaUtils.toResponseValue(record.key()));
                    data.put("Value", KafkaUtils.toResponseValue(record.value()));
                    data.put("Topic", record.topic());
                    data.put("Partition", record.partition());
                    data.put("Offset", record.offset());
                    data.put("Timestamp", LocalDateTime.ofInstant(Instant.ofEpochMilli(record.timestamp()), TimeZone.getDefault().toZoneId()).toString());

                    Iterator<Header> headerItr = record.headers().iterator();
                    if (headerItr.hasNext()) {
                        ArrayList<HeaderItem> headers = new ArrayList<>();
                        while (headerItr.hasNext()) {
                            Header header = headerItr.next();
                            headers.add(new HeaderItem(header.key(), new String(header.value())));
                        }

                        data.put("Headers", headers);
                    }
                } else {
                    data.put("HasMessage", false);
                }
            } else {
                data.put(Constants.SUCCESS, false);
                data.put("ErrorMessage", "Connection not open");
            }
        } catch (Exception ex) {
            data.put(Constants.SUCCESS, false);
            data.put("ErrorMessage", KafkaUtils.getAllExceptionMessages(ex));
        }

        return new SendResponse(data);
    }

    public Response commit(Map<String, Object> pParams) {
        Map<String, Object> data = new HashMap<>();

        try {
            if (m_consumer != null) {
                String topic = ParamUtils.getString(pParams, "Topic", "");
                int partition = ParamUtils.getInt(pParams, "Partition", 0);
                long offset = ParamUtils.getLong(pParams, "Offset", 0L);

                m_consumer.commitSync(Map.of(new TopicPartition(topic, partition), new OffsetAndMetadata(offset + 1)), Duration.ofSeconds(30));
                data.put(Constants.SUCCESS, true);
            } else {
                data.put(Constants.SUCCESS, false);
                data.put("ErrorMessage", "Connection not open");
            }
        } catch (Exception ex) {
            data.put(Constants.SUCCESS, false);
            data.put("ErrorMessage", KafkaUtils.getAllExceptionMessages(ex));
        }

        return new SendResponse(data);
    }

    public Response close(Map<String, Object> pParams) {
        closeConsumer();

        Map<String, Object> data = new HashMap<>();
        data.put(Constants.SUCCESS, true);
        return new SendResponse(data);
    }

    protected void closeConsumer() {
        if (m_consumer != null) {
            m_consumer.close();
            m_consumer = null;
        }
    }

    protected abstract KafkaConsumer<Object, V> createConsumer(Map<String, Object> pParams);
}
