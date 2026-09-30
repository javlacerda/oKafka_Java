package com.omnis.kafka;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.header.Header;

import net.omnis.OmnisCalls.OModule;
import net.omnis.OmnisCalls.Response;
import net.omnis.OmnisCalls.SendResponse;

public abstract class OKafkaProducerBase<V> extends OModule {

    protected KafkaProducer<Object, V> m_producer = null;
    protected String m_transactionalId = "";

    public Response setTransactionalId(Map<String, Object> pParams) {
        Map<String, Object> data = new HashMap<>();

        m_transactionalId = ParamUtils.getString(pParams, "TransactionalId", "");

        data.put(Constants.SUCCESS, true);
        return new SendResponse(data);
    }

    public Response connect(Map<String, Object> pParams) {
        Map<String, Object> data = new HashMap<>();

        try {
            closeProducer();
            m_producer = createProducer(pParams);

            if (!m_transactionalId.isEmpty())
                m_producer.initTransactions();

            data.put(Constants.SUCCESS, true);
        } catch (Exception ex) {
            data.put(Constants.SUCCESS, false);
            data.put("ErrorMessage", KafkaUtils.getAllExceptionMessages(ex));
        }

        return new SendResponse(data);
    }

    public Response produce(Map<String, Object> pParams) {
        Map<String, Object> data = new HashMap<>();

        try {
            if (pParams != null && pParams.size() > 0) {
                if (m_producer != null) {
                    int timeout = ParamUtils.getInt(pParams, "Timeout", 10);
                    String topic = ParamUtils.getString(pParams, "Topic", "");
                    Integer partition = ParamUtils.getInt(pParams, "Partition", 0);
                    if (partition.intValue() < 0)
                        partition = null;

                    ArrayList<Header> headers = KafkaUtils.getHeadersFromParams(pParams);
                    Object key = buildKey(pParams);
                    V value = buildValue(pParams);

                    ProducerRecord<Object, V> producerRecord = new ProducerRecord<>(topic, partition, key, value, headers);
                    Future<RecordMetadata> future = m_producer.send(producerRecord);
                    RecordMetadata metadata = future.get(timeout, TimeUnit.SECONDS);
                    KafkaUtils.setProduceResult(data, metadata);
                } else {
                    data.put(Constants.SUCCESS, false);
                    data.put("ErrorMessage", "Connection not open");
                }
            } else {
                data.put(Constants.SUCCESS, false);
                data.put("ErrorMessage", "No params provided");
            }
        } catch (Exception ex) {
            data.put(Constants.SUCCESS, false);
            data.put("ErrorMessage", KafkaUtils.getAllExceptionMessages(ex));
        }

        return new SendResponse(data);
    }

    public Response produceFromList(Map<String, Object> pParams) {
        Map<String, Object> data = new HashMap<>();

        try {
            if (pParams == null || pParams.size() == 0) {
                data.put(Constants.SUCCESS, false);
                data.put("ErrorMessage", "No params provided");
                return new SendResponse(data);
            }

            if (m_producer == null) {
                data.put(Constants.SUCCESS, false);
                data.put("ErrorMessage", "Connection not open");
                return new SendResponse(data);
            }

            String topic = ParamUtils.getString(pParams, "Topic", "");
            int timeout = ParamUtils.getInt(pParams, "Timeout", 10);
            Object messagesObj = pParams.get("Messages");

            if (messagesObj == null) {
                data.put("Results", new ArrayList<>());
                data.put(Constants.SUCCESS, true);
                return new SendResponse(data);
            }

            if (!(messagesObj instanceof List<?> messages))
                throw new Exception("Messages must be a list of rows");

            List<Future<RecordMetadata>> futures = new ArrayList<>(messages.size());
            for (Object item : messages) {
                if (!(item instanceof Map<?, ?> rawItem))
                    throw new Exception("Messages must be a list of rows");

                @SuppressWarnings("unchecked")
                Map<String, Object> messageParams = (Map<String, Object>) rawItem;

                Integer partition = ParamUtils.getInt(messageParams, "Partition", 0);
                if (partition.intValue() < 0)
                    partition = null;

                ArrayList<Header> headers = KafkaUtils.getHeadersFromParams(messageParams);
                Object key = buildKey(messageParams);
                V value = buildValue(messageParams);

                ProducerRecord<Object, V> producerRecord = new ProducerRecord<>(topic, partition, key, value, headers);
                futures.add(m_producer.send(producerRecord));
            }

            // A single timeout budget shared across the whole batch, mirroring
            // MCWKafka's KafkaProducer::produce() / okafka Python's produce_from_list().
            long deadlineNanos = System.nanoTime() + TimeUnit.SECONDS.toNanos(timeout);

            List<Object> results = new ArrayList<>(futures.size());
            boolean hasError = false;
            for (Future<RecordMetadata> future : futures) {
                Map<String, Object> itemResult = new HashMap<>();
                try {
                    long remainingNanos = Math.max(0, deadlineNanos - System.nanoTime());
                    RecordMetadata metadata = future.get(remainingNanos, TimeUnit.NANOSECONDS);
                    KafkaUtils.setProduceResult(itemResult, metadata);
                } catch (Exception ex) {
                    hasError = true;
                    itemResult.put(Constants.SUCCESS, false);
                    itemResult.put("ErrorMessage", KafkaUtils.getAllExceptionMessages(ex));
                }
                results.add(itemResult);
            }

            data.put("Results", results);
            if (hasError) {
                data.put(Constants.SUCCESS, false);
                data.put("ErrorMessage", "One or more messages failed to be delivered. Check Results for details.");
            } else {
                data.put(Constants.SUCCESS, true);
            }
        } catch (Exception ex) {
            data.put(Constants.SUCCESS, false);
            data.put("ErrorMessage", KafkaUtils.getAllExceptionMessages(ex));
        }

        return new SendResponse(data);
    }

    public Response close(Map<String, Object> pParams) {
        Map<String, Object> data = new HashMap<>();

        try {
            closeProducer();
            data.put(Constants.SUCCESS, true);
        } catch (Exception ex) {
            data.put(Constants.SUCCESS, false);
            data.put("ErrorMessage", KafkaUtils.getAllExceptionMessages(ex));
        }

        return new SendResponse(data);
    }

    public Response beginTransaction(Map<String, Object> pParams) {
        Map<String, Object> data = new HashMap<>();

        try {
            if (m_producer != null) {
                m_producer.beginTransaction();
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

    public Response commitTransaction(Map<String, Object> pParams) {
        Map<String, Object> data = new HashMap<>();

        try {
            if (m_producer != null) {
                m_producer.commitTransaction();
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

    public Response abortTransaction(Map<String, Object> pParams) {
        Map<String, Object> data = new HashMap<>();

        try {
            if (m_producer != null) {
                m_producer.abortTransaction();
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

    protected void closeProducer() throws Exception {
        if (m_producer != null) {
            m_producer.close();
            m_producer = null;
        }
    }

    // Subclasses must call this from createProducer(), before building the KafkaProducer.
    protected void applyTransactionalId(Properties properties) {
        if (!m_transactionalId.isEmpty())
            properties.setProperty(ProducerConfig.TRANSACTIONAL_ID_CONFIG, m_transactionalId);
    }

    protected abstract KafkaProducer<Object, V> createProducer(Map<String, Object> pParams) throws Exception;

    protected abstract Object buildKey(Map<String, Object> pParams) throws Exception;

    protected abstract V buildValue(Map<String, Object> pParams) throws Exception;
}
