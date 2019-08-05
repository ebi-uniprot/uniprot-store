package org.uniprot.store.datastore.voldemort;

import com.codahale.metrics.Timer;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;
import net.jodah.failsafe.Failsafe;
import net.jodah.failsafe.RetryPolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import voldemort.VoldemortException;
import voldemort.client.ClientConfig;
import voldemort.client.SocketStoreClientFactory;
import voldemort.client.StoreClient;
import voldemort.client.StoreClientFactory;
import voldemort.versioning.Version;
import voldemort.versioning.Versioned;

import java.io.IOException;
import java.time.Duration;
import java.util.*;
import java.util.stream.Collectors;

/**
 *
 *
 * @author lgonzales
 * @param <T> entity that is being saved.
 */
public abstract class VoldemortRemoteJsonBinaryStore<T> implements VoldemortClient<T> {

    private final StoreClient<String, byte[]> client;

    private static final Logger logger = LoggerFactory.getLogger(VoldemortRemoteJsonBinaryStore.class);
    private static final int DEFAULT_MAX_CONNECTION = 20;
    private final StoreClientFactory factory;
    private final RetryPolicy<Object> retryPolicy;
    private final String storeName;

    public VoldemortRemoteJsonBinaryStore(String storeName, String... voldemortUrl) {
        this(DEFAULT_MAX_CONNECTION, storeName, voldemortUrl);
    }

    @Inject
    public VoldemortRemoteJsonBinaryStore(int maxConnection, String storeName, String... voldemortUrl) {

        Properties properties = new Properties();
        String TIME_OUT_MS = "60000";

        properties.setProperty(ClientConfig.CONNECTION_TIMEOUT_MS_PROPERTY, TIME_OUT_MS);
        properties.setProperty(ClientConfig.SOCKET_TIMEOUT_MS_PROPERTY, TIME_OUT_MS);
        properties.setProperty(ClientConfig.ROUTING_TIMEOUT_MS_PROPERTY, TIME_OUT_MS);
        properties.setProperty(ClientConfig.MAX_CONNECTIONS_PER_NODE_PROPERTY, Integer.toString(maxConnection));
        properties.setProperty(ClientConfig.SYS_CONNECTION_TIMEOUT_MS, TIME_OUT_MS);
        properties.setProperty(ClientConfig.SYS_ROUTING_TIMEOUT_MS, TIME_OUT_MS);
        properties.setProperty(ClientConfig.SYS_SOCKET_TIMEOUT_MS, TIME_OUT_MS);

        ClientConfig clientConfig = new ClientConfig(properties);

        clientConfig.setSocketBufferSize(1024 * 1204);
        clientConfig.setBootstrapUrls(voldemortUrl);
        factory = new SocketStoreClientFactory(clientConfig);
        try {
            if (factory.getFailureDetector().getAvailableNodeCount() == 0) {
                throw new RuntimeException("Voldemort server is not available");
            }
        } catch (Exception e) {
            throw new RuntimeException("Voldemort server is not available");
        }
        this.storeName = storeName;
        this.client = factory.getStoreClient(storeName);
        retryPolicy = new RetryPolicy<>()
                .handle(VoldemortException.class)
                .withDelay(Duration.ofMillis(1))
                .withMaxRetries(3);
    }

    @Override
    public void saveEntry(T entry) {
        Timer.Context time = MetricsUtil.getMetricRegistryInstance().timer("voldemort-save-entry-time").time();
        String acc = getStoreId(entry);
        byte[] binaryEntry;
        try {
            binaryEntry = getStoreObjectMapper().writeValueAsBytes(entry);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Unable to parse entry to binary json: ",e); //TODO: improve it
        }
        Version put = client.put(acc, binaryEntry);
        time.stop();
    }

    public abstract String getStoreId(T entry);

    public abstract ObjectMapper getStoreObjectMapper();

    public abstract Class<T> getEntryClass();

    public Optional<T> getEntry(String acc) {
        try {
            Versioned<byte[]> entryObjectVersioned =
                    Failsafe.with(retryPolicy).get(() -> client.get(acc));

            if (entryObjectVersioned != null) {
                T entry = getEntryFromBinary(entryObjectVersioned);
                return Optional.ofNullable(entry);
            } else {
                return Optional.empty();
            }
        } catch (Exception e) {
            logger.warn("Error getting entry from BDB store.", e);
            throw new RuntimeException("Error getting entry from BDB store", e);
        }
    }

    public List<T> getEntries(Iterable<String> acc) {
        try {
            Map<String, Versioned<byte[]>> all = Failsafe.with(retryPolicy).get(() -> client.getAll(acc));
            return all.values().stream()
                    .map(this::getEntryFromBinary)
                    .collect(Collectors.toList());
        } catch (Exception e) {
            logger.warn("Error getting entry from BDB store.", e);
            throw new RuntimeException("Error getting entry from BDB store", e);
        }
    }

    private T getEntryFromBinary(Versioned<byte[]> entryObjectVersioned) {
        try {
            return getStoreObjectMapper().readValue(entryObjectVersioned.getValue(),getEntryClass());
        } catch (IOException e) {
            throw new RuntimeException("Error getting entry from BDB store.", e);
        }
    }

    public Map<String, T> getEntryMap(Iterable<String> acc) {
        Map<String, Versioned<byte[]>> all = Failsafe.with(retryPolicy).get(() -> client.getAll(acc));
        HashMap<String, T> stringEntryObjectHashMap = new HashMap<>();

        all.forEach((key, value) ->
                stringEntryObjectHashMap.put(key, getEntryFromBinary(value)));

        return stringEntryObjectHashMap;
    }

    public String getStoreName() {
        return this.storeName;
    }

    public void close() {
        this.factory.close();
    }
}