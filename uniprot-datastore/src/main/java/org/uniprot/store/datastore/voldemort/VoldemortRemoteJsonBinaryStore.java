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
import voldemort.versioning.Versioned;

import java.io.IOException;
import java.time.Duration;
import java.util.*;

/**
 * @author lgonzales
 * @param <T> entity that is being saved.
 */
public abstract class VoldemortRemoteJsonBinaryStore<T> implements VoldemortClient<T> {

    private static final Logger logger =
            LoggerFactory.getLogger(VoldemortRemoteJsonBinaryStore.class);
    private static final int DEFAULT_MAX_CONNECTION = 20;
    private static final String TIME_OUT_MILLIS = "60000";
    protected final StoreClient<String, byte[]> client;
    private final StoreClientFactory factory;
    protected final RetryPolicy<Object> retryPolicy;
    private final String storeName;

    public VoldemortRemoteJsonBinaryStore(String storeName, String... voldemortUrl) {
        this(DEFAULT_MAX_CONNECTION, storeName, voldemortUrl);
    }

    @Inject
    public VoldemortRemoteJsonBinaryStore(
            int maxConnection, String storeName, String... voldemortUrl) {

        Properties properties = new Properties();
        String timeOutMillis = TIME_OUT_MILLIS;

        properties.setProperty(ClientConfig.CONNECTION_TIMEOUT_MS_PROPERTY, timeOutMillis);
        properties.setProperty(ClientConfig.SOCKET_TIMEOUT_MS_PROPERTY, timeOutMillis);
        properties.setProperty(ClientConfig.ROUTING_TIMEOUT_MS_PROPERTY, timeOutMillis);
        properties.setProperty(
                ClientConfig.MAX_CONNECTIONS_PER_NODE_PROPERTY, Integer.toString(maxConnection));
        properties.setProperty(ClientConfig.SYS_CONNECTION_TIMEOUT_MS, timeOutMillis);
        properties.setProperty(ClientConfig.SYS_ROUTING_TIMEOUT_MS, timeOutMillis);
        properties.setProperty(ClientConfig.SYS_SOCKET_TIMEOUT_MS, timeOutMillis);

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
        this.retryPolicy =
            createRetryPolicy();
    }

    VoldemortRemoteJsonBinaryStore(String storeName, StoreClient<String, byte[]> client) {
        this.storeName = storeName;
        this.client = client;
        this.factory = null;
        this.retryPolicy = createRetryPolicy();
    }

    private RetryPolicy<Object> createRetryPolicy() {
        return new RetryPolicy<>()
            .handle(VoldemortException.class)
            .withDelay(Duration.ofMillis(1))
            .withMaxRetries(3);
    }

    @Override
    public void saveEntry(T entry) {
        Timer.Context time =
                MetricsUtil.getMetricRegistryInstance().timer("voldemort-save-entry-time").time();
        String acc = getStoreId(entry);
        byte[] binaryEntry;
        try {
            binaryEntry = getStoreObjectMapper().writeValueAsBytes(entry);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Unable to parse entry to binary json: ", e);
        }
        client.put(acc, binaryEntry);
        time.stop();
    }

    @Override
    public void truncate() {
        throw new UnsupportedOperationException(
                "Truncate remove voldemort is not a supported operation.");
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

    public List<T> getEntries(Iterable<String> accessions) {
        try {
            List<T> toReturn = new ArrayList<>();
            Map<String, Versioned<byte[]>> batch =
                    Failsafe.with(retryPolicy).get(() -> client.getAll(accessions));
            accessions.forEach(
                    acc -> {
                        Versioned<byte[]> versionedEntry = batch.get(acc);
                        if (versionedEntry != null) {
                            T entry = getEntryFromBinary(versionedEntry);
                            toReturn.add(entry);
                        }
                    });
            return toReturn;
        } catch (Exception e) {
            logger.warn("Error getting entry from BDB store.", e);
            throw new RuntimeException("Error getting entry from BDB store", e);
        }
    }

    public Map<String, T> getEntryMap(Iterable<String> acc) {
        Map<String, Versioned<byte[]>> all =
                Failsafe.with(retryPolicy).get(() -> client.getAll(acc));
        HashMap<String, T> stringEntryObjectHashMap = new HashMap<>();

        all.forEach((key, value) -> stringEntryObjectHashMap.put(key, getEntryFromBinary(value)));

        return stringEntryObjectHashMap;
    }

    public String getStoreName() {
        return this.storeName;
    }

    public void close() {
        this.factory.close();
    }

    protected T getEntryFromBinary(Versioned<byte[]> entryObjectVersioned) {
        try {
            return getStoreObjectMapper()
                    .readValue(entryObjectVersioned.getValue(), getEntryClass());
        } catch (IOException e) {
            throw new RuntimeException("Error getting entry from BDB store.", e);
        }
    }
}
