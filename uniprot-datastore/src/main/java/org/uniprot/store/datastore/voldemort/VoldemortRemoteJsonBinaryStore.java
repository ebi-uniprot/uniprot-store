package org.uniprot.store.datastore.voldemort;

import java.io.IOException;
import java.time.Duration;
import java.util.*;

import net.jodah.failsafe.Failsafe;
import net.jodah.failsafe.RetryPolicy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import voldemort.VoldemortException;
import voldemort.client.ClientConfig;
import voldemort.client.SocketStoreClientFactory;
import voldemort.client.StoreClient;
import voldemort.client.StoreClientFactory;
import voldemort.versioning.ObsoleteVersionException;
import voldemort.versioning.Versioned;

import com.aayushatharva.brotli4j.Brotli4jLoader;
import com.aayushatharva.brotli4j.decoder.Decoder;
import com.aayushatharva.brotli4j.decoder.DecoderJNI;
import com.aayushatharva.brotli4j.decoder.DirectDecompress;
import com.aayushatharva.brotli4j.encoder.Encoder;
import com.codahale.metrics.Timer;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;

/**
 * @author lgonzales
 * @param <T> entity that is being saved.
 */
public abstract class VoldemortRemoteJsonBinaryStore<T> implements VoldemortClient<T> {

    public static final int BROTLI_COMPRESSION_LEVEL = 9;
    private static final Logger logger =
            LoggerFactory.getLogger(VoldemortRemoteJsonBinaryStore.class);
    private static final int DEFAULT_MAX_CONNECTION = 20;
    private static final String TIME_OUT_MILLIS = "60000";
    private final StoreClient<String, byte[]> client;
    private final StoreClientFactory factory;
    private final RetryPolicy<Object> retryPolicy;
    private final String storeName;

    public VoldemortRemoteJsonBinaryStore(String storeName, String... voldemortUrl) {
        this(DEFAULT_MAX_CONNECTION, storeName, voldemortUrl);
    }

    @Inject
    public VoldemortRemoteJsonBinaryStore(
            int maxConnection, String storeName, String... voldemortUrl) {

        Brotli4jLoader.ensureAvailability();
        this.storeName = storeName;

        Properties properties = getVoldemortProperties(maxConnection);
        ClientConfig clientConfig = new ClientConfig(properties);
        clientConfig.setSocketBufferSize(1024 * 1204);
        clientConfig.setBootstrapUrls(voldemortUrl);

        factory = getSocketClientFactory(clientConfig);
        this.client = getStoreClient(storeName);

        retryPolicy =
                new RetryPolicy<>()
                        .handle(VoldemortException.class)
                        .withDelay(Duration.ofMillis(1))
                        .withMaxRetries(3);
    }

    StoreClient<String, byte[]> getStoreClient(String storeName) {
        return factory.getStoreClient(storeName);
    }

    SocketStoreClientFactory getSocketClientFactory(ClientConfig clientConfig) {
        SocketStoreClientFactory clientFactory = getClientFactory(clientConfig);
        try {
            if (clientFactory.getFailureDetector().getAvailableNodeCount() == 0) {
                throw new RetrievalException("Voldemort server is not available");
            }
        } catch (Exception e) {
            throw new RetrievalException("Voldemort server is not available");
        }
        return clientFactory;
    }

    SocketStoreClientFactory getClientFactory(ClientConfig clientConfig) {
        return new SocketStoreClientFactory(clientConfig);
    }

    Properties getVoldemortProperties(int maxConnection) {
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
        return properties;
    }

    @Override
    public void saveEntry(T entry) {
        String acc = getStoreId(entry);
        try {
            doSave(entry);
        } catch (ObsoleteVersionException e) {
            logger.warn(acc + " already saved in voldemort, ignoring it");
        }
    }

    @Override
    public void saveOrUpdateEntry(T entry) {
        doSave(entry);
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
            throw new RetrievalException("Error getting entry from BDB store", e);
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
            logger.warn("Error getting entries from BDB store.", e);
            throw new RetrievalException("Error getting entry from BDB store", e);
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

    private T getEntryFromBinary(Versioned<byte[]> entryObjectVersioned) {
        try {
            DirectDecompress directDecompress = Decoder.decompress(entryObjectVersioned.getValue());

            if (directDecompress.getResultStatus() == DecoderJNI.Status.DONE) {
                return getStoreObjectMapper()
                        .readValue(directDecompress.getDecompressedData(), getEntryClass());
            }

            throw new IOException("Unable to decompress the entry");

        } catch (IOException e) {
            throw new RetrievalException("Error getting entry from BDB store.", e);
        }
    }

    private void doSave(T entry) {
        Timer.Context time =
                MetricsUtil.getMetricRegistryInstance().timer("voldemort-save-entry-time").time();
        String acc = getStoreId(entry);
        byte[] binaryEntry;
        try {
            binaryEntry = getStoreObjectMapper().writeValueAsBytes(entry);
            byte[] compressed =
                    Encoder.compress(binaryEntry, new Encoder.Parameters().setQuality(BROTLI_COMPRESSION_LEVEL));
            client.put(acc, compressed);
        } catch (JsonProcessingException e) {
            throw new RetrievalException("Unable to parse entry to binary json: ", e);
        } catch (IOException ioe) {
            throw new RetrievalException("Unable to compress entry with id " + acc, ioe);
        }
        time.stop();
    }
}
