package uk.ac.ebi.uniprot.datastore.voldemort;

import com.codahale.metrics.Timer;
import com.google.inject.Inject;
import net.jodah.failsafe.Failsafe;
import net.jodah.failsafe.RetryPolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.ac.ebi.uniprot.common.Utils;
import voldemort.VoldemortException;
import voldemort.client.ClientConfig;
import voldemort.client.SocketStoreClientFactory;
import voldemort.client.StoreClient;
import voldemort.client.StoreClientFactory;
import voldemort.versioning.Versioned;

import java.time.Duration;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Created 18/04/2016
 * <p>
 * This class contains common methods to save a voldemort entry remotely.
 *
 * @author wudong
 */
public abstract class VoldemortRemoteEntryStore<T> implements VoldemortClient<T> {
    private static final Logger logger = LoggerFactory.getLogger(VoldemortRemoteEntryStore.class);
    private static final int DEFAULT_MAX_CONNECTION = 20;
    protected final StoreClient<String, T> client;
    private final StoreClientFactory factory;
    private final RetryPolicy<Object> retryPolicy;
    private final String storeName;

    public VoldemortRemoteEntryStore(String storeName, String... voldemortUrl) {
        this(DEFAULT_MAX_CONNECTION, storeName, voldemortUrl);
    }

    @Inject
    public VoldemortRemoteEntryStore(int maxConnection, String storeName, String... voldemortUrl) {
        factory = new SocketStoreClientFactory(getClientConfig(maxConnection, voldemortUrl));
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
                .withDelay(Duration.ofMillis(2))
                .withMaxRetries(3);
    }

    @Override
    public void saveEntry(T entry) {
        Timer.Context time = MetricsUtil.getMetricRegistryInstance().timer("voldemort-save-entry-time").time();
        String acc = getStoreId(entry);
        client.put(acc, entry);
        time.stop();
    }

    public abstract String getStoreId(T entry);

    public Optional<T> getEntry(String acc) {
        try {
            Versioned<T> entryObjectVersioned = Failsafe.with(retryPolicy).get(() -> client.get(acc));

            if (Utils.nonNull(entryObjectVersioned)) {
                return Optional.of(entryObjectVersioned.getValue());
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
            Map<String, Versioned<T>> all = Failsafe.with(retryPolicy).get(() -> client.getAll(acc));
            return all.values().stream()
                    .map(Versioned::getValue)
                    .collect(Collectors.toList());
        } catch (Exception e) {
            logger.warn("Error getting entry from BDB store.", e);
            throw new RuntimeException("Error getting entry from BDB store", e);
        }
    }

    public Map<String, T> getEntryMap(Iterable<String> acc) {
        Map<String, Versioned<T>> all = Failsafe.with(retryPolicy).get(() -> client.getAll(acc));
        HashMap<String, T> stringEntryObjectHashMap = new HashMap<>();

        all.forEach((key, value) ->
                            stringEntryObjectHashMap.put(key, value.getValue()));

        return stringEntryObjectHashMap;
    }

    public String getStoreName() {
        return this.storeName;
    }

    public void close() {
        this.factory.close();
    }

    private ClientConfig getClientConfig(int maxConnection, String[] voldemortUrl) {
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
        return clientConfig;
    }
}
