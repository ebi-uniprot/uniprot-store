package org.uniprot.store.datastore.voldemort.data.performance;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.uniprot.store.datastore.voldemort.VoldemortClient;
import org.uniprot.store.datastore.voldemort.uniprot.VoldemortRemoteUniProtKBEntryStore;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;

/**
 * Created 19/06/2020
 *
 * @author Edd
 */
class PerformanceCheckerTest {
    @Test
    void initialisesAsExpected(@TempDir Path tempDir) throws IOException {
        Properties props = new Properties();
        PerformanceChecker.Config config = new PerformanceChecker.Config();

        assertThat(config.getClientMap(), is(nullValue()));

        List<String> numberFields =
                asList(
                        "sleepDurationBeforeRequest",
                        "reportSizeIfGreaterThanBytes",
                        "logInterval",
                        "reportSlowFetchTimeout",
                        "storeFetchRetryDelayMillis",
                        "storeFetchMaxRetries",
                        "corePoolSize",
                        "maxPoolSize",
                        "keepAliveTime");
        String numberAsString = "1";
        numberFields.forEach(key -> props.setProperty(key, numberAsString));
        props.setProperty("storesCSV", "uniprotkb,uniref");

        props.setProperty("store.uniprotkb.storeName", "uniprotkb");
        props.setProperty("store.uniprotkb.numberOfConnections", "1");
        props.setProperty("store.uniprotkb.host", "url");

        Path pretendFile = tempDir.resolve("pretendFile");
        Files.write(pretendFile, "anything".getBytes());
        props.setProperty("filePath", pretendFile.toAbsolutePath().toString());

        new TestablePerformanceChecker().init(props, config);

        assertThat(config.getClientMap(), is(notNullValue()));
        assertThat(config.getStores(), contains("uniprotkb", "uniref"));
        assertThat(config.getRetryPolicy(), is(notNullValue()));
        assertThat(config.getExecutorService(), is(notNullValue()));
        assertThat(config.getLines(), is(notNullValue()));
        assertThat(config.getStatisticsSummary(), is(notNullValue()));

        int number = Integer.parseInt(numberAsString);
        assertThat(config.getSleepDurationBeforeRequest(), is(number));
        assertThat(config.getReportSizeIfGreaterThanBytes(), is(number));
        assertThat(config.getReportSlowFetchTimeout(), is(number));
        assertThat(config.getLogInterval(), is(number));
    }

    @Test
    void missingKeyCausesException() {
        Properties props = new Properties();
        PerformanceChecker.Config config = new PerformanceChecker.Config();

        assertThrows(
                IllegalArgumentException.class,
                () -> new TestablePerformanceChecker().init(props, config));
    }

    @Test
    void noStoresCausesException() {
        Properties props = new Properties();

        assertThrows(
                IllegalStateException.class, () -> new TestablePerformanceChecker().createClientMap(props));
    }

    @Test
    void canCreateUniProtKBStore() {
        Properties props = new Properties();
        props.setProperty("store.uniprotkb.storeName", "uniprotkb");
        props.setProperty("store.uniprotkb.numberOfConnections", "1");
        props.setProperty("store.uniprotkb.host", "url");

        Map<String, VoldemortClient<?>> clientMap = new TestablePerformanceChecker().createClientMap(props);

        assertThat(clientMap.get("uniprotkb"), is(notNullValue()));
    }

    @Test
    void canCreateUniRefStore() {
        Properties props = new Properties();
        props.setProperty("store.uniref.storeName", "uniref");
        props.setProperty("store.uniref.numberOfConnections", "1");
        props.setProperty("store.uniref.host", "url");

        Map<String, VoldemortClient<?>> clientMap = new TestablePerformanceChecker().createClientMap(props);

        assertThat(clientMap.get("uniref"), is(notNullValue()));
    }

    @Test
    void canCreateUniParcStore() {
        Properties props = new Properties();
        props.setProperty("store.uniparc.storeName", "uniparc");
        props.setProperty("store.uniparc.numberOfConnections", "1");
        props.setProperty("store.uniparc.host", "url");

        Map<String, VoldemortClient<?>> clientMap = new TestablePerformanceChecker().createClientMap(props);

        assertThat(clientMap.get("uniparc"), is(notNullValue()));
    }

    static class TestablePerformanceChecker extends PerformanceChecker {
//        @Override
//        Map<String, VoldemortClient<?>> createClientMap(Properties properties) {
//            return null;
//        }

        @Override
        VoldemortClient<?> createUniProtKBStore(
                int connections, String storeName, String host) {
            return mock(VoldemortClient.class);
        }

        @Override
        VoldemortClient<?> createUniRefStore(
                int connections, String storeName, String host) {
            return mock(VoldemortClient.class);
        }

        @Override
        VoldemortClient<?> createUniParcStore(
                int connections, String storeName, String host) {
            return mock(VoldemortClient.class);
        }
    }
}
