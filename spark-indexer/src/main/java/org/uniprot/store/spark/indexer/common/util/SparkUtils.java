package org.uniprot.store.spark.indexer.common.util;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.uniprot.store.search.SolrCollection;
import org.uniprot.store.spark.indexer.common.store.DataStore;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import lombok.extern.slf4j.Slf4j;

/**
 * @author lgonzales
 * @since 2019-11-01
 */
@Slf4j
public class SparkUtils {

    private static final String COMMA_SEPARATOR = ",";

    private SparkUtils() {}

    public static String getInputReleaseDirPath(Config applicationConfig, String releaseName) {
        String inputDir = applicationConfig.getString("input.directory.path");
        return inputDir + releaseName + File.separator;
    }

    public static String getInputReleaseMainThreadDirPath(
            Config applicationConfig, String releaseName) {
        String inputDir = applicationConfig.getString("input.directory.main.thread.path");
        return inputDir + releaseName + File.separator;
    }

    public static String getOutputReleaseDirPath(Config applicationConfig, String releaseName) {
        String inputDir = applicationConfig.getString("output.directory.path");
        return inputDir + releaseName + File.separator;
    }

    public static String getCollectionOutputReleaseDirPath(
            Config config, String releaseName, SolrCollection collection) {
        return getOutputReleaseDirPath(config, releaseName) + collection.toString();
    }

    public static List<String> readLines(String filePath, Configuration hadoopConfig) {
        List<String> lines = new ArrayList<>();
        try (BufferedReader br =
                new BufferedReader(new InputStreamReader(getInputStream(filePath, hadoopConfig)))) {
            for (String line = null; (line = br.readLine()) != null; ) {
                lines.add(line);
            }
        } catch (IOException ioe) {
            log.warn("Error while loading Supporting data file on path: " + filePath, ioe);
        }
        return lines;
    }

    @SuppressWarnings("squid:S2095")
    public static InputStream getInputStream(String filePath, Configuration hadoopConfig)
            throws IOException {
        InputStream inputStream = ClassLoader.getSystemResourceAsStream(filePath);
        if (inputStream == null) {
            inputStream = new FileInputStream(filePath);
        }
        return inputStream;
    }

    public static Config loadApplicationProperty() {
        return loadApplicationProperty("application");
    }

    public static Config loadApplicationProperty(String baseName) {
        return ConfigFactory.load(baseName);
    }

    public static JavaSparkContext loadSparkContext(Config applicationConfig, String sparkMaster) {
        if (sparkMaster.startsWith("local")) {
            return getLocalSparkContext(applicationConfig, sparkMaster);
        } else {
            return getRemoteSparkContext(applicationConfig, sparkMaster);
        }
    }

    private static JavaSparkContext getLocalSparkContext(
            Config applicationConfig, String sparkMaster) {
        String applicationName = applicationConfig.getString("spark.application.name");
        SparkConf sparkConf =
                new SparkConf()
                        .setAppName(applicationName)
                        .setMaster(sparkMaster)
                        .set("spark.driver.allowMultipleContexts", "true")
                        .set("spark.driver.host", "localhost")
                        .set("spark.sql.caseSensitive", "true")
                        .set("spark.shuffle.useOldFetchProtocol", "true");
        return new JavaSparkContext(sparkConf);
    }

    private static JavaSparkContext getRemoteSparkContext(
            Config applicationConfig, String sparkMaster) {
        String applicationName = applicationConfig.getString("spark.application.name");
        SparkConf sparkConf =
                new SparkConf()
                        .setAppName(applicationName)
                        .setMaster(sparkMaster)
                        .set("spark.scheduler.mode", "FAIR")
                        .set("spark.sql.caseSensitive", "true")
                        .set("spark.shuffle.useOldFetchProtocol", "true")
                        .set(
                                "spark.scheduler.allocation.file",
                                applicationConfig.getString("uniprot.scheduler.file.path"));
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
        sparkContext.setLocalProperty("spark.scheduler.pool", "uniprotPool");
        return sparkContext;
    }

    public static List<SolrCollection> getSolrCollection(String collectionsName) {
        List<SolrCollection> result = new ArrayList<>();
        String[] collectionsNameList = collectionsName.toLowerCase().split(COMMA_SEPARATOR);
        for (String collectionName : collectionsNameList) {
            try {
                result.add(SolrCollection.valueOf(collectionName));
            } catch (Exception e) {
                throw new IllegalArgumentException(
                        "Invalid solr collection name: " + collectionName);
            }
        }
        return result;
    }

    public static List<DataStore> getDataStores(String dataStore) {
        List<DataStore> result = new ArrayList<>();
        String[] dataStoreList = dataStore.toLowerCase().split(COMMA_SEPARATOR);
        for (String store : dataStoreList) {
            DataStore storeItem =
                    Arrays.stream(DataStore.values())
                            .filter(item -> item.getName().equalsIgnoreCase(store.trim()))
                            .findFirst()
                            .orElseThrow(
                                    () ->
                                            new IllegalArgumentException(
                                                    "Invalid data store name: " + dataStore));
            result.add(storeItem);
        }
        return result;
    }

    public static <T> T getNotNullEntry(T entry1, T entry2) {
        T result = entry1;
        if (result == null) {
            result = entry2;
        }
        return result;
    }

    public static <T> boolean isThereAnyNullEntry(T entry1, T entry2) {
        return entry1 == null || entry2 == null;
    }

    public static int scaleAnnotationScore(double score) {
        int q = (int) (score / 20d);
        return q > 4 ? 5 : q + 1;
    }
}
