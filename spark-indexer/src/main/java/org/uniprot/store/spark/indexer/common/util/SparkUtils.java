package org.uniprot.store.spark.indexer.common.util;

import java.io.*;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.*;

import lombok.extern.slf4j.Slf4j;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.uniprot.store.search.SolrCollection;
import org.uniprot.store.spark.indexer.common.store.DataStore;
import org.uniprot.store.spark.indexer.main.WriteIndexDocumentsToHDFSMain;

/**
 * @author lgonzales
 * @since 2019-11-01
 */
@Slf4j
public class SparkUtils {

    private static final String COMMA_SEPARATOR = ",";
    private static final String SPARK_MASTER = "spark.master";

    private SparkUtils() {}

    public static String getInputReleaseDirPath(
            ResourceBundle applicationConfig, String releaseName) {
        String inputDir = applicationConfig.getString("input.directory.path");
        return inputDir + releaseName + File.separator;
    }

    public static String getInputReleaseMainThreadDirPath(
            ResourceBundle applicationConfig, String releaseName) {
        String inputDir = applicationConfig.getString("input.directory.main.thread.path");
        return inputDir + releaseName + File.separator;
    }

    public static String getOutputReleaseDirPath(
            ResourceBundle applicationConfig, String releaseName) {
        String inputDir = applicationConfig.getString("output.directory.path");
        return inputDir + releaseName + File.separator;
    }

    public static String getCollectionOutputReleaseDirPath(
            ResourceBundle config, String releaseName, SolrCollection collection) {
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
            if (filePath.startsWith("hdfs:")) {
                FileSystem fs = FileSystem.get(hadoopConfig);
                inputStream = fs.open(new Path(filePath)).getWrappedStream();
            } else {
                inputStream = new FileInputStream(filePath);
            }
        }
        return inputStream;
    }

    public static ResourceBundle loadApplicationProperty() {
        URL resourceURL =
                WriteIndexDocumentsToHDFSMain.class
                        .getProtectionDomain()
                        .getCodeSource()
                        .getLocation();
        try (URLClassLoader urlLoader = new URLClassLoader(new java.net.URL[] {resourceURL})) {
            // try to load from the directory that the application is being executed
            return ResourceBundle.getBundle("application", Locale.getDefault(), urlLoader);
        } catch (MissingResourceException | IOException e) {
            // load from the classpath
            return ResourceBundle.getBundle("application");
        }
    }

    public static JavaSparkContext loadSparkContext(ResourceBundle applicationConfig) {
        String sparkMaster = applicationConfig.getString(SPARK_MASTER);
        if (sparkMaster.startsWith("local")) {
            return getLocalSparkContext(applicationConfig);
        } else {
            return getRemoteSparkContext(applicationConfig);
        }
    }

    private static JavaSparkContext getLocalSparkContext(ResourceBundle applicationConfig) {
        String applicationName = applicationConfig.getString("spark.application.name");
        String sparkMaster = applicationConfig.getString(SPARK_MASTER);
        SparkConf sparkConf =
                new SparkConf()
                        .setAppName(applicationName)
                        .setMaster(sparkMaster)
                        .set("spark.driver.host", "localhost");
        return new JavaSparkContext(sparkConf);
    }

    private static JavaSparkContext getRemoteSparkContext(ResourceBundle applicationConfig) {
        String applicationName = applicationConfig.getString("spark.application.name");
        String sparkMaster = applicationConfig.getString(SPARK_MASTER);
        SparkConf sparkConf =
                new SparkConf()
                        .setAppName(applicationName)
                        .setMaster(sparkMaster)
                        .set("spark.scheduler.mode", "FAIR")
                        .set("spark.scheduler.allocation.file", "uniprot-fair-scheduler.xml");
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
}
