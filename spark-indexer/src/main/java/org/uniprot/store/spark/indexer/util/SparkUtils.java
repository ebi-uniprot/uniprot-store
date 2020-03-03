package org.uniprot.store.spark.indexer.util;

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
import org.uniprot.store.spark.indexer.WriteIndexDocumentsToHDFSMain;

/**
 * @author lgonzales
 * @since 2019-11-01
 */
@Slf4j
public class SparkUtils {

    public static String getInputReleaseDirPath(
            ResourceBundle applicationConfig, String releaseName) {
        String inputDir = applicationConfig.getString("input.directory.path");
        return inputDir + File.separator + releaseName + File.separator;
    }

    public static String getOutputReleaseDirPath(
            ResourceBundle applicationConfig, String releaseName) {
        String inputDir = applicationConfig.getString("output.directory.path");
        return inputDir + File.separator + releaseName + File.separator;
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

    public static InputStream getInputStream(String filePath, Configuration hadoopConfig)
            throws IOException {
        InputStream inputStream = SparkUtils.class.getClassLoader().getResourceAsStream(filePath);
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
        try {
            // try to load from the directory that the application is being executed
            URL resourceURL =
                    WriteIndexDocumentsToHDFSMain.class
                            .getProtectionDomain()
                            .getCodeSource()
                            .getLocation();
            URLClassLoader urlLoader = new URLClassLoader(new java.net.URL[] {resourceURL});
            return ResourceBundle.getBundle("application", Locale.getDefault(), urlLoader);
        } catch (MissingResourceException e) {
            // load from the classpath
            return ResourceBundle.getBundle("application");
        }
    }

    public static JavaSparkContext loadSparkContext(ResourceBundle applicationConfig) {
        String sparkMaster = applicationConfig.getString("spark.master");
        SparkConf sparkConf =
                new SparkConf()
                        .setAppName(applicationConfig.getString("spark.application.name"))
                        .setMaster(sparkMaster);
        if (sparkMaster.startsWith("local")) {
            sparkConf = sparkConf.set("spark.driver.host", "localhost");
        } else {
            // Allow fair scheduling for parallel jobs
            sparkConf = sparkConf.set("spark.scheduler.mode", "FAIR");
        }
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
        sparkContext.setLocalProperty("spark.scheduler.pool", "uniprotIndexerWriter");
        return sparkContext;
    }

    public static SolrCollection getSolrCollection(String collectionName) {
        try {
            return SolrCollection.valueOf(collectionName);
        } catch (Exception e) {
            throw new RuntimeException("Invalid solr collection name: " + collectionName);
        }
    }
}
