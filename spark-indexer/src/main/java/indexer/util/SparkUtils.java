package indexer.util;

import indexer.SparkRDDDriverProgram;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.*;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.*;

/**
 * @author lgonzales
 * @since 2019-11-01
 */
@Slf4j
public class SparkUtils {

    public static List<String> readLines(String filePath, Configuration hadoopConfig) {
        List<String> lines = new ArrayList<>();
        try (BufferedReader br = new BufferedReader(new InputStreamReader(getInputStream(filePath, hadoopConfig)))) {
            for (String line = null; (line = br.readLine()) != null; ) {
                lines.add(line);
            }
        } catch (IOException ioe) {
            log.warn("Error while loading Supporting data file on path: " + filePath, ioe);
        }
        return lines;
    }

    public static InputStream getInputStream(String filePath, Configuration hadoopConfig) throws IOException {
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
            //try to load from the directory that the application is being executed
            URL resourceURL = SparkRDDDriverProgram.class.getProtectionDomain().getCodeSource().getLocation();
            URLClassLoader urlLoader = new URLClassLoader(new java.net.URL[]{resourceURL});
            return ResourceBundle.getBundle("application", Locale.getDefault(), urlLoader);
        } catch (MissingResourceException e) {
            // load from the classpath
            return ResourceBundle.getBundle("application");
        }
    }


}
