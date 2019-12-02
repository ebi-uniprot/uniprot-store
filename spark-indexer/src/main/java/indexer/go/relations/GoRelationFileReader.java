package indexer.go.relations;

import static indexer.util.SparkUtils.getInputStream;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import lombok.extern.slf4j.Slf4j;

import org.apache.hadoop.conf.Configuration;

/**
 * @author lgonzales
 * @since 2019-10-25
 */
@Slf4j
class GoRelationFileReader {
    private static final String SEPARATOR = "\t";
    private final String goRelationFPath;
    private final Configuration hadoopConfig;

    private static final String FILENAME = "GO.relations";

    GoRelationFileReader(String goRelationFPath, Configuration hadoopConfig) {
        this.hadoopConfig = hadoopConfig;
        this.goRelationFPath = goRelationFPath;
    }

    Map<String, Set<String>> read() {
        String filename = goRelationFPath + File.separator + FILENAME;
        Map<String, Set<String>> lines = new HashMap<>();
        try (BufferedReader br =
                new BufferedReader(new InputStreamReader(getInputStream(filename, hadoopConfig)))) {
            String line;
            while ((line = br.readLine()) != null) {
                String[] tokens = line.split(SEPARATOR);
                if (tokens.length >= 3) {
                    String key = tokens[0];
                    lines.computeIfAbsent(key, k -> new HashSet<>());
                    lines.get(key).add(tokens[2]);
                }
            }
        } catch (IOException e) {
            log.error("IOException loading file: " + filename, e);
            throw new RuntimeException("IOException loading file: " + filename, e);
        }
        return lines;
    }
}
