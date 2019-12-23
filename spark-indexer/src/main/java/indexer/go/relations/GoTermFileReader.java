package indexer.go.relations;

import static indexer.util.SparkUtils.getInputStream;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

import lombok.extern.slf4j.Slf4j;

import org.apache.hadoop.conf.Configuration;

/**
 * Class responsible to Load Go Terms from Hadoop File System
 *
 * @author lgonzales
 * @since 2019-10-25
 */
@Slf4j
class GoTermFileReader {
    private static final String COMMENT_PREFIX = "!";
    private static final String SEPARATOR = "\t";
    private static final String NOT_OBSOLETE = "N";
    private static final String FILENAME = "GO.terms";
    private final String filepath;
    private final Configuration hadoopConfig;

    GoTermFileReader(String filepath, Configuration hadoopConfig) {
        this.filepath = filepath;
        this.hadoopConfig = hadoopConfig;
    }

    List<GoTerm> read() {
        String filename = filepath + File.separator + FILENAME;
        List<GoTerm> lines = new ArrayList<>();
        try {
            try (BufferedReader br =
                    new BufferedReader(
                            new InputStreamReader(getInputStream(filename, hadoopConfig)))) {
                String line;
                while ((line = br.readLine()) != null) {
                    GoTerm goTerm = readLine(line);
                    if (goTerm != null) {
                        lines.add(goTerm);
                    }
                }
            }
        } catch (IOException e) {
            log.error("IOException loading file: " + filename, e);
            throw new RuntimeException("IOException loading file: " + filename, e);
        }
        return lines;
    }

    private GoTerm readLine(String line) {
        GoTerm result = null;
        if (!line.startsWith(COMMENT_PREFIX)) {
            String[] tokens = line.split(SEPARATOR);
            if (tokens.length == 4) {
                if (tokens[1].equals(NOT_OBSOLETE)) {
                    result = new GoTermImpl(tokens[0], tokens[2]);
                }
            }
        }
        return result;
    }
}
