package indexer.go.relations;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

import static indexer.util.SparkUtils.getInputStream;

/**
 * @author lgonzales
 * @since 2019-10-25
 */
@Slf4j
public class GoTermFileReader {
    private static final String COMMENT_PREFIX = "!";
    private static final String SEPARATOR = "\t";
    private static final String NOT_OBSOLETE = "N";
    private static final String FILENAME = "GO.terms";
    private final String filepath;
    private final Configuration hadoopConfig;

    public GoTermFileReader(String filepath, Configuration hadoopConfig) {
        this.filepath = filepath;
        this.hadoopConfig = hadoopConfig;
    }

    public List<GoTerm> read() {
        String filename = filepath + File.separator + FILENAME;
        List<GoTerm> lines = new ArrayList<>();
        try {
            try (BufferedReader br = new BufferedReader(new InputStreamReader(getInputStream(filename, hadoopConfig)))) {
                String line;
                while ((line = br.readLine()) != null) {
                    lines.add(readLine(line));
                }
            }
        } catch (IOException e) {
            log.error("Problem loading file.", e);
        }
        return lines;
    }

    private GoTerm readLine(String line) {
        if (line.startsWith(COMMENT_PREFIX))
            return null;

        String[] tokens = line.split(SEPARATOR);
        if (tokens.length == 4) {
            if (tokens[1].equals(NOT_OBSOLETE)) {
                return new GoTermImpl(tokens[0], tokens[2]);
            }
        }
        return null;
    }

}
