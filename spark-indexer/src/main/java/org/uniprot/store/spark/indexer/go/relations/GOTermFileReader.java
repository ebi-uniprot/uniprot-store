package org.uniprot.store.spark.indexer.go.relations;

import static org.uniprot.store.spark.indexer.util.SparkUtils.getInputStream;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

import lombok.extern.slf4j.Slf4j;

import org.apache.hadoop.conf.Configuration;
import org.uniprot.core.cv.go.GeneOntologyEntry;
import org.uniprot.core.cv.go.impl.GeneOntologyEntryBuilder;
import org.uniprot.core.util.Utils;

/**
 * Class responsible to Load GO Terms from Hadoop File System
 *
 * @author lgonzales
 * @since 2019-10-25
 */
@Slf4j
class GOTermFileReader {
    private static final String COMMENT_PREFIX = "!";
    private static final String SEPARATOR = "\t";
    private static final String NOT_OBSOLETE = "N";
    private static final String FILENAME = "GO.terms";
    private final String filepath;
    private final Configuration hadoopConfig;

    GOTermFileReader(String filepath, Configuration hadoopConfig) {
        this.filepath = filepath;
        this.hadoopConfig = hadoopConfig;
    }

    List<GeneOntologyEntry> read() {
        String filename = filepath + File.separator + FILENAME;
        List<GeneOntologyEntry> lines = new ArrayList<>();
        try {
            try (BufferedReader br =
                    new BufferedReader(
                            new InputStreamReader(getInputStream(filename, hadoopConfig)))) {
                String line;
                while ((line = br.readLine()) != null) {
                    GeneOntologyEntry goTerm = readLine(line);
                    if (Utils.notNull(goTerm)) {
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

    private GeneOntologyEntry readLine(String line) {
        GeneOntologyEntry result = null;
        if (!line.startsWith(COMMENT_PREFIX)) {
            String[] tokens = line.split(SEPARATOR);
            if (tokens.length == 4) {
                if (tokens[1].equals(NOT_OBSOLETE)) {
                    result = new GeneOntologyEntryBuilder().id(tokens[0]).name(tokens[2]).build();
                }
            }
        }
        return result;
    }
}
