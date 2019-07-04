package uk.ac.ebi.uniprot.indexer.uniprot.go;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Stream;

// TODO: 04/07/19 test this class
public class GoRelationFileReader {
    private static final String COMMENT_PREFIX = "!";
    private static final String SEPARATOR = "\t";
    private static final String PART_OF = "part_of";
    private static final String IS_A = "is_a";
    private Map<String, Set<String>> isAMap = new HashMap<>();
    private Map<String, Set<String>> isPartMap = new HashMap<>();
    private final String goRelationFPath;

    private static final String FILENAME = "GO.relations";

    private final Logger logger = LoggerFactory.getLogger(getClass());

    public GoRelationFileReader(String goRelationFPath) {
        this.goRelationFPath = goRelationFPath;
    }

    public Map<String, Set<String>> getIsAMap() {
        return isAMap;
    }

    public Map<String, Set<String>> getIsPartMap() {
        return isPartMap;
    }

    public void read() {
        String filename = goRelationFPath + File.separator + FILENAME;
        logger.info("Reading GO relation from {}", filename);
        try (Stream<String> stream = Files.lines(Paths.get(filename))) {
            stream.forEach(this::readLine);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void readLine(String line) {
        if (line.startsWith(COMMENT_PREFIX))
            return;
        String[] tokens = line.split(SEPARATOR);
        if (tokens.length == 3) {

            if (tokens[1].equals(IS_A)) {
                add2Map(isAMap, tokens[0], tokens[2]);
            } else if (tokens[1].equals(PART_OF)) {
                add2Map(isPartMap, tokens[0], tokens[2]);
            }
        }
    }

    private void add2Map(Map<String, Set<String>> map, String key, String value) {
        Set<String> values = map.computeIfAbsent(key, k -> new HashSet<>());
        values.add(value);
    }
}
