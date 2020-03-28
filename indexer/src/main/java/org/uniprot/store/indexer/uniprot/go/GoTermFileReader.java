package org.uniprot.store.indexer.uniprot.go;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.uniprot.core.cv.go.GeneOntologyEntry;
import org.uniprot.core.cv.go.impl.GeneOntologyEntryBuilder;

public class GoTermFileReader {
    private static final String COMMENT_PREFIX = "!";
    private static final String SEPARATOR = "\t";
    private static final String NOT_OBSOLETE = "N";
    private static final String FILENAME = "GO.terms";
    private final String filepath;

    public GoTermFileReader(String filepath) {
        this.filepath = filepath;
    }

    public Map<String, GeneOntologyEntry> read() {
        String filename = filepath + File.separator + FILENAME;
        try (Stream<String> stream = Files.lines(Paths.get(filename))) {
            return stream.map(this::readLine)
                    .filter(Objects::nonNull)
                    .collect(Collectors.toMap(GeneOntologyEntry::getId, val -> val));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private GeneOntologyEntry readLine(String line) {
        if (line.startsWith(COMMENT_PREFIX)) return null;

        String[] tokens = line.split(SEPARATOR);
        if (tokens.length == 4) {
            if (tokens[1].equals(NOT_OBSOLETE)) {
                return new GeneOntologyEntryBuilder().id(tokens[0]).name(tokens[2]).build();
            }
        }
        return null;
    }
}
