package org.uniprot.store.indexer.uniprot.go;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class GoTermFileReader {
    private static final String COMMENT_PREFIX = "!";
    private static final String SEPARATOR = "\t";
    private static final String NOT_OBSOLETE = "N";
    private static final String FILENAME = "GO.terms";
    private final String filepath;

    public GoTermFileReader(String filepath) {
        this.filepath = filepath;
    }

    public Map<String, GoTerm> read() {
        String filename = filepath + File.separator + FILENAME;
        try (Stream<String> stream = Files.lines(Paths.get(filename))) {
            return stream.map(this::readLine)
                    .filter(val -> val != null)
                    .collect(Collectors.toMap(GoTerm::getId, val -> val));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private GoTerm readLine(String line) {
        if (line.startsWith(COMMENT_PREFIX)) return null;

        String[] tokens = line.split(SEPARATOR);
        if (tokens.length == 4) {
            if (tokens[1].equals(NOT_OBSOLETE)) {
                return new GoTermImpl(tokens[0], tokens[2]);
            }
        }
        return null;
    }

    public static class GoTermImpl implements GoTerm {
        private final String goId;
        private final String name;

        public GoTermImpl(String goId, String name) {
            this.goId = goId;
            this.name = name;
        }

        @Override
        public String getId() {
            return goId;
        }

        @Override
        public String getName() {
            return name;
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + ((goId == null) ? 0 : goId.hashCode());
            result = prime * result + ((name == null) ? 0 : name.hashCode());
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) return true;
            if (obj == null) return false;
            if (getClass() != obj.getClass()) return false;
            GoTermImpl other = (GoTermImpl) obj;
            if (goId == null) {
                if (other.goId != null) return false;
            } else if (!goId.equals(other.goId)) return false;
            if (name == null) {
                if (other.name != null) return false;
            } else if (!name.equals(other.name)) return false;
            return true;
        }
    }
}
