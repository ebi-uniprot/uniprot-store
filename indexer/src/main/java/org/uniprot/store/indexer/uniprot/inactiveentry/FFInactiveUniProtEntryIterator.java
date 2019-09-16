package org.uniprot.store.indexer.uniprot.inactiveentry;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.zip.GZIPInputStream;

public class FFInactiveUniProtEntryIterator extends AbstractInactiveUniProtEntryIterator  {
    private final String filename;
    private BufferedReader reader;

    public FFInactiveUniProtEntryIterator(String filename) {
        this.filename = filename;
        init();
    }

    private void init() {
        try {
            if (filename.toLowerCase().endsWith(".gzip") || filename.toLowerCase().endsWith(".gz")) {
                reader = new BufferedReader(new InputStreamReader(new GZIPInputStream(new FileInputStream(filename))),
                                            16384);
            } else {
                reader = new BufferedReader(new InputStreamReader(new FileInputStream(filename)), 16384);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected InactiveUniProtEntry nextEntry() {
        try {
            String line = reader.readLine();
            hasResult = line != null;
            if (!hasResult)
                return null;
            return convert(line);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private InactiveUniProtEntry convert(String line) {
        String[] tokens = line.split(",");
        if (tokens.length == 3) {
            return InactiveUniProtEntry.from(tokens[0], tokens[1], tokens[2], "-");
        } else if (tokens.length == 4) {
            return InactiveUniProtEntry.from(tokens[0], tokens[1], tokens[2], tokens[3]);
        }
        return null;
    }

    @Override
    public void close() throws Exception {
        reader.close();

    }
}
