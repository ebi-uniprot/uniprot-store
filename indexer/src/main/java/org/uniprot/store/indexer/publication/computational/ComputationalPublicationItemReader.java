package org.uniprot.store.indexer.publication.computational;

import org.springframework.batch.item.ItemReader;
import org.uniprot.core.publication.ComputationallyMappedReference;
import org.uniprot.store.reader.publications.ComputationallyMappedReferenceConverter;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Iterator;

public class ComputationalPublicationItemReader
        implements ItemReader<ComputationallyMappedReference> {
    private final Iterator<String> lines;
    private final ComputationallyMappedReferenceConverter converter;

    public ComputationalPublicationItemReader(String filePath) throws IOException {
        lines = Files.lines(Paths.get(filePath)).iterator();
        converter = new ComputationallyMappedReferenceConverter();
    }

    @Override
    public ComputationallyMappedReference read() {
        if (lines.hasNext()) {
            return converter.convert(this.lines.next());
        }
        return null;
    }
}
