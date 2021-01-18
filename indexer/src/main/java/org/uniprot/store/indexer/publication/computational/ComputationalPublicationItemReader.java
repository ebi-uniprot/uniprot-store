package org.uniprot.store.indexer.publication.computational;

import java.io.IOException;
import java.util.List;

import org.springframework.batch.item.ItemReader;
import org.uniprot.core.publication.ComputationallyMappedReference;
import org.uniprot.store.reader.publications.ComputationallyMappedReferenceConverter;

public class ComputationalPublicationItemReader
        implements ItemReader<List<ComputationallyMappedReference>> {
    private final ComputationallyMappedReferenceConverter converter;

    public ComputationalPublicationItemReader(String filePath) throws IOException {
        converter = new ComputationallyMappedReferenceConverter(filePath);
    }

    @Override
    public List<ComputationallyMappedReference> read() {
        return converter.getNext();
    }
}
