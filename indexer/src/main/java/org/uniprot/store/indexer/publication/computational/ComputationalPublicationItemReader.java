package org.uniprot.store.indexer.publication.computational;

import java.io.IOException;
import java.util.List;

import org.springframework.batch.item.ItemReader;
import org.uniprot.core.publication.ComputationallyMappedReference;
import org.uniprot.store.reader.publications.ComputationallyMappedReferenceConverter;
import org.uniprot.store.reader.publications.MappedReferenceReader;

public class ComputationalPublicationItemReader
        implements ItemReader<List<ComputationallyMappedReference>> {
    private final MappedReferenceReader<ComputationallyMappedReference> mappedReferenceReader;

    public ComputationalPublicationItemReader(String filePath) throws IOException {
        mappedReferenceReader =
                new MappedReferenceReader(new ComputationallyMappedReferenceConverter(), filePath);
    }

    @Override
    public List<ComputationallyMappedReference> read() {
        return mappedReferenceReader.readNext();
    }
}
