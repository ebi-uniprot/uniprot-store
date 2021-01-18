package org.uniprot.store.indexer.publication.community;

import java.io.IOException;
import java.util.List;

import org.springframework.batch.item.ItemReader;
import org.uniprot.core.publication.CommunityMappedReference;
import org.uniprot.store.reader.publications.CommunityMappedReferenceConverter;

public class CommunityPublicationItemReader implements ItemReader<List<CommunityMappedReference>> {
    private final CommunityMappedReferenceConverter converter;

    public CommunityPublicationItemReader(String filePath) throws IOException {
        converter = new CommunityMappedReferenceConverter(filePath);
    }

    @Override
    public List<CommunityMappedReference> read() {
        return converter.getNext();
    }
}
