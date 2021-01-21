package org.uniprot.store.indexer.publication.community;

import java.io.IOException;
import java.util.List;

import org.springframework.batch.item.ItemReader;
import org.uniprot.core.publication.CommunityMappedReference;
import org.uniprot.store.reader.publications.CommunityMappedReferenceConverter;
import org.uniprot.store.reader.publications.MappedReferenceReader;

public class CommunityPublicationItemReader implements ItemReader<List<CommunityMappedReference>> {
    private final MappedReferenceReader<CommunityMappedReference> referenceReader;

    public CommunityPublicationItemReader(String filePath) throws IOException {
        this.referenceReader =
                new MappedReferenceReader(new CommunityMappedReferenceConverter(), filePath);
    }

    @Override
    public List<CommunityMappedReference> read() {
        return referenceReader.readNext();
    }
}
