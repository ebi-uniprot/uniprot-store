package org.uniprot.store.indexer.publication.community;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Iterator;

import org.springframework.batch.item.ItemReader;
import org.uniprot.core.publication.CommunityMappedReference;
import org.uniprot.store.reader.publications.CommunityMappedReferenceConverter;

public class CommunityPublicationItemReader implements ItemReader<CommunityMappedReference> {
    private final Iterator<String> lines;
    private final CommunityMappedReferenceConverter converter;

    public CommunityPublicationItemReader(String filePath) throws IOException {
        lines = Files.lines(Paths.get(filePath)).iterator();
        converter = new CommunityMappedReferenceConverter();
    }

    @Override
    public CommunityMappedReference read() {
        if (lines.hasNext()) {
            return converter.convert(this.lines.next());
        }
        return null;
    }
}
