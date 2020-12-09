package org.uniprot.store.indexer.publication.community;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.batch.item.ItemProcessor;
import org.uniprot.core.publication.CommunityMappedReference;
import org.uniprot.store.search.document.publication.PublicationDocument;

import java.nio.ByteBuffer;

public class CommunityPublicationProcessor implements ItemProcessor<CommunityMappedReference, PublicationDocument> {
    private final ObjectMapper objectMapper;

    public CommunityPublicationProcessor() {
        this.objectMapper = PublicationJsonConfig.getInstance().getFullObjectMapper();
    }

    @Override
    public PublicationDocument process(CommunityMappedReference reference) {
        PublicationDocument.PublicationDocumentBuilder builder = PublicationDocument.builder();

        String accession = reference.getUniProtKBAccession().getValue();
        String pubMedId = reference.getPubMedId();
        builder.pubMedId(pubMedId)
                .accession(accession)
                .id(accession + pubMedId)
                .publicationObj(ByteBuffer.wrap(getObjectBinary(reference)));

        return builder.build();
    }

    private byte[] getObjectBinary(CommunityMappedReference reference) {
        try {
            return this.objectMapper.writeValueAsBytes(reference);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Unable to parse Literature to binary json: ", e);
        }
    }
}
