package org.uniprot.store.indexer.publication.community;

import static org.uniprot.core.publication.MappedReferenceType.COMMUNITY;
import static org.uniprot.store.indexer.publication.common.PublicationUtils.computeDocumentId;

import org.springframework.batch.item.ItemProcessor;
import org.uniprot.core.json.parser.publication.CommunityMappedReferenceJsonConfig;
import org.uniprot.core.publication.CommunityMappedReference;
import org.uniprot.store.search.document.publication.PublicationDocument;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class CommunityPublicationProcessor
        implements ItemProcessor<CommunityMappedReference, PublicationDocument> {
    private final ObjectMapper objectMapper;

    public CommunityPublicationProcessor() {
        this.objectMapper = CommunityMappedReferenceJsonConfig.getInstance().getFullObjectMapper();
    }

    @Override
    public PublicationDocument process(CommunityMappedReference reference) {
        PublicationDocument.PublicationDocumentBuilder builder = PublicationDocument.builder();

        builder.pubMedId(reference.getPubMedId())
                .accession(reference.getUniProtKBAccession().getValue())
                .id(computeDocumentId(reference))
                .type(COMMUNITY.getIntValue())
                .publicationMappedReference(getObjectBinary(reference));

        return builder.build();
    }

    private byte[] getObjectBinary(CommunityMappedReference reference) {
        try {
            return this.objectMapper.writeValueAsBytes(reference);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(
                    "Unable to parse CommunityMappedReference to binary json: ", e);
        }
    }
}
