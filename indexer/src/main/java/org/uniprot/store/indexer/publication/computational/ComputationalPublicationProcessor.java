package org.uniprot.store.indexer.publication.computational;

import static org.uniprot.core.publication.MappedReferenceType.COMMUNITY;
import static org.uniprot.core.publication.MappedReferenceType.COMPUTATIONAL;
import static org.uniprot.store.indexer.publication.common.PublicationUtils.*;
import static org.uniprot.store.indexer.publication.common.PublicationUtils.asBinary;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.apache.solr.client.solrj.SolrQuery;
import org.springframework.batch.item.ItemProcessor;
import org.uniprot.core.json.parser.publication.CommunityMappedReferenceJsonConfig;
import org.uniprot.core.publication.CommunityMappedReference;
import org.uniprot.core.publication.ComputationallyMappedReference;
import org.uniprot.core.publication.MappedPublications;
import org.uniprot.core.publication.impl.MappedPublicationsBuilder;
import org.uniprot.store.indexer.common.config.UniProtSolrClient;
import org.uniprot.store.search.SolrCollection;
import org.uniprot.store.search.document.publication.PublicationDocument;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class ComputationalPublicationProcessor
        implements ItemProcessor<ComputationallyMappedReference, PublicationDocument> {
    static final String ID_COMPONENT_SEPARATOR = "__";
    private final ObjectMapper objectMapper;
    private final UniProtSolrClient uniProtSolrClient;

    public ComputationalPublicationProcessor(UniProtSolrClient uniProtSolrClient) {
        this.uniProtSolrClient = uniProtSolrClient;
        this.objectMapper = CommunityMappedReferenceJsonConfig.getInstance().getFullObjectMapper();
    }

    @Override
    public PublicationDocument process(ComputationallyMappedReference reference) {
        PublicationDocument.PublicationDocumentBuilder builder = PublicationDocument.builder();

        String docId = computeDocumentId(reference);

        List<PublicationDocument> documents =
                uniProtSolrClient.query(
                        SolrCollection.publication,
                        new SolrQuery(docsToUpdateQuery(reference)),
                        PublicationDocument.class);
        if (documents.isEmpty()) {
            return builder.pubMedId(reference.getPubMedId())
                    .accession(reference.getUniProtKBAccession().getValue())
                    .id(docId)
                    .categories(reference.getSourceCategories())
                    .types(Collections.singleton(COMPUTATIONAL.getIntValue()))
                    .publicationMappedReferences(asBinary(createMappedPublications(reference)))
                    .build();
        } else if (documents.size() == 1) {
            PublicationDocument doc = documents.get(0);
            doc.getCategories().addAll(reference.getSourceCategories());
            Set<String> categories = getMergedCategories(reference, doc);
            Set<Integer> types = getMergedTypes(doc);
            builder.pubMedId(doc.getPubMedId())
                    .accession(doc.getAccession())
                    .id(docId)
                    .categories(categories)
                    .types(types)
                    .publicationMappedReferences(
                            asBinary(addReferenceToMappedPublications(doc, reference)));
        }

        return builder.build();
    }

    private MappedPublications createMappedPublications(ComputationallyMappedReference reference) {
        return new MappedPublicationsBuilder().computationalMappedReferencesAdd(reference).build();
    }

    private MappedPublications addReferenceToMappedPublications(
            PublicationDocument document, ComputationallyMappedReference reference) {
        try {
            MappedPublications mappedPublications =
                    this.objectMapper.readValue(
                            document.getPublicationMappedReferences(), MappedPublications.class);
            return MappedPublicationsBuilder.from(mappedPublications)
                    .computationalMappedReferencesAdd(reference)
                    .build();
        } catch (IOException e) {
            throw new RuntimeException("Unable to parse MappedPublications to binary json: ", e);
        }
    }
}
