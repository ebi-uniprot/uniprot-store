package org.uniprot.store.indexer.publication.community;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.solr.client.solrj.SolrQuery;
import org.springframework.batch.item.ItemProcessor;
import org.uniprot.core.json.parser.publication.MappedPublicationsJsonConfig;
import org.uniprot.core.publication.CommunityMappedReference;
import org.uniprot.core.publication.MappedPublications;
import org.uniprot.core.publication.impl.MappedPublicationsBuilder;
import org.uniprot.store.indexer.common.config.UniProtSolrClient;
import org.uniprot.store.search.SolrCollection;
import org.uniprot.store.search.document.publication.PublicationDocument;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.uniprot.core.publication.MappedReferenceType.COMMUNITY;
import static org.uniprot.store.indexer.publication.common.PublicationUtils.computeDocumentId;

public class CommunityPublicationProcessor
        implements ItemProcessor<CommunityMappedReference, PublicationDocument> {
    private final ObjectMapper objectMapper;
    private final UniProtSolrClient uniProtSolrClient;

    public CommunityPublicationProcessor(UniProtSolrClient uniProtSolrClient) {
        this.uniProtSolrClient = uniProtSolrClient;
        this.objectMapper = MappedPublicationsJsonConfig.getInstance().getFullObjectMapper();
    }

    @Override
    public PublicationDocument process(CommunityMappedReference reference) {
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
                    .types(Collections.singleton(COMMUNITY.getIntValue()))
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

    private String docsToUpdateQuery(CommunityMappedReference reference) {
        return "accession:"
                + reference.getUniProtKBAccession().getValue()
                + " AND "
                + "pubmed_id:"
                + reference.getPubMedId();
    }

    private Set<String> getMergedCategories(
            CommunityMappedReference reference, PublicationDocument doc) {
        Set<String> categories = new HashSet<>();
        categories.addAll(doc.getCategories());
        categories.addAll(reference.getSourceCategories());
        return categories;
    }

    private Set<Integer> getMergedTypes(PublicationDocument doc) {
        Set<Integer> categories = new HashSet<>(doc.getTypes());
        categories.add(COMMUNITY.getIntValue());
        return categories;
    }

    private MappedPublications createMappedPublications(CommunityMappedReference reference) {
        return new MappedPublicationsBuilder().communityMappedReferencesAdd(reference).build();
    }

    private MappedPublications addReferenceToMappedPublications(
            PublicationDocument document, CommunityMappedReference reference) {
        try {
            MappedPublications mappedPublications =
                    this.objectMapper.readValue(
                            document.getPublicationMappedReferences(), MappedPublications.class);
            return MappedPublicationsBuilder.from(mappedPublications)
                    .communityMappedReferencesAdd(reference)
                    .build();
        } catch (IOException e) {
            throw new RuntimeException("Unable to parse MappedPublications to binary json: ", e);
        }
    }

    private byte[] asBinary(MappedPublications reference) {
        try {
            return this.objectMapper.writeValueAsBytes(reference);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Unable to parse MappedPublications to binary json: ", e);
        }
    }
}
