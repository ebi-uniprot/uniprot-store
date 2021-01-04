package org.uniprot.store.indexer.publication.computational;

import static org.uniprot.core.publication.MappedReferenceType.COMPUTATIONAL;
import static org.uniprot.store.indexer.publication.common.PublicationUtils.*;
import static org.uniprot.store.indexer.publication.common.PublicationUtils.asBinary;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.apache.solr.client.solrj.SolrQuery;
import org.springframework.batch.item.ItemProcessor;
import org.uniprot.core.json.parser.publication.MappedPublicationsJsonConfig;
import org.uniprot.core.publication.ComputationallyMappedReference;
import org.uniprot.core.publication.MappedPublications;
import org.uniprot.core.publication.impl.MappedPublicationsBuilder;
import org.uniprot.store.indexer.common.config.UniProtSolrClient;
import org.uniprot.store.search.SolrCollection;
import org.uniprot.store.search.document.publication.PublicationDocument;

import com.fasterxml.jackson.databind.ObjectMapper;

public class ComputationalPublicationProcessor
        implements ItemProcessor<ComputationallyMappedReference, List<PublicationDocument>> {
    private final ObjectMapper objectMapper;
    private final UniProtSolrClient uniProtSolrClient;

    public ComputationalPublicationProcessor(UniProtSolrClient uniProtSolrClient) {
        this.uniProtSolrClient = uniProtSolrClient;
        this.objectMapper = MappedPublicationsJsonConfig.getInstance().getFullObjectMapper();
    }

    @Override
    public List<PublicationDocument> process(ComputationallyMappedReference reference) {
        List<PublicationDocument> toReturn = new ArrayList<>();
        PublicationDocument.PublicationDocumentBuilder builder = PublicationDocument.builder();

        List<PublicationDocument> documents =
                uniProtSolrClient.query(
                        SolrCollection.publication,
                        new SolrQuery(docsToUpdateQuery(reference)),
                        PublicationDocument.class);
        if (documents.isEmpty()) {
            toReturn.add(
                    builder.pubMedId(reference.getPubMedId())
                            .accession(reference.getUniProtKBAccession().getValue())
                            .id(getDocumentId())
                            .categories(reference.getSourceCategories())
                            .types(Collections.singleton(COMPUTATIONAL.getIntValue()))
                            .publicationMappedReferences(
                                    asBinary(createMappedPublications(reference)))
                            .build());
        } else {
            PublicationDocument doc = documents.get(0);
            doc.getCategories().addAll(reference.getSourceCategories());
            Set<String> categories = getMergedCategories(reference, doc);
            Set<Integer> types = getMergedTypes(doc);
            toReturn.add(
                    builder.pubMedId(reference.getPubMedId())
                            .accession(reference.getUniProtKBAccession().getValue())
                            .id(doc.getId())
                            .categories(categories)
                            .types(types)
                            .publicationMappedReferences(
                                    asBinary(addReferenceToMappedPublications(doc, reference)))
                            .build());
        }

        return toReturn;
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
