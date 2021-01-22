package org.uniprot.store.indexer.publication.community;

import static org.uniprot.core.publication.MappedReferenceType.COMMUNITY;
import static org.uniprot.store.indexer.publication.common.PublicationUtils.asBinary;
import static org.uniprot.store.indexer.publication.common.PublicationUtils.docsToUpdateQuery;
import static org.uniprot.store.indexer.publication.common.PublicationUtils.getDocumentId;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import lombok.extern.slf4j.Slf4j;

import org.apache.solr.client.solrj.SolrQuery;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.annotation.BeforeStep;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemProcessor;
import org.uniprot.core.json.parser.publication.MappedPublicationsJsonConfig;
import org.uniprot.core.publication.CommunityMappedReference;
import org.uniprot.core.publication.MappedPublications;
import org.uniprot.core.publication.impl.MappedPublicationsBuilder;
import org.uniprot.store.indexer.common.config.UniProtSolrClient;
import org.uniprot.store.indexer.common.utils.Constants;
import org.uniprot.store.search.SolrCollection;
import org.uniprot.store.search.document.DocumentConversionException;
import org.uniprot.store.search.document.publication.PublicationDocument;

import com.fasterxml.jackson.databind.ObjectMapper;

@Slf4j
public class CommunityPublicationProcessor
        implements ItemProcessor<List<CommunityMappedReference>, List<PublicationDocument>> {
    private final ObjectMapper objectMapper;
    private final UniProtSolrClient uniProtSolrClient;
    private Set<String> largeScalePubmedIds = new HashSet<>();

    public CommunityPublicationProcessor(UniProtSolrClient uniProtSolrClient) {
        this.uniProtSolrClient = uniProtSolrClient;
        this.objectMapper = MappedPublicationsJsonConfig.getInstance().getFullObjectMapper();
    }

    @Override
    public List<PublicationDocument> process(List<CommunityMappedReference> references) {
        // get the unique categories
        Set<String> categories = new HashSet<>();
        for (CommunityMappedReference reference : references) {
            categories.addAll(reference.getSourceCategories());
        }

        CommunityMappedReference reference = references.get(0);
        PublicationDocument toReturn;
        PublicationDocument.Builder builder = PublicationDocument.builder();

        List<PublicationDocument> documents =
                uniProtSolrClient.query(
                        SolrCollection.publication,
                        new SolrQuery(docsToUpdateQuery(reference)),
                        PublicationDocument.class);
        if (documents.isEmpty()) {
            toReturn =
                    builder.pubMedId(reference.getPubMedId())
                            .accession(reference.getUniProtKBAccession().getValue())
                            .id(getDocumentId())
                            .isLargeScale(largeScalePubmedIds.contains(reference.getPubMedId()))
                            .categories(categories)
                            .mainType(COMMUNITY.getIntValue())
                            .types(Collections.singleton(COMMUNITY.getIntValue()))
                            .publicationMappedReferences(
                                    asBinary(createMappedPublications(references)))
                            .build();
        } else {
            if (documents.size() > 1) {
                String message =
                        "More than one publications for accession "
                                + reference.getUniProtKBAccession().getValue()
                                + "and pubmed id "
                                + reference.getPubMedId();
                throw new RuntimeException(message);
            }
            // merge categories and types
            PublicationDocument existingDocument = documents.get(0);
            categories.addAll(existingDocument.getCategories());
            Set<Integer> types = existingDocument.getTypes();
            types.add(COMMUNITY.getIntValue());

            toReturn =
                    builder.pubMedId(reference.getPubMedId())
                            .accession(reference.getUniProtKBAccession().getValue())
                            .id(existingDocument.getId())
                            .isLargeScale(largeScalePubmedIds.contains(reference.getPubMedId()))
                            .categories(categories)
                            .types(types)
                            .mainType(existingDocument.getMainType())
                            .publicationMappedReferences(
                                    asBinary(
                                            addReferencesToMappedPublications(
                                                    existingDocument, references)))
                            .build();
        }

        return Arrays.asList(toReturn);
    }

    private MappedPublications createMappedPublications(List<CommunityMappedReference> references) {
        return new MappedPublicationsBuilder().communityMappedReferencesSet(references).build();
    }

    private MappedPublications addReferencesToMappedPublications(
            PublicationDocument document, List<CommunityMappedReference> references) {
        try {
            MappedPublications mappedPublications =
                    this.objectMapper.readValue(
                            document.getPublicationMappedReferences(), MappedPublications.class);
            return MappedPublicationsBuilder.from(mappedPublications)
                    .communityMappedReferencesSet(references)
                    .build();
        } catch (IOException e) {
            throw new DocumentConversionException(
                    "Unable to parse MappedPublications to binary json: ", e);
        }
    }

    @BeforeStep // get the cached data from previous step
    public void getStepExecution(final StepExecution stepExecution) {
        JobExecution jobExecution = stepExecution.getJobExecution();
        ExecutionContext context = jobExecution.getExecutionContext();
        if (context.containsKey(Constants.PUBLICATION_LARGE_SCALE_KEY)) {
            List<HashSet<String>> pubmedIds =
                    (List<HashSet<String>>) context.get(Constants.PUBLICATION_LARGE_SCALE_KEY);
            if (pubmedIds != null) {
                pubmedIds.stream().flatMap(Collection::stream).forEach(largeScalePubmedIds::add);
            }
        }
    }
}
