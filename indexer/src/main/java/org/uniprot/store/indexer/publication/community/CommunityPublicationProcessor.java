package org.uniprot.store.indexer.publication.community;

import static org.uniprot.core.publication.MappedReferenceType.COMMUNITY;
import static org.uniprot.store.indexer.publication.common.PublicationUtils.*;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

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
        implements ItemProcessor<CommunityMappedReference, List<PublicationDocument>> {
    private final ObjectMapper objectMapper;
    private final UniProtSolrClient uniProtSolrClient;
    private Set<String> largeScalePubmedIds = new HashSet<>();

    public CommunityPublicationProcessor(UniProtSolrClient uniProtSolrClient) {
        this.uniProtSolrClient = uniProtSolrClient;
        this.objectMapper = MappedPublicationsJsonConfig.getInstance().getFullObjectMapper();
    }

    @Override
    public List<PublicationDocument> process(CommunityMappedReference reference) {
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
                            .isLargeScale(largeScalePubmedIds.contains(reference.getPubMedId()))
                            .categories(reference.getSourceCategories())
                            .mainType(COMMUNITY.getIntValue())
                            .types(Collections.singleton(COMMUNITY.getIntValue()))
                            .publicationMappedReferences(
                                    asBinary(createMappedPublications(reference)))
                            .build());
        } else {
            if (documents.size() > 1) {
                log.warn(
                        "More than one publications for accession {} and pubmed id {}",
                        reference.getUniProtKBAccession().getValue(),
                        reference.getPubMedId());
                log.warn(
                        "ids are {}",
                        documents.stream()
                                .map(PublicationDocument::getId)
                                .collect(Collectors.joining(",")));
            }
            for (PublicationDocument doc : documents) {
                Set<String> categories = getMergedCategories(reference, doc);
                Set<Integer> types = getMergedTypes(doc, COMMUNITY);
                toReturn.add(
                        builder.pubMedId(reference.getPubMedId())
                                .accession(reference.getUniProtKBAccession().getValue())
                                .id(doc.getId())
                                .isLargeScale(largeScalePubmedIds.contains(reference.getPubMedId()))
                                .categories(categories)
                                .types(types)
                                .mainType(doc.getMainType())
                                .publicationMappedReferences(
                                        asBinary(addReferenceToMappedPublications(doc, reference)))
                                .build());
            }
        }

        return toReturn;
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
