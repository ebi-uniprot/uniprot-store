package org.uniprot.store.indexer.publication.computational;

import static org.uniprot.core.publication.MappedReferenceType.COMPUTATIONAL;
import static org.uniprot.store.indexer.publication.common.PublicationUtils.asBinary;
import static org.uniprot.store.indexer.publication.common.PublicationUtils.getDocumentId;

import java.util.*;

import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.annotation.BeforeStep;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemProcessor;
import org.uniprot.core.publication.ComputationallyMappedReference;
import org.uniprot.core.publication.MappedPublications;
import org.uniprot.core.publication.impl.MappedPublicationsBuilder;
import org.uniprot.store.indexer.common.utils.Constants;
import org.uniprot.store.search.document.publication.PublicationDocument;

public class ComputationalPublicationProcessor
        implements ItemProcessor<ComputationallyMappedReference, List<PublicationDocument>> {

    private Set<String> largeScalePubmedIds = new HashSet<>();

    @Override
    public List<PublicationDocument> process(ComputationallyMappedReference reference) {
        List<PublicationDocument> toReturn = new ArrayList<>();
        PublicationDocument.PublicationDocumentBuilder builder = PublicationDocument.builder();
        toReturn.add(
                builder.pubMedId(reference.getPubMedId())
                        .accession(reference.getUniProtKBAccession().getValue())
                        .id(getDocumentId())
                        .isLargeScale(largeScalePubmedIds.contains(reference.getPubMedId()))
                        .categories(reference.getSourceCategories())
                        .types(Collections.singleton(COMPUTATIONAL.getIntValue()))
                        .mainType(COMPUTATIONAL.getIntValue())
                        .publicationMappedReferences(asBinary(createMappedPublications(reference)))
                        .build());

        return toReturn;
    }

    private MappedPublications createMappedPublications(ComputationallyMappedReference reference) {
        return new MappedPublicationsBuilder().computationalMappedReferencesAdd(reference).build();
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
