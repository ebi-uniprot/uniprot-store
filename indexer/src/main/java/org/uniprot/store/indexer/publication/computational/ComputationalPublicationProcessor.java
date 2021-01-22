package org.uniprot.store.indexer.publication.computational;

import static org.uniprot.core.publication.MappedReferenceType.COMPUTATIONAL;
import static org.uniprot.store.indexer.publication.common.PublicationUtils.asBinary;
import static org.uniprot.store.indexer.publication.common.PublicationUtils.getDocumentId;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

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
        implements ItemProcessor<List<ComputationallyMappedReference>, List<PublicationDocument>> {

    private Set<String> largeScalePubmedIds = new HashSet<>();

    @Override
    public List<PublicationDocument> process(List<ComputationallyMappedReference> references) {
        List<PublicationDocument> toReturn = new ArrayList<>();
        // get the unique categories
        Set<String> categories = new HashSet<>();
        for (ComputationallyMappedReference reference : references) {
            categories.addAll(reference.getSourceCategories());
        }

        // get first element from the list to populate common values
        ComputationallyMappedReference reference = references.get(0);
        PublicationDocument.Builder builder = PublicationDocument.builder();
        toReturn.add(
                builder.pubMedId(reference.getPubMedId())
                        .accession(reference.getUniProtKBAccession().getValue())
                        .id(getDocumentId())
                        .isLargeScale(largeScalePubmedIds.contains(reference.getPubMedId()))
                        .categories(categories)
                        .types(Collections.singleton(COMPUTATIONAL.getIntValue()))
                        .mainType(COMPUTATIONAL.getIntValue())
                        .publicationMappedReferences(asBinary(createMappedPublications(references)))
                        .build());

        return toReturn;
    }

    private MappedPublications createMappedPublications(
            List<ComputationallyMappedReference> references) {
        return new MappedPublicationsBuilder().computationalMappedReferencesSet(references).build();
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
