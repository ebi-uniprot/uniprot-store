package org.uniprot.store.indexer.publication.uniprotkb;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.annotation.BeforeStep;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemProcessor;
import org.uniprot.store.indexer.common.utils.Constants;
import org.uniprot.store.indexer.uniprotkb.converter.UniProtEntryReferencesConverter;
import org.uniprot.store.indexer.uniprotkb.model.UniProtEntryDocumentPair;
import org.uniprot.store.search.document.publication.PublicationDocument;

/**
 * @author sahmad
 * @created 16/12/2020
 */
public class UniProtPublicationProcessor
        implements ItemProcessor<UniProtEntryDocumentPair, List<PublicationDocument>> {

    private UniProtEntryReferencesConverter converter;
    private Set<String> largeScalePubmedIds = new HashSet<>();

    public UniProtPublicationProcessor() {
        this.converter = new UniProtEntryReferencesConverter();
    }

    @Override
    public List<PublicationDocument> process(UniProtEntryDocumentPair item) throws Exception {
        return this.converter.convertToPublicationDocuments(item.getEntry()).stream()
                .map(this::setLargeScale)
                .collect(Collectors.toList());
    }

    private PublicationDocument setLargeScale(PublicationDocument publicationDocument) {
        PublicationDocument.Builder builder = publicationDocument.toBuilder();
        builder.isLargeScale(largeScalePubmedIds.contains(publicationDocument.getPubMedId()));
        return builder.build();
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
