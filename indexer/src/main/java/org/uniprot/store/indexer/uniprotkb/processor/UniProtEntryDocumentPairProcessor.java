package org.uniprot.store.indexer.uniprotkb.processor;

import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.annotation.BeforeStep;
import org.springframework.batch.item.ExecutionContext;
import org.uniprot.core.flatfile.writer.impl.UniProtFlatfileWriter;
import org.uniprot.core.uniprot.UniProtEntry;
import org.uniprot.store.indexer.common.processor.EntryDocumentPairProcessor;
import org.uniprot.store.indexer.common.utils.Constants;
import org.uniprot.store.indexer.uniprotkb.model.UniProtEntryDocumentPair;
import org.uniprot.store.search.document.uniprot.UniProtDocument;

/**
 * Created 10/04/19
 *
 * @author Edd
 */
@Slf4j
public class UniProtEntryDocumentPairProcessor extends EntryDocumentPairProcessor<UniProtEntry, UniProtDocument, UniProtEntryDocumentPair> {
    private final UniProtEntryConverter converter;

    public UniProtEntryDocumentPairProcessor(UniProtEntryConverter converter) {
        super(converter);
        this.converter = converter;
    }

    @BeforeStep
    public void setStepExecution(final StepExecution stepExecution) {
        ExecutionContext executionContext = stepExecution.getJobExecution().getExecutionContext();
        executionContext.put(Constants.SUGGESTIONS_MAP, converter.getSuggestions());
    }

    @Override
    public String extractEntryId(UniProtEntry entry) {
        return entry.getPrimaryAccession().getValue();
    }

    @Override
    public String entryToString(UniProtEntry entry) {
        return UniProtFlatfileWriter.write(entry);
    }
}
