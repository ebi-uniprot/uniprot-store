package org.uniprot.store.indexer.uniparc;

import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.annotation.BeforeStep;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemProcessor;
import org.uniprot.core.xml.jaxb.uniparc.Entry;
import org.uniprot.store.indexer.common.utils.Constants;
import org.uniprot.store.search.document.uniparc.UniParcDocument;

/**
 * @author jluo
 * @date: 18 Jun 2019
 */
public class UniParcEntryProcessor implements ItemProcessor<Entry, UniParcDocument> {
    private final UniParcDocumentConverter documentConverter;

    public UniParcEntryProcessor(UniParcDocumentConverter documentConverter) {
        this.documentConverter = documentConverter;
    }

    @Override
    public UniParcDocument process(Entry item) throws Exception {
        return documentConverter.convert(item);
    }

    @BeforeStep
    public void setStepExecution(final StepExecution stepExecution) {
        ExecutionContext executionContext = stepExecution.getJobExecution().getExecutionContext();
        executionContext.put(Constants.SUGGESTIONS_MAP, this.documentConverter.getSuggestions());
    }
}
