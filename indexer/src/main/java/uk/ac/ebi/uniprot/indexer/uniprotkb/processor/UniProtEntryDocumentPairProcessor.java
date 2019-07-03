package uk.ac.ebi.uniprot.indexer.uniprotkb.processor;

import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.annotation.BeforeStep;
import org.springframework.batch.item.ExecutionContext;
import uk.ac.ebi.uniprot.domain.uniprot.UniProtEntry;
import uk.ac.ebi.uniprot.flatfile.parser.ffwriter.impl.UniProtFlatfileWriter;
import uk.ac.ebi.uniprot.indexer.common.processor.EntryDocumentPairProcessor;
import uk.ac.ebi.uniprot.indexer.common.utils.Constants;
import uk.ac.ebi.uniprot.indexer.uniprotkb.model.UniProtEntryDocumentPair;
import uk.ac.ebi.uniprot.search.document.uniprot.UniProtDocument;

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
    // TODO: 03/07/19 no longer triggered with async
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
