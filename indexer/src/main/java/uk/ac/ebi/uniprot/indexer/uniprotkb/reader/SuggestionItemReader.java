package uk.ac.ebi.uniprot.indexer.uniprotkb.reader;

import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.annotation.BeforeStep;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemReader;
import uk.ac.ebi.uniprot.indexer.common.utils.Constants;
import uk.ac.ebi.uniprot.search.document.suggest.SuggestDocument;

import java.util.Iterator;
import java.util.Map;

import static java.util.Objects.nonNull;

/**
 * Created 10/04/19
 *
 * @author Edd
 */
public class SuggestionItemReader implements ItemReader<SuggestDocument> {
    private Iterator<SuggestDocument> suggestDocumentIterator;

    @Override
    public SuggestDocument read() {
        if (suggestDocumentIterator != null && suggestDocumentIterator.hasNext()) {
            return suggestDocumentIterator.next();
        } else {
            return null;
        }
    }

    @BeforeStep
    @SuppressWarnings("unchecked")
    public void setStepExecution(final StepExecution stepExecution) {
        ExecutionContext executionContext = stepExecution.getJobExecution().getExecutionContext();

        Map<String, SuggestDocument> suggestDocuments = (Map<String, SuggestDocument>) executionContext
                .get(Constants.SUGGESTIONS_MAP);
        if (nonNull(suggestDocuments)) {
            this.suggestDocumentIterator = suggestDocuments.values().iterator();
        }
    }
}
