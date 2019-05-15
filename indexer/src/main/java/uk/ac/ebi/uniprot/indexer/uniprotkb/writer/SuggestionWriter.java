package uk.ac.ebi.uniprot.indexer.uniprotkb.writer;

import org.springframework.batch.item.ItemWriter;
import uk.ac.ebi.uniprot.search.document.suggest.SuggestDocument;

import java.util.List;
import java.util.Set;

/**
 * Created 15/05/19
 *
 * @author Edd
 */
public class SuggestionWriter implements ItemWriter<SuggestDocument> {
    private Set<SuggestDocument> suggestDocuments;

    @Override
    public void write(List<? extends SuggestDocument> list) throws Exception {
        // TODO: 15/05/19 implement this!
        for (SuggestDocument suggestDocument : list) {
            System.out.println("writing suggestion: " + suggestDocument.id);
        }
    }
}
