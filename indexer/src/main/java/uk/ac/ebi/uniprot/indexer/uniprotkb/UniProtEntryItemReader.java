package uk.ac.ebi.uniprot.indexer.uniprotkb;

import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.NonTransientResourceException;
import org.springframework.batch.item.ParseException;
import org.springframework.batch.item.UnexpectedInputException;
import uk.ac.ebi.uniprot.domain.uniprot.UniProtEntry;

/**
 * Created 10/04/19
 *
 * @author Edd
 */
public class UniProtEntryItemReader implements ItemReader<UniProtEntry> {
    @Override
    public UniProtEntry read() throws Exception, UnexpectedInputException, ParseException, NonTransientResourceException {
        return null;
    }
}
