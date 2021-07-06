package org.uniprot.store.indexer.help;

import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.NonTransientResourceException;
import org.springframework.batch.item.ParseException;
import org.springframework.batch.item.UnexpectedInputException;
import org.uniprot.store.search.document.help.HelpDocument;

/**
 * @author sahmad
 * @created 06/07/2021
 */
public class HelpPageItemReader implements ItemReader<HelpDocument> {

    @Override
    public HelpDocument read() throws Exception, UnexpectedInputException, ParseException, NonTransientResourceException {
        return null;
    }
}
