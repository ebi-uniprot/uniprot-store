package org.uniprot.store.indexer.help;

import org.springframework.batch.item.ItemReader;
import org.uniprot.store.search.document.help.HelpDocument;

/**
 * @author sahmad
 * @created 06/07/2021
 */
public class HelpPageItemReader implements ItemReader<HelpDocument> {
    private final String directoryPath;

    public HelpPageItemReader(String directoryPath) {
        this.directoryPath = directoryPath;
    }

    @Override
    public HelpDocument read() throws Exception {

        return null;
    }
}
