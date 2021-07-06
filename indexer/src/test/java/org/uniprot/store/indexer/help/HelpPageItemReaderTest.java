package org.uniprot.store.indexer.help;

import org.junit.jupiter.api.Test;

/**
 * @author sahmad
 * @created 06/07/2021
 */
public class HelpPageItemReaderTest {

    @Test
    void testRead() throws Exception {
        int count = 0;
        HelpPageItemReader reader = new HelpPageItemReader("/Users/sahmad/proj/uniprot-manual.wiki");
        while(reader.read() != null){
            count++;
        }
        System.out.println("total number of file " + count);
    }
}
