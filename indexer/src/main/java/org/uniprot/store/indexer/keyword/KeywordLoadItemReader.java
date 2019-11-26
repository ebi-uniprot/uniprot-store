package org.uniprot.store.indexer.keyword;

import java.io.IOException;
import java.util.Iterator;

import org.springframework.batch.item.ItemReader;
import org.uniprot.core.cv.keyword.KeywordEntry;
import org.uniprot.core.cv.keyword.KeywordFileReader;

/** @author lgonzales */
public class KeywordLoadItemReader implements ItemReader<KeywordEntry> {
    private Iterator<KeywordEntry> keywordIterator;

    public KeywordLoadItemReader(String filePath) throws IOException {
        KeywordFileReader keywordFileReader = new KeywordFileReader();
        this.keywordIterator = keywordFileReader.parse(filePath).iterator();
    }

    @Override
    public KeywordEntry read() {
        if (this.keywordIterator.hasNext()) {
            return this.keywordIterator.next();
        }
        return null;
    }
}
