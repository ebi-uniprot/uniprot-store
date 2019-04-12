package uk.ac.ebi.uniprot.indexer.uniprotkb;

import org.springframework.batch.item.ItemReader;
import uk.ac.ebi.uniprot.flatfile.parser.impl.DefaultUniProtEntryIterator;

/**
 * Created 10/04/19
 *
 * @author Edd
 */
public class UniProtEntryItemReader implements ItemReader<ConvertableEntry> {
    private final DefaultUniProtEntryIterator entryIterator;
    public UniProtEntryItemReader(UniProtKBIndexingProperties indexingProperties) {
        DefaultUniProtEntryIterator uniProtEntryIterator =
                new DefaultUniProtEntryIterator(indexingProperties.getEntryIteratorThreads(),
                                                indexingProperties.getEntryIteratorQueueSize(),
                                                indexingProperties.getEntryIteratorFFQueueSize());
        uniProtEntryIterator.setInput(indexingProperties.getUniProtEntryFile(),
                                      indexingProperties.getKeywordFile(),
                                      indexingProperties.getDiseaseFile(),
                                      indexingProperties.getAccessionGoPubmedFile(),
                                      indexingProperties.getSubcellularLocationFile());
        this.entryIterator = uniProtEntryIterator;
    }

    @Override
    public ConvertableEntry read() {
        if (entryIterator.hasNext()) {
            return ConvertableEntry.createConvertableEntry(entryIterator.next());
        } else {
            return null;
        }
    }
}
