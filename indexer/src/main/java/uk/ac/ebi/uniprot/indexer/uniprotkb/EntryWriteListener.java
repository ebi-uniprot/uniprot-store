package uk.ac.ebi.uniprot.indexer.uniprotkb;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.ItemWriteListener;
import uk.ac.ebi.uniprot.flatfile.parser.ffwriter.impl.UniProtFlatfileWriter;

import java.util.List;

/**
 * If there is a failure to write the document, then print the entry to a file for future reference.
 *
 * Created 12/04/19
 *
 * @author Edd
 */
public class EntryWriteListener implements ItemWriteListener<ConvertableEntry> {
    private static final Logger INDEXING_FAILED_LOGGER = LoggerFactory
            .getLogger("indexing-doc-write-failed-entries");

    @Override
    public void onWriteError(Exception exception, List<? extends ConvertableEntry> entryList) {
        for (ConvertableEntry convertableEntry : entryList) {
            String entryFF = UniProtFlatfileWriter.write(convertableEntry.getEntry());
            INDEXING_FAILED_LOGGER.error(entryFF);
        }
    }

    @Override
    public void beforeWrite(List<? extends ConvertableEntry> list) {
        // no-op
    }

    @Override
    public void afterWrite(List<? extends ConvertableEntry> list) {
        // no-op
    }
}
