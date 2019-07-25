package uk.ac.ebi.uniprot.datastore.uniprotkb.writer;

import net.jodah.failsafe.RetryPolicy;
import uk.ac.ebi.uniprot.datastore.Store;
import uk.ac.ebi.uniprot.datastore.writer.ItemRetryWriter;
import uk.ac.ebi.uniprot.domain.uniprot.UniProtEntry;
import uk.ac.ebi.uniprot.flatfile.parser.ffwriter.impl.UniProtFlatfileWriter;

/**
 * Created 25/07/19
 *
 * @author Edd
 */
public class UniProtEntryRetryWriter extends ItemRetryWriter<UniProtEntry> {
    public UniProtEntryRetryWriter(Store store, RetryPolicy<Object> retryPolicy) {
        super(store, retryPolicy);
    }

    @Override
    public String extractItemId(UniProtEntry item) {
        return null;
    }

    @Override
    public String entryToString(UniProtEntry entry) {
        return UniProtFlatfileWriter.write(entry);
    }

    @Override
    public Object itemToEntry(UniProtEntry item) {
        return item;
    }
}
