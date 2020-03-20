package org.uniprot.store.datastore.uniprotkb.writer;

import java.util.List;
import java.util.stream.Collectors;

import net.jodah.failsafe.RetryPolicy;

import org.uniprot.core.flatfile.writer.impl.UniProtFlatfileWriter;
import org.uniprot.core.scorer.uniprotkb.UniProtEntryScored;
import org.uniprot.core.uniprotkb.UniProtKBEntry;
import org.uniprot.core.uniprotkb.impl.UniProtKBEntryBuilder;
import org.uniprot.store.job.common.store.Store;
import org.uniprot.store.job.common.writer.ItemRetryWriter;

/**
 * Created 25/07/19
 *
 * @author Edd
 */
public class UniProtEntryRetryWriter extends ItemRetryWriter<UniProtKBEntry, UniProtKBEntry> {
    public UniProtEntryRetryWriter(Store<UniProtKBEntry> store, RetryPolicy<Object> retryPolicy) {
        super(store, retryPolicy);
    }

    @Override
    public String extractItemId(UniProtKBEntry item) {
        return null;
    }

    @Override
    public String entryToString(UniProtKBEntry entry) {
        return UniProtFlatfileWriter.write(entry);
    }

    @Override
    public UniProtKBEntry itemToEntry(UniProtKBEntry item) {
        return item;
    }

    @Override
    public void write(List<? extends UniProtKBEntry> items) {
        super.write(items.stream().map(this::addAnnotationScore).collect(Collectors.toList()));
    }

    // TODO: 26/07/19 why don't we set the annotation score by default when we read the entry?
    private UniProtKBEntry addAnnotationScore(UniProtKBEntry entry) {
        UniProtEntryScored entryScored = new UniProtEntryScored(entry);
        double score = entryScored.score();
        UniProtKBEntryBuilder builder = UniProtKBEntryBuilder.from(entry);
        builder.annotationScore(score);
        return builder.build();
    }
}
