package org.uniprot.store.datastore.uniprotkb.writer;

import java.util.List;
import java.util.stream.Collectors;

import net.jodah.failsafe.RetryPolicy;

import org.uniprot.core.flatfile.writer.impl.UniProtFlatfileWriter;
import org.uniprot.core.scorer.uniprotkb.UniProtEntryScored;
import org.uniprot.core.uniprot.UniProtEntry;
import org.uniprot.core.uniprot.impl.UniProtEntryBuilder;
import org.uniprot.store.job.common.store.Store;
import org.uniprot.store.job.common.writer.ItemRetryWriter;

/**
 * Created 25/07/19
 *
 * @author Edd
 */
public class UniProtEntryRetryWriter extends ItemRetryWriter<UniProtEntry, UniProtEntry> {
    public UniProtEntryRetryWriter(Store<UniProtEntry> store, RetryPolicy<Object> retryPolicy) {
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
    public UniProtEntry itemToEntry(UniProtEntry item) {
        return item;
    }

    @Override
    public void write(List<? extends UniProtEntry> items) {
        super.write(items.stream().map(this::addAnnotationScore).collect(Collectors.toList()));
    }

    // TODO: 26/07/19 why don't we set the annotation score by default when we read the entry?
    private UniProtEntry addAnnotationScore(UniProtEntry entry) {
        UniProtEntryScored entryScored = new UniProtEntryScored(entry);
        double score = entryScored.score();
        UniProtEntryBuilder builder = UniProtEntryBuilder.from(entry);
        builder.annotationScore(score);
        return builder.build();
    }
}
