package uk.ac.ebi.uniprot.datastore.uniprotkb.writer;

import net.jodah.failsafe.RetryPolicy;
import org.springframework.scheduling.annotation.Async;
import uk.ac.ebi.uniprot.domain.uniprot.UniProtEntry;
import uk.ac.ebi.uniprot.domain.uniprot.builder.UniProtEntryBuilder;
import uk.ac.ebi.uniprot.flatfile.parser.ffwriter.impl.UniProtFlatfileWriter;
import uk.ac.ebi.uniprot.job.common.store.Store;
import uk.ac.ebi.uniprot.job.common.writer.ItemRetryWriter;
import uk.ebi.uniprot.scorer.uniprotkb.UniProtEntryScored;

import java.util.List;
import java.util.stream.Collectors;

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
    @Async(ITEM_WRITER_TASK_EXECUTOR)
    public void write(List<? extends UniProtEntry> items) {
        super.write(items.stream().map(this::addAnnotationScore).collect(Collectors.toList()));
    }

    // TODO: 26/07/19 why don't we set the annotation score by default when we read the entry? 
    private UniProtEntry addAnnotationScore(UniProtEntry entry) {
        UniProtEntryScored entryScored = new UniProtEntryScored(entry);
        double score = entryScored.score();
        UniProtEntryBuilder.ActiveEntryBuilder builder = new UniProtEntryBuilder().from(entry);
        builder.annotationScore(score);
        return  builder.build();
    }
}
