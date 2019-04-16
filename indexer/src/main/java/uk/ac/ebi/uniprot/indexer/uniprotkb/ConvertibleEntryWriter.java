package uk.ac.ebi.uniprot.indexer.uniprotkb;

import net.jodah.failsafe.Failsafe;
import net.jodah.failsafe.RetryPolicy;
import org.slf4j.Logger;
import org.springframework.batch.item.ItemWriter;
import org.springframework.data.solr.core.SolrTemplate;
import uk.ac.ebi.uniprot.flatfile.parser.ffwriter.impl.UniProtFlatfileWriter;
import uk.ac.ebi.uniprot.indexer.document.SolrCollection;
import uk.ac.ebi.uniprot.indexer.document.uniprot.UniProtDocument;

import java.util.List;
import java.util.stream.Collectors;

import static org.slf4j.LoggerFactory.getLogger;

/**
 * Created 12/04/19
 *
 * @author Edd
 */
public class ConvertibleEntryWriter implements ItemWriter<ConvertibleEntry> {
    private static final Logger INDEXING_FAILED_LOGGER = getLogger("indexing-doc-write-failed-entries");
    private final SolrTemplate solrTemplate;
    private final SolrCollection collection;
    private final RetryPolicy<Object> retryPolicy;

    public ConvertibleEntryWriter(SolrTemplate solrTemplate, SolrCollection collection, RetryPolicy<Object> retryPolicy) {
        this.solrTemplate = solrTemplate;
        this.collection = collection;
        this.retryPolicy = retryPolicy;
    }

    @Override
    public void write(List<? extends ConvertibleEntry> convertibleEntries) {
        List<UniProtDocument> uniProtDocuments = convertibleEntries.stream()
                .map(ConvertibleEntry::getDocument)
                .collect(Collectors.toList());

        try {
            Failsafe.with(retryPolicy)
                    .onFailure(throwable -> logFailedEntriesToFile(convertibleEntries))
                    .run(() -> solrTemplate.saveBeans(collection.name(), uniProtDocuments));
        } catch(Throwable error){
            System.out.println("thing");
        }
    }

    private void logFailedEntriesToFile(List<? extends ConvertibleEntry> convertibleEntries) {
        for (ConvertibleEntry convertibleEntry : convertibleEntries) {
            String entryFF = UniProtFlatfileWriter.write(convertibleEntry.getEntry());
            INDEXING_FAILED_LOGGER.error(entryFF);
        }
    }
}
