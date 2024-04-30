package org.uniprot.store.indexer.uniprotkb.writer;

import org.uniprot.core.flatfile.writer.impl.UniProtFlatfileWriter;
import org.uniprot.store.indexer.common.config.UniProtSolrClient;
import org.uniprot.store.indexer.uniprotkb.model.UniProtEntryDocumentPair;
import org.uniprot.store.job.common.writer.ItemRetryWriter;
import org.uniprot.store.search.SolrCollection;
import org.uniprot.store.search.document.uniprot.UniProtDocument;

import lombok.extern.slf4j.Slf4j;
import net.jodah.failsafe.RetryPolicy;

/**
 * Created 12/04/19
 *
 * @author Edd
 */
@Slf4j
public class UniProtEntryDocumentPairWriter
        extends ItemRetryWriter<UniProtEntryDocumentPair, UniProtDocument> {
    public UniProtEntryDocumentPairWriter(
            UniProtSolrClient solrOperations,
            SolrCollection collection,
            RetryPolicy<Object> retryPolicy) {
        super(items -> solrOperations.saveBeans(collection, items), retryPolicy);
    }

    @Override
    protected String extractItemId(UniProtEntryDocumentPair item) {
        return item.getDocument().accession;
    }

    @Override
    protected String entryToString(UniProtEntryDocumentPair entry) {
        return UniProtFlatfileWriter.write(entry.getEntry());
    }

    @Override
    public UniProtDocument itemToEntry(UniProtEntryDocumentPair item) {
        return item.getDocument();
    }
}
