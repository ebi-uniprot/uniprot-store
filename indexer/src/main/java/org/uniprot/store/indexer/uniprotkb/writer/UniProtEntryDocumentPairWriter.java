package org.uniprot.store.indexer.uniprotkb.writer;

import java.util.List;

import lombok.extern.slf4j.Slf4j;
import net.jodah.failsafe.RetryPolicy;

import org.uniprot.core.flatfile.writer.impl.UniProtFlatfileWriter;
import org.uniprot.store.indexer.common.config.UniProtSolrClient;
import org.uniprot.store.indexer.uniprotkb.model.UniProtEntryDocumentPair;
import org.uniprot.store.job.common.writer.ItemRetryWriter;
import org.uniprot.store.search.SolrCollection;
import org.uniprot.store.search.document.uniprot.UniProtDocument;

/**
 * Created 12/04/19
 *
 * @author Edd
 */
@Slf4j
public class UniProtEntryDocumentPairWriter
        extends ItemRetryWriter<UniProtEntryDocumentPair, UniProtDocument> {
    private PublicationDocumentWriter publicationWriter;

    public UniProtEntryDocumentPairWriter(
            UniProtSolrClient solrOperations,
            SolrCollection collection,
            RetryPolicy<Object> retryPolicy,
            PublicationDocumentWriter publicationWriter) {
        super(items -> solrOperations.saveBeans(collection, items), retryPolicy);
        this.publicationWriter = publicationWriter;
    }

    @Override
    public void write(List<? extends UniProtEntryDocumentPair> items) {
        super.write(items);
        try { // other writer
            this.publicationWriter.write(items);
        } catch (Exception e) {
            log.error("Error while writing publication document {}", e.getMessage());
        }
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
