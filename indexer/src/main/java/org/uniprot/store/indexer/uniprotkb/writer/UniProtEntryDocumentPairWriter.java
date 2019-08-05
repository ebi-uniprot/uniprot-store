package org.uniprot.store.indexer.uniprotkb.writer;

import org.uniprot.core.flatfile.writer.impl.UniProtFlatfileWriter;
import org.uniprot.core.uniprot.UniProtEntry;
import org.uniprot.store.indexer.common.config.UniProtSolrOperations;
import org.uniprot.store.indexer.common.writer.EntryDocumentPairRetryWriter;
import org.uniprot.store.indexer.uniprotkb.model.UniProtEntryDocumentPair;
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
public class UniProtEntryDocumentPairWriter extends EntryDocumentPairRetryWriter<UniProtEntry, UniProtDocument, UniProtEntryDocumentPair> {
    public UniProtEntryDocumentPairWriter(UniProtSolrOperations solrOperations, SolrCollection collection, RetryPolicy<Object> retryPolicy) {
        super(solrOperations, collection, retryPolicy);
    }

    @Override
    public String extractDocumentId(UniProtDocument document) {
        return document.accession;
    }

    @Override
    public String entryToString(UniProtEntry entry) {
        return UniProtFlatfileWriter.write(entry);
    }
}
