package org.uniprot.store.indexer.uniprotkb.writer;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import lombok.extern.slf4j.Slf4j;
import net.jodah.failsafe.RetryPolicy;

import org.uniprot.store.indexer.common.config.UniProtSolrClient;
import org.uniprot.store.indexer.uniprotkb.converter.UniProtEntryReferencesConverter;
import org.uniprot.store.indexer.uniprotkb.model.UniProtEntryDocumentPair;
import org.uniprot.store.job.common.writer.ItemRetryWriter;
import org.uniprot.store.search.SolrCollection;
import org.uniprot.store.search.document.publication.PublicationDocument;

/**
 * Created 11/12/20
 *
 * @author sahmad
 */
@Slf4j
public class PublicationDocumentWriter
        extends ItemRetryWriter<UniProtEntryDocumentPair, List<PublicationDocument>> {
    private UniProtEntryReferencesConverter converter;
    private AtomicInteger failedWritingEntriesCount;
    private AtomicInteger writtenEntriesCount;
    private UniProtSolrClient solrOperations;

    public PublicationDocumentWriter(
            UniProtSolrClient solrOperations,
            SolrCollection collection,
            RetryPolicy<Object> retryPolicy) {
        super(
                items ->
                        solrOperations.saveBeans(
                                collection,
                                items.stream()
                                        .flatMap(Collection::stream)
                                        .collect(Collectors.toList())),
                retryPolicy);
        this.solrOperations = solrOperations;
        this.converter = new UniProtEntryReferencesConverter();
        this.failedWritingEntriesCount = new AtomicInteger(0);
        this.writtenEntriesCount = new AtomicInteger(0);
    }

    @Override
    protected String extractItemId(UniProtEntryDocumentPair item) {
        return item.getDocument().accession;
    }

    @Override
    protected String entryToString(UniProtEntryDocumentPair entry) {
        return entry.getDocument().accession;
    }

    @Override
    public List<PublicationDocument> itemToEntry(UniProtEntryDocumentPair item) {
        return this.converter.convertToPublicationDocuments(item.getEntry());
    }

    @Override
    protected AtomicInteger getWrittenEntriesCount() {
        return this.writtenEntriesCount;
    }

    @Override
    protected AtomicInteger getFailedWritingEntriesCount() {
        return this.failedWritingEntriesCount;
    }
}
