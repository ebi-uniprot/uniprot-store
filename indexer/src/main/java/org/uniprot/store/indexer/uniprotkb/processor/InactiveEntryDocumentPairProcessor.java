package org.uniprot.store.indexer.uniprotkb.processor;

import org.uniprot.store.indexer.uniprot.inactiveentry.InactiveUniProtEntry;
import org.uniprot.store.indexer.uniprotkb.model.InactiveEntryDocumentPair;
import org.uniprot.store.job.common.processor.EntryDocumentPairProcessor;
import org.uniprot.store.search.document.DocumentConverter;
import org.uniprot.store.search.document.uniprot.UniProtDocument;

/**
 * // TODO: 18/04/19 Need to use plug this into another step with it's own FF reader and reuse
 * writer Created 18/04/19
 *
 * @author Edd
 */
public class InactiveEntryDocumentPairProcessor
        extends EntryDocumentPairProcessor<
                InactiveUniProtEntry, UniProtDocument, InactiveEntryDocumentPair> {
    public InactiveEntryDocumentPairProcessor(
            DocumentConverter<InactiveUniProtEntry, UniProtDocument> converter) {
        super(converter);
    }

    @Override
    public String extractEntryId(InactiveUniProtEntry entry) {
        return entry.getAccession();
    }

    @Override
    public String entryToString(InactiveUniProtEntry entry) {
        // TODO: 18/04/19 need to use InactiveUniProtEntryWriter ... but I don't know if we have
        // one?!
        return entry.getAccession();
    }
}
