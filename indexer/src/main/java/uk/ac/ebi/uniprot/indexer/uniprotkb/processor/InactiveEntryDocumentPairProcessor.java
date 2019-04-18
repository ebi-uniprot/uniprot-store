package uk.ac.ebi.uniprot.indexer.uniprotkb.processor;

import uk.ac.ebi.uniprot.indexer.common.processor.EntryDocumentPairProcessor;
import uk.ac.ebi.uniprot.indexer.converter.DocumentConverter;
import uk.ac.ebi.uniprot.indexer.uniprot.inactiveentry.InactiveUniProtEntry;
import uk.ac.ebi.uniprot.indexer.uniprotkb.model.InactiveEntryDocumentPair;
import uk.ac.ebi.uniprot.search.document.uniprot.UniProtDocument;

/**
 * // TODO: 18/04/19 Need to use plug this into another step with it's own FF reader and reuse writer
 * Created 18/04/19
 *
 * @author Edd
 */
public class InactiveEntryDocumentPairProcessor extends EntryDocumentPairProcessor<InactiveUniProtEntry, UniProtDocument, InactiveEntryDocumentPair> {
    public InactiveEntryDocumentPairProcessor(DocumentConverter<InactiveUniProtEntry, UniProtDocument> converter) {
        super(converter);
    }

    @Override
    public String extractEntryId(InactiveUniProtEntry entry) {
        return entry.getAccession();
    }

    @Override
    public String entryToString(InactiveUniProtEntry entry) {
        // TODO: 18/04/19 need to use InactiveUniProtEntryWriter ... but I don't know if we have one?!
        return entry.getAccession();
    }
}
