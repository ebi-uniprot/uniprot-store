package org.uniprot.store.indexer.uniprotkb.processor;

import org.uniprot.store.indexer.converter.DocumentConverter;
import org.uniprot.store.indexer.uniprot.inactiveentry.InactiveUniProtEntry;
import org.uniprot.store.search.document.uniprot.UniProtDocument;

/**
 * Created 18/04/19
 *
 * @author Edd
 */
public class InactiveEntryConverter implements DocumentConverter<InactiveUniProtEntry, UniProtDocument> {
    @Override
    public UniProtDocument convert(InactiveUniProtEntry source) {
        UniProtDocument document = new UniProtDocument();

        document.accession = source.getAccession();
        document.id = source.getId();
        document.inactiveReason = source.getInactiveReason();
        document.active = false;

        return document;
    }
}
