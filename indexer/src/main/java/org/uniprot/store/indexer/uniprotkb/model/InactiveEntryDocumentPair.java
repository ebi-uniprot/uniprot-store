package org.uniprot.store.indexer.uniprotkb.model;

import org.uniprot.store.indexer.uniprot.inactiveentry.InactiveUniProtEntry;
import org.uniprot.store.job.common.model.AbstractEntryDocumentPair;
import org.uniprot.store.search.document.uniprot.UniProtDocument;

import lombok.EqualsAndHashCode;

/**
 * Created 18/04/19
 *
 * @author Edd
 */
@EqualsAndHashCode(callSuper = true)
public class InactiveEntryDocumentPair
        extends AbstractEntryDocumentPair<InactiveUniProtEntry, UniProtDocument> {
    public InactiveEntryDocumentPair(InactiveUniProtEntry entry) {
        super(entry);
    }
}
