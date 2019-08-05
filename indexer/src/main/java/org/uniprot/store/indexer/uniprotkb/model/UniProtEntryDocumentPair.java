package org.uniprot.store.indexer.uniprotkb.model;

import org.uniprot.core.uniprot.UniProtEntry;
import org.uniprot.store.indexer.common.model.AbstractEntryDocumentPair;
import org.uniprot.store.search.document.uniprot.UniProtDocument;

import lombok.EqualsAndHashCode;

/**
 * Created 12/04/19
 *
 * @author Edd
 */
@EqualsAndHashCode(callSuper = true)
public class UniProtEntryDocumentPair extends AbstractEntryDocumentPair<UniProtEntry, UniProtDocument> {
    public UniProtEntryDocumentPair(UniProtEntry entry) {
        super(entry);
    }
}
