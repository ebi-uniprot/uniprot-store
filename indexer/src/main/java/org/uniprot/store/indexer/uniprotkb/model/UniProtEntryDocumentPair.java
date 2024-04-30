package org.uniprot.store.indexer.uniprotkb.model;

import org.uniprot.core.uniprotkb.UniProtKBEntry;
import org.uniprot.store.job.common.model.AbstractEntryDocumentPair;
import org.uniprot.store.search.document.uniprot.UniProtDocument;

import lombok.EqualsAndHashCode;

/**
 * Created 12/04/19
 *
 * @author Edd
 */
@EqualsAndHashCode(callSuper = true)
public class UniProtEntryDocumentPair
        extends AbstractEntryDocumentPair<UniProtKBEntry, UniProtDocument> {
    public UniProtEntryDocumentPair(UniProtKBEntry entry) {
        super(entry);
    }
}
