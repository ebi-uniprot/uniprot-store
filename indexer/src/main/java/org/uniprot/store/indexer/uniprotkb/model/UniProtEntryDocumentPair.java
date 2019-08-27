package org.uniprot.store.indexer.uniprotkb.model;

import lombok.EqualsAndHashCode;
import org.uniprot.core.uniprot.UniProtEntry;
import org.uniprot.store.job.common.model.AbstractEntryDocumentPair;
import org.uniprot.store.search.document.uniprot.UniProtDocument;

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
