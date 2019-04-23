package uk.ac.ebi.uniprot.indexer.uniprotkb.model;

import lombok.EqualsAndHashCode;
import uk.ac.ebi.uniprot.indexer.common.model.AbstractEntryDocumentPair;
import uk.ac.ebi.uniprot.indexer.uniprot.inactiveentry.InactiveUniProtEntry;
import uk.ac.ebi.uniprot.search.document.uniprot.UniProtDocument;

/**
 * Created 18/04/19
 *
 * @author Edd
 */
@EqualsAndHashCode(callSuper = true)
public class InactiveEntryDocumentPair extends AbstractEntryDocumentPair<InactiveUniProtEntry, UniProtDocument> {
    public InactiveEntryDocumentPair(InactiveUniProtEntry entry) {
        super(entry);
    }
}
