package uk.ac.ebi.uniprot.indexer.uniprotkb.model;

import lombok.EqualsAndHashCode;
import uk.ac.ebi.uniprot.domain.uniprot.UniProtEntry;
import uk.ac.ebi.uniprot.indexer.common.model.AbstractEntryDocumentPair;
import uk.ac.ebi.uniprot.search.document.uniprot.UniProtDocument;

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
