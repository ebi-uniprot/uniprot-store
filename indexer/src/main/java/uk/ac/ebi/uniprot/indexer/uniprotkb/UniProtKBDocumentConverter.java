package uk.ac.ebi.uniprot.indexer.uniprotkb;

import org.springframework.batch.item.ItemProcessor;
import uk.ac.ebi.uniprot.domain.uniprot.UniProtEntry;
import uk.ac.ebi.uniprot.indexer.document.uniprot.UniProtDocument;

/**
 * Created 10/04/19
 *
 * @author Edd
 */
public class UniProtKBDocumentConverter implements ItemProcessor<UniProtEntry, UniProtDocument> {
    @Override
    public UniProtDocument process(UniProtEntry uniProtEntry) throws Exception {
        // TODO: 10/04/19 refactor uniprot document converter to be in this class 
        return null;
    }
}
