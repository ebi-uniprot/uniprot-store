package uk.ac.ebi.uniprot.indexer.uniprotkb.processor;

import org.springframework.batch.item.ItemProcessor;
import uk.ac.ebi.uniprot.indexer.uniprot.inactiveentry.InactiveUniProtEntry;
import uk.ac.ebi.uniprot.indexer.uniprotkb.model.InactiveEntryDocumentPair;
import uk.ac.ebi.uniprot.search.document.uniprot.UniProtDocument;

/**
 * // TODO: 18/04/19 Need to use plug this into another step with it's own FF reader and reuse writer
 * Created 18/04/19
 *
 * @author Edd
 */
public class InactiveEntryProcessor implements ItemProcessor<InactiveEntryDocumentPair, InactiveEntryDocumentPair> {
    @Override
    public InactiveEntryDocumentPair process(InactiveEntryDocumentPair inactiveEntryDocumentPair) throws Exception {
        InactiveUniProtEntry source = inactiveEntryDocumentPair.getEntry();
        UniProtDocument document = new UniProtDocument();

        document.accession = source.getAccession();
        document.id = source.getId();
        document.inactiveReason = source.getInactiveReason();
        document.active = false;

        inactiveEntryDocumentPair.setDocument(document);
        return inactiveEntryDocumentPair;
    }
}
