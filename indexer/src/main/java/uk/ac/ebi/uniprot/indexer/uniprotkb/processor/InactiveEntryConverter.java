package uk.ac.ebi.uniprot.indexer.uniprotkb.processor;

import uk.ac.ebi.uniprot.indexer.converter.DocumentConverter;
import uk.ac.ebi.uniprot.indexer.uniprot.inactiveentry.InactiveUniProtEntry;
import uk.ac.ebi.uniprot.search.document.uniprot.UniProtDocument;

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
