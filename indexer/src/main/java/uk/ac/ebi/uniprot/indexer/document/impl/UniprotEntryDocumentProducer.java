package uk.ac.ebi.uniprot.indexer.document.impl;


import uk.ac.ebi.uniprot.domain.uniprot.UniProtEntry;
import uk.ac.ebi.uniprot.indexer.document.AbstractDocumentProducer;

/**
 * Produces a collection of {@link uk.ac.ebi.uniprot.dataservice.document.Document} from a given {@link UniProtEntry}
 */

public class UniprotEntryDocumentProducer extends AbstractDocumentProducer<UniProtEntry> {

    public UniprotEntryDocumentProducer(UniprotEntryConverter converter){
        super.configConverters(converter);
    }
}
