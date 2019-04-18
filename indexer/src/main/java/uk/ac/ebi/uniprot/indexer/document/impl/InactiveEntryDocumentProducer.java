package uk.ac.ebi.uniprot.indexer.document.impl;

import uk.ac.ebi.uniprot.indexer.document.AbstractDocumentProducer;
import uk.ac.ebi.uniprot.indexer.uniprot.inactiveentry.InactiveUniProtEntry;

public class InactiveEntryDocumentProducer extends AbstractDocumentProducer<InactiveUniProtEntry> {

	    public InactiveEntryDocumentProducer(InactiveEntryConverter converter) {
	        super.configConverters(converter);
	    }
}
