package uk.ac.ebi.uniprot.indexer.document.impl;



import java.util.ArrayList;
import java.util.List;

import uk.ac.ebi.uniprot.indexer.document.AbstractDocumentConverter;
import uk.ac.ebi.uniprot.indexer.uniprot.inactiveentry.InactiveUniProtEntry;
import uk.ac.ebi.uniprot.search.document.uniprot.UniProtDocument;


public class InactiveEntryConverter extends AbstractDocumentConverter<InactiveUniProtEntry, UniProtDocument> {

	@Override
	public List<UniProtDocument> convert(InactiveUniProtEntry source) {
		List<UniProtDocument> solrDocuments = new ArrayList<>();
		UniProtDocument solrDocument = new UniProtDocument();

		solrDocument.accession = source.getAccession();
		solrDocument.id = source.getId();
		solrDocument.inactiveReason=source.getInactiveReason();
		solrDocument.active = false;
		solrDocuments.add(solrDocument);
		return solrDocuments;
	}

}
