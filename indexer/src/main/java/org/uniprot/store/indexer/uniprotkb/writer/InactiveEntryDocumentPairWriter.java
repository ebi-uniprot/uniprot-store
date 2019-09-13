package org.uniprot.store.indexer.uniprotkb.writer;

import org.uniprot.store.indexer.common.config.UniProtSolrOperations;
import org.uniprot.store.indexer.uniprotkb.model.InactiveEntryDocumentPair;
import org.uniprot.store.job.common.writer.ItemRetryWriter;
import org.uniprot.store.search.SolrCollection;
import org.uniprot.store.search.document.uniprot.UniProtDocument;

import net.jodah.failsafe.RetryPolicy;

/**
 *
 * @author jluo
 * @date: 5 Sep 2019
 *
*/

public class InactiveEntryDocumentPairWriter extends ItemRetryWriter<InactiveEntryDocumentPair, UniProtDocument> {

	  public InactiveEntryDocumentPairWriter(UniProtSolrOperations solrOperations, SolrCollection collection, RetryPolicy<Object> retryPolicy) {
	        super(items -> solrOperations.saveBeans(collection.name(), items), retryPolicy);
	    }
	
	@Override
	protected String extractItemId(InactiveEntryDocumentPair item) {
		return item.getDocument().accession;
	}

	@Override
	protected String entryToString(InactiveEntryDocumentPair entry) {
		 return entry.getEntry().toString();
	}

	@Override
	public UniProtDocument itemToEntry(InactiveEntryDocumentPair item) {
		return item.getDocument();
	}

}

