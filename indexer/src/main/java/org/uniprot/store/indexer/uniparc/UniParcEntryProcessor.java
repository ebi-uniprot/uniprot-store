package org.uniprot.store.indexer.uniparc;

import org.springframework.batch.item.ItemProcessor;
import org.uniprot.core.xml.jaxb.uniparc.Entry;
import org.uniprot.store.job.common.converter.DocumentConverter;
import org.uniprot.store.search.document.uniparc.UniParcDocument;

/**
 *
 * @author jluo
 * @date: 18 Jun 2019
 *
 */

public class UniParcEntryProcessor implements ItemProcessor<Entry, UniParcDocument> {
	private final DocumentConverter<Entry, UniParcDocument> documentConverter;
	public UniParcEntryProcessor(DocumentConverter<Entry, UniParcDocument> documentConverter) {
		this.documentConverter =documentConverter;
	}

	@Override
	public UniParcDocument process(Entry item) throws Exception {
		return documentConverter.convert(item);
	}

}
