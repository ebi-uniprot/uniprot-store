package uk.ac.ebi.uniprot.indexer.uniparc;

import org.springframework.batch.item.ItemProcessor;

import uk.ac.ebi.uniprot.indexer.converter.DocumentConverter;
import uk.ac.ebi.uniprot.search.document.uniparc.UniParcDocument;
import uk.ac.ebi.uniprot.xml.jaxb.uniparc.Entry;

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
