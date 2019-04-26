package uk.ac.ebi.uniprot.indexer.proteome;

import org.springframework.batch.item.ItemProcessor;

import uk.ac.ebi.uniprot.indexer.converter.DocumentConverter;
import uk.ac.ebi.uniprot.search.document.proteome.ProteomeDocument;
import uk.ac.ebi.uniprot.xml.jaxb.proteome.Proteome;
/**
 * 
 * @author jluo
 *
 */
public class ProteomeDocumentProcessor implements ItemProcessor<Proteome, ProteomeDocument>{
	private final DocumentConverter<Proteome, ProteomeDocument> documentConverter;
	
	public ProteomeDocumentProcessor(DocumentConverter<Proteome, ProteomeDocument> documentConverter) {
		this.documentConverter = documentConverter;
	}
	
	
	@Override
	public ProteomeDocument process(Proteome source) throws Exception {
		return documentConverter.convert(source);
	}

}
