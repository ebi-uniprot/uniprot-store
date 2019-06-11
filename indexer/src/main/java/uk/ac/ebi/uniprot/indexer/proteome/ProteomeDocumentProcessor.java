package uk.ac.ebi.uniprot.indexer.proteome;

import org.springframework.batch.item.ItemProcessor;
import uk.ac.ebi.uniprot.indexer.converter.DocumentConverter;
import uk.ac.ebi.uniprot.search.document.proteome.ProteomeDocument;
import uk.ac.ebi.uniprot.xml.jaxb.proteome.ProteomeType;

/**
 * @author jluo
 */
public class ProteomeDocumentProcessor implements ItemProcessor<ProteomeType, ProteomeDocument> {
    private final DocumentConverter<ProteomeType, ProteomeDocument> documentConverter;

    public ProteomeDocumentProcessor(DocumentConverter<ProteomeType, ProteomeDocument> documentConverter) {
        this.documentConverter = documentConverter;
    }


    @Override
    public ProteomeDocument process(ProteomeType source) throws Exception {
        return documentConverter.convert(source);
    }

}
