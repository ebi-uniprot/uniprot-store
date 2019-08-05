package org.uniprot.store.indexer.proteome;

import org.springframework.batch.item.ItemProcessor;
import org.uniprot.core.xml.jaxb.proteome.Proteome;
import org.uniprot.store.indexer.converter.DocumentConverter;
import org.uniprot.store.search.document.proteome.ProteomeDocument;

/**
 * @author jluo
 */
public class ProteomeDocumentProcessor implements ItemProcessor<Proteome, ProteomeDocument> {
    private final DocumentConverter<Proteome, ProteomeDocument> documentConverter;

    public ProteomeDocumentProcessor(DocumentConverter<Proteome, ProteomeDocument> documentConverter) {
        this.documentConverter = documentConverter;
    }


    @Override
    public ProteomeDocument process(Proteome source) throws Exception {
        return documentConverter.convert(source);
    }

}
