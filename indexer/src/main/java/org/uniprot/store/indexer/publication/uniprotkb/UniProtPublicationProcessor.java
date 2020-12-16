package org.uniprot.store.indexer.publication.uniprotkb;

import java.util.List;

import org.springframework.batch.item.ItemProcessor;
import org.uniprot.store.indexer.uniprotkb.converter.UniProtEntryReferencesConverter;
import org.uniprot.store.indexer.uniprotkb.model.UniProtEntryDocumentPair;
import org.uniprot.store.search.document.publication.PublicationDocument;

/**
 * @author sahmad
 * @created 16/12/2020
 */
public class UniProtPublicationProcessor
        implements ItemProcessor<UniProtEntryDocumentPair, List<PublicationDocument>> {

    private UniProtEntryReferencesConverter converter;

    public UniProtPublicationProcessor() {
        this.converter = new UniProtEntryReferencesConverter();
    }

    @Override
    public List<PublicationDocument> process(UniProtEntryDocumentPair item) throws Exception {
        return this.converter.convertToPublicationDocuments(item.getEntry());
    }
}
