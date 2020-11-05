package org.uniprot.store.indexer.genecentric;

import org.springframework.batch.item.ItemProcessor;
import org.uniprot.core.genecentric.GeneCentricEntry;
import org.uniprot.store.search.document.proteome.GeneCentricDocument;

/**
 * @author lgonzales
 * @since 02/11/2020
 */
public class GeneCentricCanonicalProcessor
        implements ItemProcessor<GeneCentricEntry, GeneCentricDocument> {

    private final GeneCentricDocumentConverter converter;

    public GeneCentricCanonicalProcessor(
            GeneCentricDocumentConverter geneCentricDocumentConverter) {
        converter = geneCentricDocumentConverter;
    }

    @Override
    public GeneCentricDocument process(GeneCentricEntry geneCentricEntry) throws Exception {
        return converter.convert(geneCentricEntry);
    }
}
