package org.uniprot.store.indexer.uniref;

import org.springframework.batch.item.ItemProcessor;
import org.uniprot.core.xml.jaxb.uniref.Entry;
import org.uniprot.store.job.common.converter.DocumentConverter;
import org.uniprot.store.search.document.uniref.UniRefDocument;

/**
 * @author jluo
 * @date: 14 Aug 2019
 */
public class UniRefEntryProcessor implements ItemProcessor<Entry, UniRefDocument> {

    private final DocumentConverter<Entry, UniRefDocument> documentConverter;

    public UniRefEntryProcessor(DocumentConverter<Entry, UniRefDocument> documentConverter) {
        this.documentConverter = documentConverter;
    }

    @Override
    public UniRefDocument process(Entry item) throws Exception {
        return documentConverter.convert(item);
    }
}
