package org.uniprot.store.spark.indexer.literature.mapper;

import org.apache.spark.api.java.function.Function;
import org.uniprot.core.literature.LiteratureEntry;
import org.uniprot.store.converter.LiteratureDocumentConverter;
import org.uniprot.store.search.document.literature.LiteratureDocument;

/**
 * @author lgonzales
 * @since 25/03/2021
 */
public class LiteratureEntryToDocumentMapper
        implements Function<LiteratureEntry, LiteratureDocument> {
    private static final long serialVersionUID = 5401905039496811550L;

    @Override
    public LiteratureDocument call(LiteratureEntry entry) throws Exception {
        LiteratureDocumentConverter documentConverter = new LiteratureDocumentConverter();
        return documentConverter.convert(entry);
    }
}
