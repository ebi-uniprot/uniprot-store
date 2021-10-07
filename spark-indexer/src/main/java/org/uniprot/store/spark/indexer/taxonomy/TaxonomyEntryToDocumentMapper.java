package org.uniprot.store.spark.indexer.taxonomy;

import org.apache.spark.api.java.function.Function;
import org.uniprot.core.json.parser.taxonomy.TaxonomyJsonConfig;
import org.uniprot.core.taxonomy.TaxonomyEntry;
import org.uniprot.store.search.document.taxonomy.TaxonomyDocument;
import org.uniprot.store.search.document.taxonomy.TaxonomyDocumentConverter;


import com.fasterxml.jackson.databind.ObjectMapper;

public class TaxonomyEntryToDocumentMapper implements Function<TaxonomyEntry, TaxonomyDocument> {
    private static final long serialVersionUID = 1698828648779824974L;
    private final ObjectMapper objectMapper;

    public TaxonomyEntryToDocumentMapper() {
        objectMapper = TaxonomyJsonConfig.getInstance().getFullObjectMapper();
    }

    @Override
    public TaxonomyDocument call(TaxonomyEntry entry) throws Exception {
        TaxonomyDocumentConverter converter = new TaxonomyDocumentConverter(objectMapper);
        return converter.convert(entry);
    }
}
