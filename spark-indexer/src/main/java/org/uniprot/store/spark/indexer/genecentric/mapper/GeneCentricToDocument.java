package org.uniprot.store.spark.indexer.genecentric.mapper;

import org.apache.spark.api.java.function.Function;
import org.uniprot.core.genecentric.GeneCentricEntry;
import org.uniprot.core.json.parser.genecentric.GeneCentricJsonConfig;
import org.uniprot.store.search.document.genecentric.GeneCentricDocument;
import org.uniprot.store.search.document.genecentric.GeneCentricDocumentConverter;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * @author lgonzales
 * @since 22/10/2020
 */
public class GeneCentricToDocument implements Function<GeneCentricEntry, GeneCentricDocument> {
    private static final long serialVersionUID = -3437292527678058919L;

    private final ObjectMapper objectMapper;

    public GeneCentricToDocument() {
        objectMapper = GeneCentricJsonConfig.getInstance().getFullObjectMapper();
    }

    @Override
    public GeneCentricDocument call(GeneCentricEntry geneCentricEntry) throws Exception {
        GeneCentricDocumentConverter converter = new GeneCentricDocumentConverter(objectMapper);
        return converter.convert(geneCentricEntry);
    }
}
