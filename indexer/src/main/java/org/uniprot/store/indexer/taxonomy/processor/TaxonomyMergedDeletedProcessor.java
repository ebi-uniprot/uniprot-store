package org.uniprot.store.indexer.taxonomy.processor;

import java.nio.ByteBuffer;

import org.springframework.batch.item.ItemProcessor;
import org.uniprot.core.json.parser.taxonomy.TaxonomyJsonConfig;
import org.uniprot.core.taxonomy.TaxonomyEntry;
import org.uniprot.store.search.document.taxonomy.TaxonomyDocument;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

/** @author lgonzales */
public class TaxonomyMergedDeletedProcessor
        implements ItemProcessor<TaxonomyEntry, TaxonomyDocument> {

    private final ObjectMapper jsonMapper;

    public TaxonomyMergedDeletedProcessor() {
        jsonMapper = TaxonomyJsonConfig.getInstance().getFullObjectMapper();
    }

    @Override
    public TaxonomyDocument process(TaxonomyEntry taxonomyEntry) throws Exception {
        TaxonomyDocument.TaxonomyDocumentBuilder documentBuilder = TaxonomyDocument.builder();
        documentBuilder.id(String.valueOf(taxonomyEntry.getTaxonId()));
        documentBuilder.taxId(taxonomyEntry.getTaxonId());
        documentBuilder.active(false);
        documentBuilder.taxonomyObj(getTaxonomyBinary(taxonomyEntry));

        return documentBuilder.build();
    }

    private ByteBuffer getTaxonomyBinary(TaxonomyEntry entry) {
        try {
            return ByteBuffer.wrap(jsonMapper.writeValueAsBytes(entry));
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Unable to parse TaxonomyEntry to binary json: ", e);
        }
    }
}
