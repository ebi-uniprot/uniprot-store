package org.uniprot.store.spark.indexer.taxonomy.mapper;

import java.io.IOException;

import org.uniprot.core.json.parser.taxonomy.TaxonomyJsonConfig;
import org.uniprot.core.taxonomy.TaxonomyEntry;
import org.uniprot.store.spark.indexer.common.exception.IndexDataStoreException;

import com.fasterxml.jackson.databind.ObjectMapper;

public class TaxonomyUtils {

    static TaxonomyEntry getEntry(byte[] bytes) {
        try {
            ObjectMapper objectMapper = TaxonomyJsonConfig.getInstance().getFullObjectMapper();
            return objectMapper.readValue(bytes, TaxonomyEntry.class);
        } catch (IOException e) {
            throw new IndexDataStoreException("Unable to parse taxonomy", e);
        }
    }
}
