package org.uniprot.store.spark.indexer.suggest.mapper;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

class TaxonomyAggregatorTest {
    @Test
    void aggregateTaxonomyOneOnly() throws Exception {
        TaxonomyAggregator mapper = new TaxonomyAggregator();
        String taxValue = "Value";
        String result = mapper.call(taxValue, null);
        assertNotNull(result);
        assertEquals(taxValue, result);
    }

    @Test
    void aggregateTaxonomyTwoOnly() throws Exception {
        TaxonomyAggregator mapper = new TaxonomyAggregator();
        String taxValue = "Value";
        String result = mapper.call(null, taxValue);
        assertNotNull(result);
        assertEquals(taxValue, result);
    }

    @Test
    void aggregateMerge() throws Exception {
        TaxonomyAggregator mapper = new TaxonomyAggregator();
        String taxValue = "Value";
        String result = mapper.call(taxValue, taxValue);
        assertNotNull(result);
        assertEquals(taxValue, result);
    }
}
