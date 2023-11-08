package org.uniprot.store.spark.indexer.validator.impl;

import org.apache.spark.api.java.JavaPairRDD;
import org.uniprot.core.taxonomy.TaxonomyEntry;
import org.uniprot.store.search.SolrCollection;
import org.uniprot.store.spark.indexer.common.JobParameter;
import org.uniprot.store.spark.indexer.taxonomy.reader.TaxonomyRDDReader;
import org.uniprot.store.spark.indexer.validator.AbstractSolrIndexValidator;

public class TaxonomySolrIndexValidator extends AbstractSolrIndexValidator {

    public TaxonomySolrIndexValidator(JobParameter jobParameter) {
        super(jobParameter);
    }

    @Override
    protected long getRddCount(JobParameter jobParameter) {
        TaxonomyRDDReader reader = new TaxonomyRDDReader(jobParameter, false);
        JavaPairRDD<String, TaxonomyEntry> taxonomyRDD = reader.load();
        return taxonomyRDD.count();
    }

    @Override
    protected SolrCollection getCollection() {
        return SolrCollection.taxonomy;
    }

    @Override
    protected String getSolrFl() {
        return "id";
    }
}
