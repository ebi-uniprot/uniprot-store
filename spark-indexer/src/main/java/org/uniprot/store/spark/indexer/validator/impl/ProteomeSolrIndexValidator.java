package org.uniprot.store.spark.indexer.validator.impl;

import org.apache.spark.api.java.JavaPairRDD;
import org.uniprot.core.proteome.ProteomeEntry;
import org.uniprot.store.search.SolrCollection;
import org.uniprot.store.spark.indexer.common.JobParameter;
import org.uniprot.store.spark.indexer.proteome.ProteomeRDDReader;

public class ProteomeSolrIndexValidator extends AbstractSolrIndexValidator {

    public ProteomeSolrIndexValidator(JobParameter jobParameter) {
        super(jobParameter);
    }

    @Override
    protected long getRddCount(JobParameter jobParameter) {
        ProteomeRDDReader reader = new ProteomeRDDReader(jobParameter, false);
        JavaPairRDD<String, ProteomeEntry> proteomeRdd = reader.load();
        return proteomeRdd.count();
    }

    @Override
    protected SolrCollection getCollection() {
        return SolrCollection.proteome;
    }
}
