package org.uniprot.store.spark.indexer.validator.impl;

import org.apache.spark.api.java.JavaRDD;
import org.uniprot.core.uniparc.UniParcEntry;
import org.uniprot.store.search.SolrCollection;
import org.uniprot.store.spark.indexer.common.JobParameter;
import org.uniprot.store.spark.indexer.uniparc.UniParcRDDTupleReader;
import org.uniprot.store.spark.indexer.validator.AbstractSolrIndexValidator;

public class UniParcSolrIndexValidator extends AbstractSolrIndexValidator {

    public UniParcSolrIndexValidator(JobParameter jobParameter) {
        super(jobParameter);
    }

    @Override
    protected long getRddCount(JobParameter jobParameter) {
        UniParcRDDTupleReader reader = new UniParcRDDTupleReader(jobParameter, false);
        JavaRDD<UniParcEntry> uniparcRDD = reader.load();
        return uniparcRDD.count();
    }

    @Override
    protected SolrCollection getCollection() {
        return SolrCollection.uniparc;
    }

    @Override
    protected String getSolrFl() {
        return "upi";
    }

}
