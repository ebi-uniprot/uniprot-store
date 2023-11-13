package org.uniprot.store.spark.indexer.validator.impl;

import org.apache.spark.api.java.JavaRDD;
import org.uniprot.core.uniref.UniRefEntry;
import org.uniprot.core.uniref.UniRefType;
import org.uniprot.store.search.SolrCollection;
import org.uniprot.store.spark.indexer.common.JobParameter;
import org.uniprot.store.spark.indexer.uniref.UniRefRDDTupleReader;

public class UniRefSolrIndexValidator extends AbstractSolrIndexValidator {

    public UniRefSolrIndexValidator(JobParameter jobParameter) {
        super(jobParameter);
    }

    @Override
    protected long getRddCount(JobParameter jobParameter) {
        UniRefRDDTupleReader reader50 =
                new UniRefRDDTupleReader(UniRefType.UniRef50, jobParameter, false);
        JavaRDD<UniRefEntry> uniref50RDD = reader50.load();

        UniRefRDDTupleReader reader90 =
                new UniRefRDDTupleReader(UniRefType.UniRef90, jobParameter, false);
        JavaRDD<UniRefEntry> uniref90RDD = reader90.load();

        UniRefRDDTupleReader reader100 =
                new UniRefRDDTupleReader(UniRefType.UniRef100, jobParameter, false);
        JavaRDD<UniRefEntry> uniref100RDD = reader100.load();

        return uniref50RDD.count() + uniref90RDD.count() + uniref100RDD.count();
    }

    @Override
    protected SolrCollection getCollection() {
        return SolrCollection.uniref;
    }

    @Override
    protected String getSolrFl() {
        return "id";
    }
}
