package org.uniprot.store.spark.indexer.validator.impl;

import org.apache.spark.api.java.JavaPairRDD;
import org.uniprot.core.cv.subcell.SubcellularLocationEntry;
import org.uniprot.store.search.SolrCollection;
import org.uniprot.store.spark.indexer.common.JobParameter;
import org.uniprot.store.spark.indexer.subcell.SubcellularLocationRDDReader;

public class SubcellularLocationSolrIndexValidator extends AbstractSolrIndexValidator {

    public SubcellularLocationSolrIndexValidator(JobParameter jobParameter) {
        super(jobParameter);
    }

    @Override
    protected long getRddCount(JobParameter jobParameter) {
        SubcellularLocationRDDReader reader = new SubcellularLocationRDDReader(jobParameter);
        JavaPairRDD<String, SubcellularLocationEntry> subcellRDD = reader.load();
        return subcellRDD.count();
    }

    @Override
    protected SolrCollection getCollection() {
        return SolrCollection.subcellularlocation;
    }
}
