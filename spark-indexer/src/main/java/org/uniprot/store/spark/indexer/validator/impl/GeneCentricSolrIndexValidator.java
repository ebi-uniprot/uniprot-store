package org.uniprot.store.spark.indexer.validator.impl;

import org.apache.spark.api.java.JavaPairRDD;
import org.uniprot.core.genecentric.GeneCentricEntry;
import org.uniprot.store.search.SolrCollection;
import org.uniprot.store.spark.indexer.common.JobParameter;
import org.uniprot.store.spark.indexer.genecentric.GeneCentricCanonicalRDDReader;
import org.uniprot.store.spark.indexer.genecentric.GeneCentricRelatedRDDReader;

public class GeneCentricSolrIndexValidator extends AbstractSolrIndexValidator {

    public GeneCentricSolrIndexValidator(JobParameter jobParameter) {
        super(jobParameter);
    }

    @Override
    protected long getRddCount(JobParameter jobParameter) {
        GeneCentricCanonicalRDDReader canonicalRDDReader =
                new GeneCentricCanonicalRDDReader(jobParameter);
        JavaPairRDD<String, GeneCentricEntry> canonicalRDD = canonicalRDDReader.load();

        GeneCentricRelatedRDDReader relatedRDDReader =
                new GeneCentricRelatedRDDReader(jobParameter);
        JavaPairRDD<String, Iterable<GeneCentricEntry>> relatedRDD =
                relatedRDDReader.load().groupByKey();

        return canonicalRDD.leftOuterJoin(relatedRDD).count();
    }

    @Override
    protected SolrCollection getCollection() {
        return SolrCollection.genecentric;
    }

    @Override
    protected String getSolrFl() {
        return "accession_id";
    }
}
