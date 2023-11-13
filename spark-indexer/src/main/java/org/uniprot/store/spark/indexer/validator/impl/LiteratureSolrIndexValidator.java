package org.uniprot.store.spark.indexer.validator.impl;

import org.apache.spark.api.java.JavaPairRDD;
import org.uniprot.core.citation.Literature;
import org.uniprot.core.literature.LiteratureEntry;
import org.uniprot.store.search.SolrCollection;
import org.uniprot.store.spark.indexer.common.JobParameter;
import org.uniprot.store.spark.indexer.literature.LiteratureRDDTupleReader;
import org.uniprot.store.spark.indexer.literature.LiteratureUniProtKBRDDReader;

public class LiteratureSolrIndexValidator extends AbstractSolrIndexValidator {

    public LiteratureSolrIndexValidator(JobParameter jobParameter) {
        super(jobParameter);
    }

    @Override
    protected long getRddCount(JobParameter jobParameter) {
        LiteratureRDDTupleReader reader = new LiteratureRDDTupleReader(jobParameter);
        JavaPairRDD<String, Literature> literatureRDD = reader.load();
        LiteratureUniProtKBRDDReader literatureUniProtKBRDDReader = new LiteratureUniProtKBRDDReader(jobParameter);
        JavaPairRDD<String, LiteratureEntry> literatureUniProtKBRDD = literatureUniProtKBRDDReader.load();
        return literatureRDD.fullOuterJoin(literatureUniProtKBRDD).count();
    }

    @Override
    protected SolrCollection getCollection() {
        return SolrCollection.literature;
    }

    @Override
    protected String getSolrFl() {
        return "id";
    }
}
