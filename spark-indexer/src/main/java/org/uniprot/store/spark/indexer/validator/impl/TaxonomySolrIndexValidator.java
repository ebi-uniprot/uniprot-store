package org.uniprot.store.spark.indexer.validator.impl;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.uniprot.core.taxonomy.TaxonomyEntry;
import org.uniprot.store.search.SolrCollection;
import org.uniprot.store.search.document.taxonomy.TaxonomyDocument;
import org.uniprot.store.spark.indexer.common.JobParameter;
import org.uniprot.store.spark.indexer.taxonomy.reader.TaxonomyDeletedRDDReader;
import org.uniprot.store.spark.indexer.taxonomy.reader.TaxonomyMergedRDDReader;
import org.uniprot.store.spark.indexer.taxonomy.reader.TaxonomyRDDReader;

public class TaxonomySolrIndexValidator extends AbstractSolrIndexValidator {

    public TaxonomySolrIndexValidator(JobParameter jobParameter) {
        super(jobParameter);
    }

    @Override
    protected long getRddCount(JobParameter jobParameter) {
        TaxonomyRDDReader reader = new TaxonomyRDDReader(jobParameter, false);
        JavaPairRDD<String, TaxonomyEntry> taxonomyRDD = reader.load();
        TaxonomyDeletedRDDReader deletedRDDReader = new TaxonomyDeletedRDDReader(jobParameter);
        JavaRDD<TaxonomyDocument> deletedTaxonomyRDD = deletedRDDReader.load();
        TaxonomyMergedRDDReader mergedRDDReader = new TaxonomyMergedRDDReader(jobParameter);
        JavaRDD<TaxonomyDocument> mergedTaxonomyRDD = mergedRDDReader.load();
        return taxonomyRDD.count() + deletedTaxonomyRDD.count() + mergedTaxonomyRDD.count();
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
