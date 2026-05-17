package org.uniprot.store.spark.indexer.validator.impl;

import org.uniprot.store.search.SolrCollection;
import org.uniprot.store.spark.indexer.common.JobParameter;
import org.uniprot.store.spark.indexer.precomputed.PrecomputedAnnotationDocumentRDDReader;

public class PrecomputedAnnotationSolrIndexValidator extends AbstractSolrIndexValidator {

    public PrecomputedAnnotationSolrIndexValidator(JobParameter jobParameter) {
        super(jobParameter);
    }

    @Override
    protected long getRddCount(JobParameter jobParameter) {
        PrecomputedAnnotationDocumentRDDReader reader =
                new PrecomputedAnnotationDocumentRDDReader(jobParameter);
        return reader.load().count();
    }

    @Override
    protected SolrCollection getCollection() {
        return SolrCollection.precomputedannotation;
    }
}
