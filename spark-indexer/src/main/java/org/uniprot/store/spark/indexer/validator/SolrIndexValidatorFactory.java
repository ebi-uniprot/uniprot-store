package org.uniprot.store.spark.indexer.validator;

import org.uniprot.store.search.SolrCollection;
import org.uniprot.store.spark.indexer.common.JobParameter;
import org.uniprot.store.spark.indexer.validator.impl.*;

public class SolrIndexValidatorFactory {

    public SolrIndexValidator createSolrIndexValidator(
            SolrCollection collection, JobParameter jobParameter) {
        SolrIndexValidator validator = null;
        switch (collection) {
            case genecentric:
                validator = new GeneCentricSolrIndexValidator(jobParameter);
                break;
            case literature:
                validator = new LiteratureSolrIndexValidator(jobParameter);
                break;
            case proteome:
                validator = new ProteomeSolrIndexValidator(jobParameter);
                break;
            case publication:
                validator = new PublicationSolrIndexValidator(jobParameter);
                break;
            case subcellularlocation:
                validator = new SubcellularLocationSolrIndexValidator(jobParameter);
                break;
            case taxonomy:
                validator = new TaxonomySolrIndexValidator(jobParameter);
                break;
            case uniparc:
                validator = new UniParcSolrIndexValidator(jobParameter);
                break;
            case uniprot:
                validator = new UniProtKBSolrIndexValidator(jobParameter);
                break;
            case uniref:
                validator = new UniRefSolrIndexValidator(jobParameter);
                break;
            default:
                throw new UnsupportedOperationException(
                        "Collection not yet supported by solr index validator: "
                                + collection.name());
        }
        return validator;
    }
}
