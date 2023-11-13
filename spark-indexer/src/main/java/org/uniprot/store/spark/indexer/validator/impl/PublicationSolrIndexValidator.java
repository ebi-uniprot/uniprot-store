package org.uniprot.store.spark.indexer.validator.impl;

import org.apache.spark.api.java.JavaPairRDD;
import org.uniprot.core.publication.MappedReference;
import org.uniprot.store.search.SolrCollection;
import org.uniprot.store.spark.indexer.common.JobParameter;
import org.uniprot.store.spark.indexer.publication.MappedReferenceRDDReader;
import org.uniprot.store.spark.indexer.publication.mapper.UniProtKBPublicationToMappedReference;
import org.uniprot.store.spark.indexer.uniprot.UniProtKBRDDTupleReader;

public class PublicationSolrIndexValidator extends AbstractSolrIndexValidator {

    public PublicationSolrIndexValidator(JobParameter jobParameter) {
        super(jobParameter);
    }

    @Override
    protected long getRddCount(JobParameter jobParameter) {
        MappedReferenceRDDReader mappedReferenceReader =
                new MappedReferenceRDDReader(jobParameter, MappedReferenceRDDReader.KeyType.ACCESSION_AND_CITATION_ID);

        JavaPairRDD<String, MappedReference> communityRDD = mappedReferenceReader.loadCommunityMappedReference();
        JavaPairRDD<String, MappedReference> computationalRDD = mappedReferenceReader.loadComputationalMappedReference();

        UniProtKBRDDTupleReader uniProtKBReader =
                new UniProtKBRDDTupleReader(jobParameter, false);
        JavaPairRDD<String, MappedReference> uniprotKBRDD = uniProtKBReader.loadFlatFileToRDD()
                .flatMapToPair(new UniProtKBPublicationToMappedReference());

        return communityRDD.union(computationalRDD).union(uniprotKBRDD).groupByKey().count();
    }

    @Override
    protected SolrCollection getCollection() {
        return SolrCollection.publication;
    }

    @Override
    protected String getSolrFl() {
        return "id";
    }
}
