package org.uniprot.store.spark.indexer.uniparc;

import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.uniprot.core.uniparc.UniParcCrossReference;
import org.uniprot.core.uniparc.UniParcEntry;
import org.uniprot.store.spark.indexer.common.JobParameter;
import org.uniprot.store.spark.indexer.common.store.DataStoreParameter;

import scala.Tuple2;

public class UniParcCrossReferenceDataStoreIndexer
        extends BaseUniParcDataStoreIndexer<UniParcCrossReference> {

    private final JobParameter parameter;

    public UniParcCrossReferenceDataStoreIndexer(JobParameter parameter) {
        super(parameter);
        this.parameter = parameter;
    }

    @Override
    public void indexInDataStore() {
        JavaRDD<UniParcEntry> uniParcRDD = getUniParcRDD();
        // now logic to extract cross-references
        // <uniParcid, cross references>
        JavaPairRDD<String, List<UniParcCrossReference>> uniParcXrefsRDD =
                uniParcRDD.mapToPair(
                        up ->
                                new Tuple2<>(
                                        up.getUniParcId().getValue(),
                                        up.getUniParcCrossReferences()));
        //
    }

    @Override
    void saveInDataStore(JavaRDD<UniParcCrossReference> uniParcXrefRDD) {
        DataStoreParameter dataStoreParameter =
                getDataStoreParameter(parameter.getApplicationConfig());
        uniParcXrefRDD.foreachPartition(
                new UniParcCrossReferenceDataStoreWriter(dataStoreParameter));
    }
}
