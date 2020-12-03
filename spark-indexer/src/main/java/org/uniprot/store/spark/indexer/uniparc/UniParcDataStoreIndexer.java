package org.uniprot.store.spark.indexer.uniparc;

import java.util.Iterator;
import java.util.ResourceBundle;

import lombok.extern.slf4j.Slf4j;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.VoidFunction;
import org.uniprot.core.uniparc.UniParcEntry;
import org.uniprot.store.spark.indexer.common.JobParameter;
import org.uniprot.store.spark.indexer.common.store.DataStoreIndexer;
import org.uniprot.store.spark.indexer.common.store.DataStoreParameter;

/**
 * @author lgonzales
 * @since 2020-02-26
 */
@Slf4j
public class UniParcDataStoreIndexer implements DataStoreIndexer {

    private final JobParameter parameter;

    public UniParcDataStoreIndexer(JobParameter parameter) {
        this.parameter = parameter;
    }

    @Override
    public void indexInDataStore() {
        ResourceBundle config = parameter.getApplicationConfig();
        UniParcRDDTupleReader reader = new UniParcRDDTupleReader(parameter, false);
        JavaRDD<UniParcEntry> uniparcRDD = reader.load();

        DataStoreParameter dataStoreParameter = getDataStoreParameter(config);

        uniparcRDD.foreachPartition(getWriter(dataStoreParameter));
        log.info("Completed UniParc Data Store index");
    }

    VoidFunction<Iterator<UniParcEntry>> getWriter(DataStoreParameter parameter) {
        return new UniParcDataStoreWriter(parameter);
    }

    private DataStoreParameter getDataStoreParameter(ResourceBundle config) {
        String numberOfConnections = config.getString("store.uniparc.numberOfConnections");
        String maxRetry = config.getString("store.uniparc.retry");
        String delay = config.getString("store.uniparc.delay");
        return DataStoreParameter.builder()
                .connectionURL(config.getString("store.uniparc.host"))
                .storeName(config.getString("store.uniparc.storeName"))
                .numberOfConnections(Integer.parseInt(numberOfConnections))
                .maxRetry(Integer.parseInt(maxRetry))
                .delay(Long.parseLong(delay))
                .build();
    }
}
