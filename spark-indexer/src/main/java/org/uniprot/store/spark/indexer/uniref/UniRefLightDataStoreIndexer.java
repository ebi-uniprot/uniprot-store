package org.uniprot.store.spark.indexer.uniref;

import java.util.Iterator;

import lombok.extern.slf4j.Slf4j;

import org.apache.spark.api.java.JavaFutureAction;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.VoidFunction;
import org.uniprot.core.uniref.UniRefEntryLight;
import org.uniprot.core.uniref.UniRefType;
import org.uniprot.store.spark.indexer.common.JobParameter;
import org.uniprot.store.spark.indexer.common.exception.IndexDataStoreException;
import org.uniprot.store.spark.indexer.common.store.DataStoreIndexer;
import org.uniprot.store.spark.indexer.common.store.DataStoreParameter;
import org.uniprot.store.spark.indexer.uniref.writer.UniRefLightDataStoreWriter;

import com.typesafe.config.Config;

/**
 * Created 08/07/2020
 *
 * @author Edd
 */
@Slf4j
public class UniRefLightDataStoreIndexer implements DataStoreIndexer {

    private final JobParameter jobParameter;

    public UniRefLightDataStoreIndexer(JobParameter jobParameter) {
        this.jobParameter = jobParameter;
    }

    @Override
    public void indexInDataStore() {
        try {
            JavaFutureAction<Void> uniref100 = indexUniRef(UniRefType.UniRef100, jobParameter);
            JavaFutureAction<Void> uniref90 = indexUniRef(UniRefType.UniRef90, jobParameter);
            JavaFutureAction<Void> uniref50 = indexUniRef(UniRefType.UniRef50, jobParameter);
            uniref50.get();
            uniref90.get();
            uniref100.get();
        } catch (Exception e) {
            throw new IndexDataStoreException("Execution error during DataStore index", e);
        } finally {
            log.info("Completed UniRef Data Store index");
        }
    }

    private JavaFutureAction<Void> indexUniRef(UniRefType type, JobParameter jobParameter) {
        Config config = jobParameter.getApplicationConfig();
        DataStoreParameter parameter = getDataStoreParameter(config);

        UniRefLightRDDTupleReader reader = new UniRefLightRDDTupleReader(type, jobParameter, false);
        JavaRDD<UniRefEntryLight> uniRefRDD = reader.load();
        return uniRefRDD.foreachPartitionAsync(getWriter(parameter));
    }

    private DataStoreParameter getDataStoreParameter(Config config) {
        String numberOfConnections = config.getString("store.uniref.light.numberOfConnections");
        String maxRetry = config.getString("store.uniref.light.retry");
        String delay = config.getString("store.uniref.light.delay");
        return DataStoreParameter.builder()
                .connectionURL(config.getString("store.uniref.light.host"))
                .storeName(config.getString("store.uniref.light.storeName"))
                .numberOfConnections(Integer.parseInt(numberOfConnections))
                .maxRetry(Integer.parseInt(maxRetry))
                .delay(Long.parseLong(delay))
                .brotliEnabled(config.getBoolean("brotli.compression.enabled"))
                .brotliLevel(config.getInt("brotli.compression.level"))
                .build();
    }

    VoidFunction<Iterator<UniRefEntryLight>> getWriter(DataStoreParameter parameter) {
        return new UniRefLightDataStoreWriter(parameter);
    }
}
