package org.uniprot.store.spark.indexer.uniref;

import java.util.ResourceBundle;

import lombok.extern.slf4j.Slf4j;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.uniprot.core.uniref.UniRefEntry;
import org.uniprot.core.uniref.UniRefType;
import org.uniprot.store.datastore.voldemort.VoldemortClient;
import org.uniprot.store.datastore.voldemort.uniref.VoldemortRemoteUniRefEntryStore;

/**
 * @author lgonzales
 * @since 2020-03-06
 */
@Slf4j
public class UniRefDataStoreIndexer {

    public static void indexDataStore(
            JavaSparkContext sparkContext, ResourceBundle applicationConfig, String releaseName) {
        SparkConf sparkConf = sparkContext.sc().conf();

        indexUniRef(UniRefType.UniRef50, applicationConfig, releaseName, sparkConf);
        indexUniRef(UniRefType.UniRef90, applicationConfig, releaseName, sparkConf);
        indexUniRef(UniRefType.UniRef100, applicationConfig, releaseName, sparkConf);

        log.info("Completed UniRef Data Store index");
    }

    private static void indexUniRef(
            UniRefType type,
            ResourceBundle applicationConfig,
            String releaseName,
            SparkConf sparkConf) {
        String numberOfConnections =
                applicationConfig.getString("store.uniprot.numberOfConnections");
        String storeName = applicationConfig.getString("store.uniprot.storeName");
        String connectionURL = applicationConfig.getString("store.uniprot.host");

        JavaRDD<UniRefEntry> uniRefRDD =
                (JavaRDD<UniRefEntry>)
                        UniRefRDDTupleReader.load(type, sparkConf, applicationConfig, releaseName);

        uniRefRDD.foreachPartition(
                uniProtEntryIterator -> {
                    VoldemortClient<UniRefEntry> client =
                            new VoldemortRemoteUniRefEntryStore(
                                    Integer.valueOf(numberOfConnections), storeName, connectionURL);
                    while (uniProtEntryIterator.hasNext()) {
                        UniRefEntry entry = uniProtEntryIterator.next();
                        client.saveEntry(entry);
                    }
                });
        log.info("Completed " + type + " Data Store index");
    }
}
