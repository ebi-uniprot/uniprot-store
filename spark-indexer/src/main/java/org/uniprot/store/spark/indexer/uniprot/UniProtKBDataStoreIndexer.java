package org.uniprot.store.spark.indexer.uniprot;

import java.util.ResourceBundle;

import lombok.extern.slf4j.Slf4j;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.uniprot.core.uniprot.UniProtEntry;
import org.uniprot.store.datastore.voldemort.VoldemortClient;
import org.uniprot.store.datastore.voldemort.uniprot.VoldemortRemoteUniProtKBEntryStore;

import scala.Tuple2;

/**
 * @author lgonzales
 * @since 2020-03-06
 */
@Slf4j
public class UniProtKBDataStoreIndexer {

    public static void indexDataStore(
            JavaSparkContext sparkContext, ResourceBundle applicationConfig, String releaseName) {

        JavaPairRDD<String, UniProtEntry> uniprotRDD =
                (JavaPairRDD<String, UniProtEntry>)
                        UniProtKBRDDTupleReader.load(sparkContext, applicationConfig, releaseName);

        String numberOfConnections =
                applicationConfig.getString("store.uniprot.numberOfConnections");
        String storeName = applicationConfig.getString("store.uniprot.storeName");
        String connectionURL = applicationConfig.getString("store.uniprot.host");

        uniprotRDD.foreachPartition(
                uniProtEntryIterator -> {
                    VoldemortClient<UniProtEntry> client =
                            new VoldemortRemoteUniProtKBEntryStore(
                                    Integer.valueOf(numberOfConnections), storeName, connectionURL);
                    while (uniProtEntryIterator.hasNext()) {
                        Tuple2<String, UniProtEntry> tuple2 = uniProtEntryIterator.next();
                        client.saveEntry(tuple2._2);
                    }
                });
        log.info("Completed UniProtKb Data Store index");
    }
}
