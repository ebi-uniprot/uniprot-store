package org.uniprot.store.spark.indexer.common.reader;

import org.apache.spark.api.java.JavaPairRDD;

/**
 * Class responsible to load a JavaPairRDD.
 *
 * @author lgonzales
 * @since 30/05/2020
 */
public interface PairRDDReader<K, V> {

    JavaPairRDD<K, V> load();
}
