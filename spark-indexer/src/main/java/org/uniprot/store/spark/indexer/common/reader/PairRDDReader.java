package org.uniprot.store.spark.indexer.common.reader;

import org.apache.spark.api.java.JavaPairRDD;

/**
 * @author lgonzales
 * @since 30/05/2020
 */
public interface PairRDDReader<K, V> {

    JavaPairRDD<K, V> load();
}
