package org.uniprot.store.spark.indexer.common.reader;

import org.apache.spark.api.java.JavaRDD;

/**
 * Class responsible to load a JavaRDD.
 *
 * @author lgonzales
 * @since 30/05/2020
 */
public interface RDDReader<V> {

    JavaRDD<V> load();
}
