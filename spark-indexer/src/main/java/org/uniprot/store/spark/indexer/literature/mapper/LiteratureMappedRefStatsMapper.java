package org.uniprot.store.spark.indexer.literature.mapper;

import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

/**
 * @author lgonzales
 * @since 26/04/2021
 */
public class LiteratureMappedRefStatsMapper
        implements PairFunction<Tuple2<String, Long>, String, Long> {

    private static final long serialVersionUID = 2031459603468933914L;

    @Override
    public Tuple2<String, Long> call(Tuple2<String, Long> tuple2) throws Exception {
        String accessionPubmedKey = tuple2._1;
        String pubMedKey = accessionPubmedKey.substring(accessionPubmedKey.indexOf("_") + 1);
        return new Tuple2<>(pubMedKey, tuple2._2);
    }
}
