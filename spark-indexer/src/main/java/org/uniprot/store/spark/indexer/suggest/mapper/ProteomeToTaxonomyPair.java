package org.uniprot.store.spark.indexer.suggest.mapper;

import org.apache.spark.api.java.function.PairFunction;
import org.uniprot.core.proteome.ProteomeEntry;

import scala.Tuple2;

/**
 * @author lgonzales
 * @since 25/11/2020
 */
public class ProteomeToTaxonomyPair
        implements PairFunction<Tuple2<String, ProteomeEntry>, String, String> {
    private static final long serialVersionUID = 7752127351935413490L;

    @Override
    public Tuple2<String, String> call(Tuple2<String, ProteomeEntry> tuple) throws Exception {
        String taxId = String.valueOf(tuple._2.getTaxonomy().getTaxonId());
        String proteomeId = tuple._1;
        return new Tuple2<>(taxId, proteomeId);
    }
}
