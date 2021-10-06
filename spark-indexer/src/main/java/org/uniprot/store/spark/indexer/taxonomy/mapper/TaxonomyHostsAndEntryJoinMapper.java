package org.uniprot.store.spark.indexer.taxonomy.mapper;

import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.uniprot.core.taxonomy.TaxonomyEntry;
import org.uniprot.core.uniprotkb.taxonomy.Taxonomy;
import org.uniprot.core.uniprotkb.taxonomy.impl.TaxonomyBuilder;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class TaxonomyHostsAndEntryJoinMapper implements PairFlatMapFunction<Tuple2<String, Tuple2<Iterable<String>, Optional<TaxonomyEntry>>>, String, Taxonomy> {
    private static final long serialVersionUID = -4297072275882262089L;

    @Override
    public Iterator<Tuple2<String, Taxonomy>> call(Tuple2<String, Tuple2<Iterable<String>, Optional<TaxonomyEntry>>> tuple) throws Exception {
        List<Tuple2<String, Taxonomy>> result = new ArrayList<>();
        if(tuple._2 != null && tuple._2._2.isPresent()) {
            TaxonomyEntry entry = tuple._2._2.get();
            Taxonomy host = new TaxonomyBuilder()
                    .taxonId(entry.getTaxonId())
                    .scientificName(entry.getScientificName())
                    .commonName(entry.getCommonName())
                    .mnemonic(entry.getMnemonic())
                    .build();
            for(String taxId: tuple._2._1){
                result.add(new Tuple2<>(taxId, host));
            }
        }
        return result.iterator();
    }

}
