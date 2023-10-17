package org.uniprot.store.spark.indexer.proteome.mapper;

import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.PairFunction;
import org.uniprot.core.proteome.ProteomeEntry;
import org.uniprot.core.proteome.impl.ProteomeEntryBuilder;
import scala.Tuple2;

public class ProteomeGeneCountToProteomeEntryMapper implements PairFunction<Tuple2<ProteomeEntry, Optional<Integer>>, String, ProteomeEntry> {
    private static final long serialVersionUID = 4862930174648399807L;

    @Override
    public Tuple2<String, ProteomeEntry> call(Tuple2<ProteomeEntry, Optional<Integer>> proteomeEntryProteomeGeneCountTuple2) throws Exception {
        return new Tuple2<>(
                proteomeEntryProteomeGeneCountTuple2._1.getId().getValue(),
                ProteomeEntryBuilder.from(proteomeEntryProteomeGeneCountTuple2._1)
                        .geneCount(proteomeEntryProteomeGeneCountTuple2._2.orElse(0))
                        .build());
    }
}
