package org.uniprot.store.spark.indexer.uniparc.mapper;

import java.io.Serial;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class UniParcSourceJoin
        implements PairFunction<
                Tuple2<String, Tuple2<String, Optional<Set<String>>>>,
                String,
                Map<String, Set<String>>> {
    @Serial private static final long serialVersionUID = -3987507162814044120L;

    @Override
    public Tuple2<String, Map<String, Set<String>>> call(
            Tuple2<String, Tuple2<String, Optional<Set<String>>>> tuple) throws Exception {
        String accession = tuple._1;
        Tuple2<String, Optional<Set<String>>> uniParcMapTuple = tuple._2;

        String uniParcId = uniParcMapTuple._1;
        Set<String> sources = getSources(uniParcMapTuple._2);

        return new Tuple2<>(uniParcId, Map.of(accession, sources));
    }

    private Set<String> getSources(Optional<Set<String>> optSources) {
        Set<String> sources = new HashSet<>();
        if (optSources.isPresent()) {
            sources.addAll(optSources.get());
        }
        return sources;
    }
}
