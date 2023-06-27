package org.uniprot.store.spark.indexer.uniprot.mapper;

import java.util.Set;

import org.apache.spark.api.java.function.PairFunction;
import org.uniprot.store.spark.indexer.common.exception.SparkIndexException;

import scala.Tuple2;

public class UniProtIdTrackerMapper implements PairFunction<String, String, Set<String>> {

    @Override
    public Tuple2<String, Set<String>> call(String line) throws Exception {
        if (line == null || line.split("\t").length != 2) {
            throw new SparkIndexException("Unable to parse UniProtIdTrackerMapper line: " + line);
        }
        String[] tokens = line.split("\t");
        String accession = tokens[0].strip();
        String proteinId = tokens[1].strip();
        return new Tuple2<>(accession, Set.of(proteinId));
    }
}
