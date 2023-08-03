package org.uniprot.store.spark.indexer.proteome;

import org.apache.spark.api.java.function.PairFunction;
import org.uniprot.store.spark.indexer.proteome.mapper.model.ProteomeStatisticsWrapper;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class ProteomeJoinMapper implements PairFunction<String, String, ProteomeStatisticsWrapper> {

    public static final String PROTEOMES = "proteomes";

    @Override
    public Tuple2<String, ProteomeStatisticsWrapper> call(String entryStr) throws Exception {

        String[] lines = entryStr.split("\n");
        List<String> filteredLines = Arrays.stream(lines)
                .filter(line -> line.startsWith(PROTEOMES))
                .collect(Collectors.toList());
        filteredLines

        return null;
    }
}
