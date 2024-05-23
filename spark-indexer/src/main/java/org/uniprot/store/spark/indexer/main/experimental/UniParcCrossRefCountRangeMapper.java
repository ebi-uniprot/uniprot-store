package org.uniprot.store.spark.indexer.main.experimental;

import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class UniParcCrossRefCountRangeMapper
        implements PairFunction<Tuple2<String, Integer>, String, Long> {
    private static final String[] RANGES = {
        "0-1000",
        "1000-2000",
        "2000-3000",
        "3000-4000",
        "4000-5000",
        "5000-6000",
        "6000-7000",
        "7000-8000",
        "8000-9000",
        "9000-10000",
        "10000-11000",
        "11000-12000",
        "12000-13000",
        "13000-14000",
        "14000-15000",
        "15000-16000",
        "16000-17000",
        "17000-18000",
        "18000-19000",
        "19000-20000",
        "20000-"
    };

    @Override
    public Tuple2<String, Long> call(Tuple2<String, Integer> tuple2) throws Exception {
        int size = tuple2._2;
        int index = size / 1000;
        index = index >= RANGES.length ? RANGES.length - 1 : index;
        return new Tuple2<>(RANGES[index], 1L);
    }
}
