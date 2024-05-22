package org.uniprot.store.spark.indexer.main.experimental;

import org.apache.spark.api.java.function.PairFunction;
import org.uniprot.core.uniparc.UniParcEntry;
import scala.Tuple2;

public class UniParcCrossRefCountRangeMapper implements PairFunction<UniParcEntry, String, Long> {
    private static final String[] RANGES = {
            "0-1000", "1000-2000", "2000-3000", "3000-4000", "4000-5000",
            "5000-6000", "6000-7000", "7000-8000", "8000-9000", "9000-10000",
            "10000-11000", "11000-12000", "12000-13000", "13000-14000", "14000-15000",
            "15000-16000", "16000-17000", "17000-18000", "18000-19000", "19000-20000", "20000-"
    };


    @Override
    public Tuple2<String, Long> call(UniParcEntry entry) throws Exception {
        int size = entry.getUniParcCrossReferences().size();
        int index = size / 1000;
        index = index >= RANGES.length ? RANGES.length-1 : index;
        return new Tuple2<>(RANGES[index], 1L);
    }
}
