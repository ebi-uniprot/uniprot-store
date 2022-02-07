package org.uniprot.store.spark.indexer.subcellularlocation.mapper;

import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function;
import org.uniprot.core.Statistics;
import org.uniprot.core.cv.subcell.SubcellularLocationEntry;
import org.uniprot.core.cv.subcell.impl.SubcellularLocationEntryBuilder;

import scala.Tuple2;

/**
 * @author sahmad
 * @created 07/02/2022
 */
public class SubcellularLocationEntryStatisticsMerger implements Function<Tuple2<SubcellularLocationEntry,
        Optional<Statistics>>,SubcellularLocationEntry> {

    @Override
    public SubcellularLocationEntry call(Tuple2<SubcellularLocationEntry, Optional<Statistics>> tuple) throws Exception {
        Optional<Statistics> optStatistics = tuple._2;
        if(optStatistics.isPresent()){
            SubcellularLocationEntry entry = tuple._1;
            return SubcellularLocationEntryBuilder.from(entry).statistics(optStatistics.get()).build();
        }
        return tuple._1;
    }
}
