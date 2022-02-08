package org.uniprot.store.spark.indexer.uniprot.mapper;

import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.PairFunction;
import org.uniprot.core.cv.subcell.SubcellularLocationEntry;

import scala.Tuple2;

/**
 * @author sahmad
 * @created 08/02/2022
 */
public class SubcellularLocationEntryTupleMapper implements PairFunction<Tuple2<SubcellularLocationEntry, Optional<String>>, String, SubcellularLocationEntry> {
    @Override
    public Tuple2<String, SubcellularLocationEntry> call(Tuple2<SubcellularLocationEntry, Optional<String>> tuple) throws Exception {
        return new Tuple2<>(tuple._2.get(), tuple._1());
    }
}
