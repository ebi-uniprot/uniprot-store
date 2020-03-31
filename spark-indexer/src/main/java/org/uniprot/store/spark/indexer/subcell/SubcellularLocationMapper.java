package org.uniprot.store.spark.indexer.subcell;

import lombok.extern.slf4j.Slf4j;

import org.apache.spark.api.java.function.PairFunction;
import org.uniprot.core.cv.subcell.SubcellularLocationEntry;

import scala.Tuple2;

/**
 * @author lgonzales
 * @since 2020-01-16
 */
@Slf4j
public class SubcellularLocationMapper
        implements PairFunction<SubcellularLocationEntry, String, SubcellularLocationEntry> {

    private static final long serialVersionUID = -3872452787976158342L;

    @Override
    public Tuple2<String, SubcellularLocationEntry> call(SubcellularLocationEntry entry)
            throws Exception {
        return new Tuple2<>(entry.getName().toLowerCase(), entry);
    }
}
