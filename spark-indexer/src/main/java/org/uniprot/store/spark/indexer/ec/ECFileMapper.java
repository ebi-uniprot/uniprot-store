package org.uniprot.store.spark.indexer.ec;

import org.apache.spark.api.java.function.PairFunction;
import org.uniprot.core.cv.ec.ECEntry;

import scala.Tuple2;

/**
 * @author lgonzales
 * @since 2020-01-17
 */
public class ECFileMapper implements PairFunction<ECEntry, String, ECEntry> {

    private static final long serialVersionUID = 3886786650647301422L;

    @Override
    public Tuple2<String, ECEntry> call(ECEntry ec) throws Exception {
        return new Tuple2<>(ec.getId(), ec);
    }
}
