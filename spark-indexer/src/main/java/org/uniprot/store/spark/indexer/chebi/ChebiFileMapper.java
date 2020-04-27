package org.uniprot.store.spark.indexer.chebi;

import java.util.Arrays;
import java.util.List;

import lombok.extern.slf4j.Slf4j;

import org.apache.spark.api.java.function.PairFunction;
import org.uniprot.core.cv.chebi.ChebiEntry;
import org.uniprot.core.util.Utils;
import org.uniprot.cv.chebi.ChebiFileReader;
import org.uniprot.store.spark.indexer.common.exception.SparkIndexException;

import scala.Tuple2;

/**
 * @author lgonzales
 * @since 2020-01-17
 */
@Slf4j
public class ChebiFileMapper implements PairFunction<String, String, ChebiEntry> {
    private static final long serialVersionUID = 4927291557380701545L;

    @Override
    public Tuple2<String, ChebiEntry> call(String chebiLines) throws Exception {
        ChebiFileReader reader = new ChebiFileReader();
        List<ChebiEntry> result = reader.parseLines(Arrays.asList(chebiLines.split("\n")));
        if (Utils.notNullNotEmpty(result)) {
            ChebiEntry chebi = result.get(0);
            return new Tuple2<>(chebi.getId(), chebi);
        } else {
            log.info("ERROR PARSING ChebiFileMapper WITH LINES: " + chebiLines);
            throw new SparkIndexException(
                    "ERROR PARSING ChebiFileMapper WITH LINES: " + chebiLines);
        }
    }
}
