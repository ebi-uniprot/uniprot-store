package org.uniprot.store.spark.indexer.chebi;

import java.util.Arrays;
import java.util.List;

import lombok.extern.slf4j.Slf4j;

import org.apache.spark.api.java.function.PairFunction;
import org.uniprot.core.util.Utils;
import org.uniprot.cv.chebi.Chebi;
import org.uniprot.cv.chebi.ChebiFileReader;

import scala.Tuple2;

/**
 * @author lgonzales
 * @since 2020-01-17
 */
@Slf4j
public class ChebiFileMapper implements PairFunction<String, String, Chebi> {
    private static final long serialVersionUID = 4927291557380701545L;

    @Override
    public Tuple2<String, Chebi> call(String chebiLines) throws Exception {
        ChebiFileReader reader = new ChebiFileReader();
        List<Chebi> result = reader.parseLines(Arrays.asList(chebiLines.split("\n")));
        if (Utils.notNullNotEmpty(result)) {
            Chebi chebi = result.get(0);
            return new Tuple2<>(chebi.getId(), chebi);
        } else {
            log.info("ERROR PARSING ChebiFileMapper WITH LINES: " + chebiLines);
            throw new RuntimeException("ERROR PARSING ChebiFileMapper WITH LINES: " + chebiLines);
        }
    }
}
