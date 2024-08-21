package org.uniprot.store.spark.indexer.uniprot.mapper;

import java.io.Serial;

import org.apache.spark.api.java.function.PairFunction;
import org.uniprot.store.spark.indexer.common.exception.SparkIndexException;

import scala.Tuple2;

public class UniProtKBUniParcMapper implements PairFunction<String, String, String> {

    @Serial private static final long serialVersionUID = 1882386700586087003L;

    @Override
    public Tuple2<String, String> call(String line) throws Exception {
        if (line == null || line.split("\s").length != 3) {
            throw new SparkIndexException("Unable to parse UniProtKBUniParcMapper line: " + line);
        }
        String[] tokens = line.split("\s");
        String uniParcId = tokens[0].strip();
        String accession = tokens[1].strip();
        return new Tuple2<>(accession, uniParcId);
    }
}
