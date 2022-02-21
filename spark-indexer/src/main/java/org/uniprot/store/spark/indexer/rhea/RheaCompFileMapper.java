package org.uniprot.store.spark.indexer.rhea;

import org.apache.spark.api.java.function.PairFunction;
import org.uniprot.store.spark.indexer.rhea.model.RheaComp;

import scala.Tuple2;

public class RheaCompFileMapper implements PairFunction<String, String, RheaComp> {
    private static final long serialVersionUID = 5704304051520521438L;

    @Override
    public Tuple2<String, RheaComp> call(String line) throws Exception {
        String rheaCompId = line.substring(0, line.indexOf("\t")).strip();
        String rheaCompName = line.substring(line.indexOf("\t")).strip();
        RheaComp rheaComp = RheaComp.builder().id(rheaCompId).name(rheaCompName).build();
        return new Tuple2<>(rheaCompId, rheaComp);
    }
}
