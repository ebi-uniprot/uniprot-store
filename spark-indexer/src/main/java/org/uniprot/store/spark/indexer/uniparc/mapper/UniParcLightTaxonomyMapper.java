package org.uniprot.store.spark.indexer.uniparc.mapper;

import java.io.Serial;
import java.util.Iterator;

import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.uniprot.core.uniparc.CommonOrganism;
import org.uniprot.core.uniparc.UniParcEntryLight;

import scala.Tuple2;

public class UniParcLightTaxonomyMapper
        implements PairFlatMapFunction<UniParcEntryLight, String, String> {

    @Serial private static final long serialVersionUID = 2418839185049239335L;

    @Override
    public Iterator<Tuple2<String, String>> call(UniParcEntryLight uniParcEntry) throws Exception {
        return uniParcEntry.getCommonTaxons().stream()
                .map(CommonOrganism::getTopLevel)
                .map(taxId -> new Tuple2<>(taxId, uniParcEntry.getUniParcId()))
                .toList()
                .iterator();
    }
}
