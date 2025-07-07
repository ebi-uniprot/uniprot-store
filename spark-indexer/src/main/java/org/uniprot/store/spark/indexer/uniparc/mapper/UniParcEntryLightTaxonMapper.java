package org.uniprot.store.spark.indexer.uniparc.mapper;

import java.io.Serial;
import java.util.List;

import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function;
import org.uniprot.core.uniparc.CommonOrganism;
import org.uniprot.core.uniparc.UniParcEntryLight;
import org.uniprot.core.uniparc.impl.CommonOrganismBuilder;
import org.uniprot.core.uniparc.impl.UniParcEntryLightBuilder;

import scala.Tuple2;
import scala.Tuple3;

public class UniParcEntryLightTaxonMapper
        implements Function<
                Tuple2<UniParcEntryLight, Optional<List<Tuple3<String, Long, String>>>>,
                UniParcEntryLight> {
    @Serial private static final long serialVersionUID = 8954314933313810454L;

    @Override
    public UniParcEntryLight call(
            Tuple2<UniParcEntryLight, Optional<List<Tuple3<String, Long, String>>>> uniParcTaxons)
            throws Exception {
        UniParcEntryLight uniParcEntryLight = uniParcTaxons._1;
        if (uniParcTaxons._2.isPresent()) { // check if taxonomy exists
            List<Tuple3<String, Long, String>> commonTaxons = uniParcTaxons._2.get();
            List<CommonOrganism> mappedCommonTaxon =
                    commonTaxons.stream().map(this::getCommonTaxon).toList();
            return UniParcEntryLightBuilder.from(uniParcEntryLight)
                    .commonTaxonsSet(mappedCommonTaxon)
                    .build();
        }
        return uniParcEntryLight;
    }

    private CommonOrganism getCommonTaxon(Tuple3<String, Long, String> ct) {
        return new CommonOrganismBuilder()
                .topLevel(ct._1())
                .commonTaxonId(ct._2())
                .commonTaxon(ct._3())
                .build();
    }
}
