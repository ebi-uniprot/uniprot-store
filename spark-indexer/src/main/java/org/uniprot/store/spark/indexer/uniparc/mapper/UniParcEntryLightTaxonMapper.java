package org.uniprot.store.spark.indexer.uniparc.mapper;

import java.io.Serial;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.spark.api.java.function.Function;
import org.uniprot.core.uniparc.UniParcEntryLight;
import org.uniprot.core.uniparc.impl.UniParcEntryLightBuilder;
import org.uniprot.core.util.Pair;
import org.uniprot.core.util.PairImpl;

import scala.Tuple2;

public class UniParcEntryLightTaxonMapper
        implements Function<
                Tuple2<UniParcEntryLight, List<Tuple2<String, String>>>, UniParcEntryLight> {
    @Serial private static final long serialVersionUID = 8954314933313810454L;

    @Override
    public UniParcEntryLight call(
            Tuple2<UniParcEntryLight, List<Tuple2<String, String>>> uniParcTaxons)
            throws Exception {
        UniParcEntryLight uniParcEntryLight = uniParcTaxons._1;
        List<Tuple2<String, String>> commonTaxons = uniParcTaxons._2;
        List<Pair<String, String>> commonTaxonsPairs =
                commonTaxons.stream()
                        .map(ct -> new PairImpl<>(ct._1, ct._2))
                        .collect(Collectors.toList());
        return UniParcEntryLightBuilder.from(uniParcEntryLight)
                .commonTaxonsSet(commonTaxonsPairs)
                .build();
    }
}
