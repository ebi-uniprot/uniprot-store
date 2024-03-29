package org.uniprot.store.spark.indexer.suggest.mapper.flatfile;

import java.util.*;

import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.uniprot.core.uniprotkb.feature.UniProtKBFeature;
import org.uniprot.core.uniprotkb.feature.UniprotKBFeatureDatabase;
import org.uniprot.core.uniprotkb.feature.UniprotKBFeatureType;
import org.uniprot.core.util.Utils;
import org.uniprot.store.spark.indexer.suggest.SuggesterUtil;

import scala.Tuple2;

public class FlatFileToBindingFeatureChebi implements PairFlatMapFunction<String, String, String> {

    private static final long serialVersionUID = 1L;

    @Override
    public Iterator<Tuple2<String, String>> call(String entryStr) throws Exception {
        Set<Tuple2<String, String>> result = new HashSet<>();
        if (entryStr.contains("FT   BINDING         ")) {
            List<UniProtKBFeature> features =
                    SuggesterUtil.getFeaturesByType(entryStr, UniprotKBFeatureType.BINDING);
            features.stream()
                    .filter(feature -> Utils.notNullNotEmpty(feature.getFeatureCrossReferences()))
                    .flatMap(feature -> feature.getFeatureCrossReferences().stream())
                    .filter(xref -> xref.getDatabase() == UniprotKBFeatureDatabase.CHEBI)
                    .forEach(
                            xref -> {
                                String id = xref.getId();
                                if (id.startsWith("CHEBI:")) {
                                    id = id.substring("CHEBI:".length());
                                }
                                result.add(new Tuple2<>(id, xref.getId()));
                            });
        }
        return result.iterator();
    }
}
