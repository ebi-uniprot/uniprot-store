package org.uniprot.store.spark.indexer.uniparc.mapper;

import static org.uniprot.store.spark.indexer.uniparc.mapper.UniParcJoinUtils.*;

import java.io.Serial;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function;
import org.jetbrains.annotations.NotNull;
import org.uniprot.core.Property;
import org.uniprot.core.taxonomy.TaxonomyEntry;
import org.uniprot.core.uniparc.UniParcCrossReference;
import org.uniprot.core.uniparc.UniParcEntry;
import org.uniprot.core.uniparc.impl.UniParcCrossReferenceBuilder;
import org.uniprot.core.uniparc.impl.UniParcEntryBuilder;
import org.uniprot.core.util.Utils;
import org.uniprot.store.spark.indexer.uniparc.model.UniParcTaxonomySequenceSource;

import lombok.extern.slf4j.Slf4j;
import scala.Tuple2;

/**
 * @author lgonzales
 * @since 21/01/2021
 */
@Slf4j
public class UniParcEntryJoin
        implements Function<
                Tuple2<String, Tuple2<UniParcEntry, Optional<UniParcTaxonomySequenceSource>>>,
                UniParcEntry> {

    @Serial private static final long serialVersionUID = -6093868325315670369L;

    @Override
    public UniParcEntry call(
            Tuple2<String, Tuple2<UniParcEntry, Optional<UniParcTaxonomySequenceSource>>> tuple)
            throws Exception {
        UniParcEntry result = tuple._2._1;
        Optional<UniParcTaxonomySequenceSource> uniParcJoin = tuple._2._2;
        if (uniParcJoin.isPresent()) {
            UniParcTaxonomySequenceSource join = uniParcJoin.get();
            UniParcEntryBuilder builder = UniParcEntryBuilder.from(result);
            Map<Long, TaxonomyEntry> mappedTaxons = getMappedTaxons(join.organisms());
            Map<String, Set<String>> sourceMap =
                    getMappedSourcesWithProteomes(
                            result.getUniParcCrossReferences(), join.sequenceSources());

            List<UniParcCrossReference> mappedXRefs =
                    result.getUniParcCrossReferences().stream()
                            .map(xref -> mapTaxonomy(xref, mappedTaxons))
                            .map(xref -> mapSequenceSource(xref, sourceMap))
                            .collect(Collectors.toList());
            builder.uniParcCrossReferencesSet(mappedXRefs);
            result = builder.build();
        }
        return result;
    }

    private UniParcCrossReference mapSequenceSource(
            UniParcCrossReference xref, Map<String, Set<String>> sourceMap) {
        Set<String> sources = sourceMap.get(xref.getId());
        if (Utils.notNullNotEmpty(sources)) {
            UniParcCrossReferenceBuilder xrefBuilder = UniParcCrossReferenceBuilder.from(xref);
            xrefBuilder.propertiesAdd(new Property("sources", String.join(",", sources)));
            xref = xrefBuilder.build();
        }
        return xref;
    }

    private Map<String, Set<String>> getMappedSourcesWithProteomes(
            List<UniParcCrossReference> xrefs, Map<String, Set<String>> sourceMap) {
        Map<String, Set<String>> result = new HashMap<>();
        if (sourceMap != null) {
            Map<String, UniParcCrossReference> mappedCrossReference =
                    getMappedCrossReference(xrefs);
            result =
                    sourceMap.entrySet().stream()
                            .collect(
                                    Collectors.toMap(
                                            Map.Entry::getKey,
                                            source -> addProteome(source, mappedCrossReference)));
        }
        return result;
    }

    @NotNull
    private static Map<String, UniParcCrossReference> getMappedCrossReference(
            List<UniParcCrossReference> xrefs) {
        return xrefs.stream()
                .collect(
                        Collectors.toMap(
                                UniParcCrossReference::getId,
                                xref -> xref,
                                (xref1, xref2) -> xref1.isActive() ? xref1 : xref2));
    }

    @NotNull
    private static Set<String> addProteome(
            Map.Entry<String, Set<String>> e,
            Map<String, UniParcCrossReference> mappedCrossReference) {
        return e.getValue().stream()
                .map(mappedCrossReference::get)
                .filter(xref -> xref != null && xref.getProteomeId() != null)
                .map(UniParcEntryJoin::getAccessionWithProteome)
                .collect(Collectors.toSet());
    }

    private static String getAccessionWithProteome(UniParcCrossReference xref) {
        return xref.getId() + ":" + xref.getProteomeId() + ":" + xref.getComponent();
    }
}
