package org.uniprot.store.spark.indexer.uniparc.mapper;

import java.io.Serial;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function;
import org.jetbrains.annotations.NotNull;
import org.uniprot.core.Property;
import org.uniprot.core.taxonomy.TaxonomyEntry;
import org.uniprot.core.uniparc.UniParcCrossReference;
import org.uniprot.core.uniparc.UniParcEntry;
import org.uniprot.core.uniparc.impl.UniParcCrossReferenceBuilder;
import org.uniprot.core.uniparc.impl.UniParcEntryBuilder;
import org.uniprot.core.uniprotkb.taxonomy.Organism;
import org.uniprot.core.uniprotkb.taxonomy.Taxonomy;
import org.uniprot.core.uniprotkb.taxonomy.impl.OrganismBuilder;
import org.uniprot.core.util.Utils;

import lombok.extern.slf4j.Slf4j;
import org.uniprot.store.spark.indexer.uniparc.model.UniParcTaxonomySequenceSource;
import scala.Tuple2;

/**
 * @author lgonzales
 * @since 21/01/2021
 */
@Slf4j
public class UniParcEntryJoin implements Function<Tuple2<String, Tuple2<UniParcEntry, Optional<UniParcTaxonomySequenceSource>>>, UniParcEntry> {

    @Serial
    private static final long serialVersionUID = -6093868325315670369L;

    @Override
    public UniParcEntry call(Tuple2<String, Tuple2<UniParcEntry, Optional<UniParcTaxonomySequenceSource>>> tuple) throws Exception {
        UniParcEntry result = tuple._2._1;
        Optional<UniParcTaxonomySequenceSource> uniParcJoin = tuple._2._2;
        if (uniParcJoin.isPresent()) {
            UniParcTaxonomySequenceSource join = uniParcJoin.get();
            UniParcEntryBuilder builder = UniParcEntryBuilder.from(result);
            Map<Long, TaxonomyEntry> mappedTaxons = getMappedTaxons(join.organisms());
            Map<String, Set<String>> sourceMap = getMappedSourcesWithProteomes(result.getUniParcCrossReferences(), join.sequenceSources());

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

    private static Map<Long, TaxonomyEntry> getMappedTaxons(Iterable<TaxonomyEntry> organisms) {
        Map<Long, TaxonomyEntry> result = new HashMap<>();
        if(Utils.notNull(organisms)){
            result = StreamSupport.stream(organisms.spliterator(), false)
                    .collect(
                            Collectors.toMap(
                                    Taxonomy::getTaxonId,
                                    java.util.function.Function.identity(),
                                    (tax1, tax2) -> tax1));
        }
        return result;
    }

    private UniParcCrossReference mapTaxonomy(
            UniParcCrossReference xref, Map<Long, TaxonomyEntry> taxonMap) {
        if (Utils.notNull(xref.getOrganism())) {
            TaxonomyEntry taxonomyEntry = taxonMap.get(xref.getOrganism().getTaxonId());
            if (taxonomyEntry != null) {
                UniParcCrossReferenceBuilder builder = UniParcCrossReferenceBuilder.from(xref);
                Organism taxonomy =
                        new OrganismBuilder()
                                .taxonId(taxonomyEntry.getTaxonId())
                                .scientificName(taxonomyEntry.getScientificName())
                                .commonName(taxonomyEntry.getCommonName())
                                .build();
                builder.organism(taxonomy);
                xref = builder.build();
            } else {
                log.warn("Unable to get mapped taxon:" + xref.getOrganism().getTaxonId());
            }
        }
        return xref;
    }

    private UniParcCrossReference mapSequenceSource(UniParcCrossReference xref, Map<String, Set<String>> sourceMap) {
        Set<String> sources = sourceMap.get(xref.getId());
        if (Utils.notNullNotEmpty(sources)) {
            UniParcCrossReferenceBuilder xrefBuilder =
                    UniParcCrossReferenceBuilder.from(xref);
            xrefBuilder.propertiesAdd(new Property("sources", String.join(",", sources)));
            xref = xrefBuilder.build();
        }
        return xref;
    }

    private Map<String, Set<String>> getMappedSourcesWithProteomes(
            List<UniParcCrossReference> uniParcCrossReferences,
            Map<String, Set<String>> sourceMap) {
        Map<String, Set<String>> result = new HashMap<>();
        if (sourceMap != null) {
            Map<String, UniParcCrossReference> mappedCrossReference =
                    uniParcCrossReferences.stream()
                            .collect(
                                    Collectors.toMap(
                                            UniParcCrossReference::getId,
                                            e -> e,
                                            (e1, e2) -> e1.isActive() ? e1 : e2));
            result = sourceMap.entrySet().stream()
                    .collect(
                            Collectors.toMap(
                                    Map.Entry::getKey,
                                    e ->
                                            e.getValue().stream()
                                                    .map(mappedCrossReference::get)
                                                    .filter(xref -> xref != null && xref.getProteomeId() != null)
                                                    .map(
                                                            xref ->
                                                                    xref.getId()
                                                                            + ":"
                                                                            + xref.getProteomeId()
                                                                            + ":"
                                                                            + xref.getComponent())
                                                    .collect(Collectors.toSet())));
        }
        return result;
    }
}
