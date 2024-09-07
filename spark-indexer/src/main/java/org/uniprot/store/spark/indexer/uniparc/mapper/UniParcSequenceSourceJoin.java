package org.uniprot.store.spark.indexer.uniparc.mapper;

import java.io.Serial;
import java.util.*;
import java.util.stream.Collectors;

import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function;
import org.uniprot.core.Property;
import org.uniprot.core.uniparc.UniParcCrossReference;
import org.uniprot.core.uniparc.UniParcEntry;
import org.uniprot.core.uniparc.impl.UniParcCrossReferenceBuilder;
import org.uniprot.core.uniparc.impl.UniParcEntryBuilder;
import org.uniprot.core.util.Utils;
import org.uniprot.store.spark.indexer.common.exception.IndexDataStoreException;

import scala.Tuple2;

@Slf4j
public class UniParcSequenceSourceJoin
        implements Function<
                Tuple2<UniParcEntry, Optional<Map<String, Set<String>>>>, UniParcEntry> {
    @Serial private static final long serialVersionUID = -6606026932459151470L;

    @Override
    public UniParcEntry call(Tuple2<UniParcEntry, Optional<Map<String, Set<String>>>> tuple2)
            throws Exception {
        UniParcEntry result = tuple2._1;
        if (tuple2._2.isPresent()) {
            Map<String, Set<String>> sourceMap = tuple2._2.get();
            List<String> invalidIds =
                    getInvalidMappedIds(result.getUniParcCrossReferences(), sourceMap);
            if (!invalidIds.isEmpty()) {
                log.warn(
                        "INVALID SOURCE MAP IDS for "
                                + result.getUniParcId().getValue()
                                + " not found ids "
                                + String.join(",", invalidIds));
            }

            sourceMap =
                    getMappedSourcesWithProteomes(result.getUniParcCrossReferences(), sourceMap);
            UniParcEntryBuilder builder = UniParcEntryBuilder.from(result);
            List<UniParcCrossReference> updatedXRefs = new ArrayList<>();
            for (UniParcCrossReference xref : result.getUniParcCrossReferences()) {
                Set<String> sources = sourceMap.get(xref.getId());
                if (Utils.notNullNotEmpty(sources)) {
                    UniParcCrossReferenceBuilder xrefBuilder =
                            UniParcCrossReferenceBuilder.from(xref);
                    xrefBuilder.propertiesAdd(new Property("sources", String.join(",", sources)));
                    xref = xrefBuilder.build();
                }
                updatedXRefs.add(xref);
            }
            builder.uniParcCrossReferencesSet(updatedXRefs);
            result = builder.build();
        }
        return result;
    }

    private List<String> getInvalidMappedIds(
            List<UniParcCrossReference> uniParcCrossReferences,
            Map<String, Set<String>> sourceMap) {
        List<String> uniParcXrefIds =
                uniParcCrossReferences.stream().map(UniParcCrossReference::getId).toList();
        return sourceMap.entrySet().stream()
                .flatMap(
                        entry -> {
                            Set<String> values = new HashSet<>(entry.getValue());
                            values.add(entry.getKey());
                            return values.stream();
                        })
                .filter(xref -> !uniParcXrefIds.contains(xref))
                .toList();
    }

    private Map<String, Set<String>> getMappedSourcesWithProteomes(
            List<UniParcCrossReference> uniParcCrossReferences,
            Map<String, Set<String>> sourceMap) {
        Map<String, UniParcCrossReference> mappedCrossReference =
                uniParcCrossReferences.stream()
                        .collect(
                                Collectors.toMap(
                                        UniParcCrossReference::getId,
                                        e -> e,
                                        (e1, e2) -> e1.isActive() ? e1 : e2));
        return sourceMap.entrySet().stream()
                .collect(
                        Collectors.toMap(
                                Map.Entry::getKey,
                                e ->
                                        e.getValue().stream()
                                                .map(mappedCrossReference::get)
                                                .filter(xref -> xref.getProteomeId() != null)
                                                .map(
                                                        xref ->
                                                                xref.getId()
                                                                        + ":"
                                                                        + xref.getProteomeId()
                                                                        + ":"
                                                                        + xref.getComponent())
                                                .collect(Collectors.toSet())));
    }
}
