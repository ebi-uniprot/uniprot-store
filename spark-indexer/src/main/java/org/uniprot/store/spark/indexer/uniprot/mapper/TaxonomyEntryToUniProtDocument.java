package org.uniprot.store.spark.indexer.uniprot.mapper;

import java.util.*;
import java.util.stream.Collectors;

import lombok.extern.slf4j.Slf4j;

import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function;
import org.uniprot.core.taxonomy.TaxonomyEntry;
import org.uniprot.core.util.Utils;
import org.uniprot.store.search.document.uniprot.UniProtDocument;
import org.uniprot.store.spark.indexer.uniprot.converter.UniProtEntryConverterUtil;

import scala.Tuple2;

/**
 * This class Merge a Iterable of TaxonomyEntry into UniProtDocument for organism, lineage and virus
 * host.
 *
 * @author lgonzales
 * @since 2019-11-12
 */
@Slf4j
public class TaxonomyEntryToUniProtDocument
        implements Function<
                Tuple2<UniProtDocument, Optional<Iterable<TaxonomyEntry>>>, UniProtDocument> {
    private static final long serialVersionUID = -7030543979688452323L;

    private static final Map<Long, String> MODEL_ORGANIMS_TAX_NAME =
            Collections.unmodifiableMap(
                    new HashMap<Long, String>() {
                        private static final long serialVersionUID = 7236156454194571508L;

                        {
                            put(9606L, "Human");
                            put(10090L, "Mouse");
                            put(10116L, "Rat");
                            put(9913L, "Bovine");
                            put(7955L, "Zebrafish");
                            put(7227L, "Fruit fly");
                            put(6239L, "C. elegans");
                            put(44689L, "Slime mold");
                            put(3702L, "A. thaliana");
                            put(39947L, "Rice");
                            put(83333L, "E. coli K12");
                            put(224308L, "B. subtilis");
                            put(559292L, "S. cerevisiae");
                        }
                    });

    /**
     * @param tuple Iterable of TaxonomyEntry that are related with a protein entry.
     * @return UniProtDocument with organism, lineage and organism host.
     */
    @Override
    public UniProtDocument call(Tuple2<UniProtDocument, Optional<Iterable<TaxonomyEntry>>> tuple)
            throws Exception {
        UniProtDocument doc = tuple._1;
        if (tuple._2.isPresent()) {
            Map<Long, TaxonomyEntry> taxonomyEntryMap = new HashMap<>();
            tuple._2
                    .get()
                    .forEach(
                            taxonomyEntry -> {
                                taxonomyEntryMap.put(taxonomyEntry.getTaxonId(), taxonomyEntry);
                            });

            TaxonomyEntry organism = taxonomyEntryMap.get((long) doc.organismTaxId);
            if (Utils.notNull(organism)) {
                updateOrganismFields(doc, organism);
            } else {
                log.warn(
                        getWarnMessage(
                                doc.accession, taxonomyEntryMap.keySet(), doc.organismTaxId));
            }

            if (Utils.notNullNotEmpty(doc.organismHostIds)) {
                doc.organismHostIds.forEach(
                        taxId -> {
                            TaxonomyEntry organismHost = taxonomyEntryMap.get((long) taxId);
                            if (Utils.notNull(organismHost)) {
                                doc.organismHostNames.addAll(getOrganismNames(organismHost));
                            } else {
                                log.warn(
                                        getWarnMessage(
                                                doc.accession, taxonomyEntryMap.keySet(), taxId));
                            }
                        });
                doc.content.addAll(doc.organismHostNames);
            }
        } else {
            log.warn(
                    "Unable to join organism for "
                            + doc.organismTaxId
                            + " in document: "
                            + doc.accession);
        }
        return doc;
    }

    private String getWarnMessage(String accession, Set<Long> keys, int organismTaxId) {
        return "Unable to find organism id"
                + organismTaxId
                + " in mapped organisms "
                + keys
                + "for accession "
                + accession;
    }

    private void updateOrganismFields(UniProtDocument doc, TaxonomyEntry organism) {
        List<String> organismNames = getOrganismNames(organism);
        doc.organismName.addAll(organismNames);
        doc.organismSort =
                UniProtEntryConverterUtil.truncatedSortValue(String.join(" ", organismNames));
        if (organism.hasLineage()) {
            organism.getLineages()
                    .forEach(
                            lineage -> {
                                doc.taxLineageIds.add(new Long(lineage.getTaxonId()).intValue());
                                doc.organismTaxon.add(lineage.getScientificName());
                                if (lineage.hasCommonName()) {
                                    doc.organismTaxon.add(lineage.getCommonName());
                                }
                            });
        } else {
            log.warn("Unable to find organism lineage for: " + doc.organismTaxId);
        }
        doc.content.addAll(doc.organismName);
        doc.content.addAll(
                doc.taxLineageIds.stream().map(String::valueOf).collect(Collectors.toList()));
        doc.content.addAll(doc.organismTaxon);

        String modelOrgamism = MODEL_ORGANIMS_TAX_NAME.get(organism.getTaxonId());
        if (Utils.notNull(modelOrgamism)) {
            doc.modelOrganism = modelOrgamism;
        } else {
            doc.otherOrganism = organism.getScientificName();
        }
    }

    private List<String> getOrganismNames(TaxonomyEntry organism) {
        List<String> organismNames = new ArrayList<>();
        organismNames.add(organism.getScientificName());
        if (organism.hasCommonName()) {
            organismNames.add(organism.getCommonName());
        }
        if (organism.hasSynonyms()) {
            organismNames.addAll(organism.getSynonyms());
        }
        if (organism.hasMnemonic()) {
            organismNames.add(organism.getMnemonic());
        }
        return organismNames;
    }
}
