package indexer.uniprot.converter;

import org.uniprot.core.uniprot.taxonomy.Organism;
import org.uniprot.core.uniprot.taxonomy.OrganismHost;
import org.uniprot.store.search.document.uniprot.UniProtDocument;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * @author lgonzales
 * @since 2019-09-05
 */
class UniprotEntryTaxonomyConverter {

    private static final Map<Integer, String> POPULAR_ORGANIMS_TAX_NAME = Collections
            .unmodifiableMap(new HashMap<Integer, String>() {
                {
                    put(9606, "Human");
                    put(10090, "Mouse");
                    put(10116, "Rat");
                    put(9913, "Bovine");
                    put(7955, "Zebrafish");
                    put(7227, "Fruit fly");
                    put(6239, "C. elegans");
                    put(44689, "Slime mold");
                    put(3702, "A. thaliana");
                    put(39947, "Rice");
                    put(83333, "E. coli K12");
                    put(224308, "B. subtilis");
                    put(559292, "S. cerevisiae");
                }
            });

    void convertOrganism(Organism organism, UniProtDocument document) {
        if (organism != null) {
            int taxonomyId = Math.toIntExact(organism.getTaxonId());
            document.organismTaxId = taxonomyId;

/*
            //TODO: @lgonzales: This code will be moved to the join with the taxonnomyRDD
            if(taxonomyRepo != null) {
                Optional<TaxonomicNode> taxonomicNode = taxonomyRepo.retrieveNodeUsingTaxID(taxonomyId);
                if (taxonomicNode.isPresent()) {

                    TaxonomicNode node = taxonomicNode.get();
                    List<String> extractedTaxoNode = TaxonomyRepoUtil.extractTaxonFromNode(node);
                    document.organismName.addAll(extractedTaxoNode);
                    document.organismSort = UniProtEntryConverterUtil.truncatedSortValue(String.join(" ", extractedTaxoNode));

                    String popularOrgamism = POPULAR_ORGANIMS_TAX_NAME.get(taxonomyId);
                    if (popularOrgamism != null) {
                        document.popularOrganism = popularOrgamism;
                    } else {
                        if (node.mnemonic() != null && !node.mnemonic().isEmpty()) {
                            document.otherOrganism = node.mnemonic();
                        } else if (node.commonName() != null && !node.commonName().isEmpty()) {
                            document.otherOrganism = node.commonName();
                        } else {
                            document.otherOrganism = node.scientificName();
                        }
                    }
                    addTaxonSuggestions(SuggestDictionary.ORGANISM, taxonomyId, extractedTaxoNode);

                }
            }*/
            convertLineageTaxon(taxonomyId, document);
        }
    }

    void convertOrganismHosts(List<OrganismHost> hosts, UniProtDocument document) {
        hosts.forEach(host -> {
            int taxonomyId = Math.toIntExact(host.getTaxonId());
            document.organismHostIds.add(taxonomyId);
/*            if(taxonomyRepo != null) {
                Optional<TaxonomicNode> taxonomicNode = taxonomyRepo.retrieveNodeUsingTaxID(taxonomyId);
                if (taxonomicNode.isPresent()) {
                    TaxonomicNode node = taxonomicNode.get();
                    List<String> extractedTaxoNode = TaxonomyRepoUtil.extractTaxonFromNode(node);
                    document.organismHostNames.addAll(extractedTaxoNode);
                    addTaxonSuggestions(SuggestDictionary.HOST, taxonomyId, extractedTaxoNode);
                }
            }*/
        });

        document.content.addAll(document.organismHostNames);
        document.content.addAll(document.organismHostIds.stream()
                .map(String::valueOf)
                .collect(Collectors.toList()));
    }

    private void convertLineageTaxon(int taxId, UniProtDocument document) {
/*        if (taxId > 0 && taxonomyRepo != null) {
            List<TaxonomicNode> nodes = TaxonomyRepoUtil.getTaxonomyLineage(taxonomyRepo, taxId);
            nodes.forEach(node -> {
                int id = node.id();
                document.taxLineageIds.add(id);
                List<String> taxons = TaxonomyRepoUtil.extractTaxonFromNode(node);
                document.organismTaxon.addAll(taxons);
                addTaxonSuggestions(SuggestDictionary.TAXONOMY, id, taxons);
            });
        }*/
        document.content.addAll(document.organismTaxon);
        document.content.addAll(document.taxLineageIds.stream()
                .map(String::valueOf)
                .collect(Collectors.toList()));
    }

/*    private void addTaxonSuggestions(SuggestDictionary dicType, int id, List<String> taxons) {
        Iterator<String> taxonIterator = taxons.iterator();
        if (taxonIterator.hasNext()) {
            String idStr = Integer.toString(id);
            String key = UniProtEntryConverterUtil.createSuggestionMapKey(dicType, idStr);
            if (suggestions.containsKey(key)) {
                SuggestDocument doc = suggestions.get(key);
                String mainName = taxonIterator.next();
                if (doc.value == null || !doc.value.equals(mainName)) {
                    doc.value = mainName;
                }

                List<String> currentSynonyms = new ArrayList<>(doc.altValues);
                while (taxonIterator.hasNext()) {
                    String synonym = taxonIterator.next();
                    if (!doc.altValues.contains(synonym)) {
                        currentSynonyms.add(synonym);
                    }
                }
                doc.altValues = currentSynonyms;
            } else {
                SuggestDocument.SuggestDocumentBuilder documentBuilder = SuggestDocument.builder().id(idStr)
                        .dictionary(dicType.name()).value(taxonIterator.next());
                while (taxonIterator.hasNext()) {
                    documentBuilder.altValue(taxonIterator.next());
                }
                suggestions.put(key, documentBuilder.build());
            }
        }
    }*/
}
