package org.uniprot.store.indexer.uniprotkb.converter;

import java.util.*;
import java.util.stream.Collectors;

import org.uniprot.core.uniprotkb.taxonomy.Organism;
import org.uniprot.core.uniprotkb.taxonomy.OrganismHost;
import org.uniprot.cv.taxonomy.TaxonomicNode;
import org.uniprot.cv.taxonomy.TaxonomyRepo;
import org.uniprot.store.indexer.util.TaxonomyRepoUtil;
import org.uniprot.store.search.document.suggest.SuggestDictionary;
import org.uniprot.store.search.document.suggest.SuggestDocument;
import org.uniprot.store.search.document.uniprot.UniProtDocument;

/**
 * @author lgonzales
 * @since 2019-09-05
 */
class UniProtEntryTaxonomyConverter {

    private static final Map<Integer, String> MODEL_ORGANISMS_TAX_NAME;

    static {
        HashMap<Integer, String> modelOrganismsTaxName = new HashMap<>();
        modelOrganismsTaxName.put(9606, "Human");
        modelOrganismsTaxName.put(10090, "Mouse");
        modelOrganismsTaxName.put(10116, "Rat");
        modelOrganismsTaxName.put(9913, "Bovine");
        modelOrganismsTaxName.put(7955, "Zebrafish");
        modelOrganismsTaxName.put(7227, "Fruit fly");
        modelOrganismsTaxName.put(6239, "C. elegans");
        modelOrganismsTaxName.put(44689, "Slime mold");
        modelOrganismsTaxName.put(3702, "A. thaliana");
        modelOrganismsTaxName.put(39947, "Rice");
        modelOrganismsTaxName.put(83333, "E. coli K12");
        modelOrganismsTaxName.put(224308, "B. subtilis");
        modelOrganismsTaxName.put(559292, "S. cerevisiae");
        MODEL_ORGANISMS_TAX_NAME = Collections.unmodifiableMap(modelOrganismsTaxName);
    }

    private final TaxonomyRepo taxonomyRepo;
    private final Map<String, SuggestDocument> suggestions;

    UniProtEntryTaxonomyConverter(
            TaxonomyRepo taxonomyRepo, Map<String, SuggestDocument> suggestDocuments) {
        this.taxonomyRepo = taxonomyRepo;
        this.suggestions = suggestDocuments;
    }

    void convertOrganism(Organism organism, UniProtDocument document) {
        if (organism != null) {
            int taxonomyId = Math.toIntExact(organism.getTaxonId());
            document.organismTaxId = taxonomyId;
            if (taxonomyRepo != null) {
                Optional<TaxonomicNode> taxonomicNode =
                        taxonomyRepo.retrieveNodeUsingTaxID(taxonomyId);
                if (taxonomicNode.isPresent()) {

                    TaxonomicNode node = taxonomicNode.get();
                    List<String> extractedTaxoNode = TaxonomyRepoUtil.extractTaxonFromNode(node);
                    document.organismName.addAll(extractedTaxoNode);
                    document.organismSort =
                            UniProtEntryConverterUtil.truncatedSortValue(
                                    String.join(" ", extractedTaxoNode));

                    String modelOrgamism = MODEL_ORGANISMS_TAX_NAME.get(taxonomyId);
                    if (modelOrgamism != null) {
                        document.modelOrganism = modelOrgamism;
                    } else {
                        document.otherOrganism = node.scientificName();
                    }
                    addTaxonSuggestions(SuggestDictionary.ORGANISM, taxonomyId, extractedTaxoNode);
                }
            }
            convertLineageTaxon(taxonomyId, document);
        }
    }

    void convertOrganismHosts(List<OrganismHost> hosts, UniProtDocument document) {
        hosts.forEach(
                host -> {
                    int taxonomyId = Math.toIntExact(host.getTaxonId());
                    document.organismHostIds.add(taxonomyId);
                    if (taxonomyRepo != null) {
                        Optional<TaxonomicNode> taxonomicNode =
                                taxonomyRepo.retrieveNodeUsingTaxID(taxonomyId);
                        if (taxonomicNode.isPresent()) {
                            TaxonomicNode node = taxonomicNode.get();
                            List<String> extractedTaxoNode =
                                    TaxonomyRepoUtil.extractTaxonFromNode(node);
                            document.organismHostNames.addAll(extractedTaxoNode);
                            addTaxonSuggestions(
                                    SuggestDictionary.HOST, taxonomyId, extractedTaxoNode);
                        }
                    }
                });

//        document.content.addAll(document.organismHostNames);
//        document.content.addAll(
//                document.organismHostIds.stream()
//                        .map(String::valueOf)
//                        .collect(Collectors.toList()));
    }

    private void convertLineageTaxon(int taxId, UniProtDocument document) {
        if (taxId > 0 && taxonomyRepo != null) {
            List<TaxonomicNode> nodes = TaxonomyRepoUtil.getTaxonomyLineage(taxonomyRepo, taxId);
            nodes.forEach(
                    node -> {
                        int id = node.id();
                        document.taxLineageIds.add(id);
                        if (!node.scientificName().equals("root")) {
                            List<String> taxons = TaxonomyRepoUtil.extractTaxonFromNodeNoMnemonic(node);
                            // not index mnemonic for taxonomy_name
                            document.organismTaxon.addAll(taxons);
                            addTaxonSuggestions(SuggestDictionary.TAXONOMY, id, taxons);
                        }
                    });
        }
//        document.content.addAll(document.organismTaxon);
//        document.content.addAll(
//                document.taxLineageIds.stream().map(String::valueOf).collect(Collectors.toList()));
    }

    private void addTaxonSuggestions(SuggestDictionary dicType, int id, List<String> taxons) {
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
                SuggestDocument.SuggestDocumentBuilder documentBuilder =
                        SuggestDocument.builder()
                                .id(idStr)
                                .dictionary(dicType.name())
                                .value(taxonIterator.next());
                while (taxonIterator.hasNext()) {
                    documentBuilder.altValue(taxonIterator.next());
                }
                suggestions.put(key, documentBuilder.build());
            }
        }
    }
}
