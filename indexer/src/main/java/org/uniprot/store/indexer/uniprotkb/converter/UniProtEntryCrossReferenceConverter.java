package org.uniprot.store.indexer.uniprotkb.converter;

import static java.util.Arrays.asList;
import static org.uniprot.cv.go.RelationshipType.IS_A;
import static org.uniprot.cv.go.RelationshipType.PART_OF;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.uniprot.core.Property;
import org.uniprot.core.cv.go.GeneOntologyEntry;
import org.uniprot.core.uniprotkb.xdb.UniProtKBCrossReference;
import org.uniprot.cv.go.GORepo;
import org.uniprot.store.search.document.suggest.SuggestDictionary;
import org.uniprot.store.search.document.suggest.SuggestDocument;
import org.uniprot.store.search.document.uniprot.ProteinsWith;
import org.uniprot.store.search.document.uniprot.UniProtDocument;

/**
 * @author lgonzales
 * @since 2019-09-04
 */
class UniProtEntryCrossReferenceConverter {

    private static final String GO = "go_";
    private static final String XREF_COUNT = "xref_count_";
    private final GORepo goRepo;
    private final Map<String, SuggestDocument> suggestions;

    UniProtEntryCrossReferenceConverter(
            GORepo goRepo, Map<String, SuggestDocument> suggestDocuments) {
        this.goRepo = goRepo;
        this.suggestions = suggestDocuments;
    }

    void convertCrossReferences(
            List<UniProtKBCrossReference> references, UniProtDocument document) {
        convertXref(references, document);
        convertXrefCount(references, document);
    }

    private void convertXref(List<UniProtKBCrossReference> references, UniProtDocument document) {
        boolean d3structure = false;
        for (UniProtKBCrossReference xref : references) {
            if (xref.getDatabase().getName().equalsIgnoreCase("PDB")) {
                d3structure = true;
            }
            String dbname = xref.getDatabase().getName().toLowerCase();
            document.databases.add(dbname);
            String id = xref.getId();
            if (!dbname.equalsIgnoreCase("PIR")) {
                convertXRefId(document, dbname, id);
            }
            switch (dbname.toLowerCase()) {
                case "embl":
                    convertEmbl(document, xref, dbname);
                    break;
                case "refseq":
                case "pir":
                case "unipathway":
                case "ensembl":
                    if (xref.hasProperties()) {
                        List<String> properties =
                                xref.getProperties().stream()
                                        .filter(
                                                property ->
                                                        !property.getValue().equalsIgnoreCase("-"))
                                        .map(Property::getValue)
                                        .collect(Collectors.toList());
                        properties.forEach(s -> convertXRefId(document, dbname, s));
                    }
                    break;
                case "proteomes":
                    convertProteomes(document, xref);
                    break;
                case "go":
                    convertGoTerm(xref, document);
                    break;
                default:
            }
        }
        document.d3structure = d3structure;
        if (d3structure && !document.proteinsWith.contains(ProteinsWith.D3_STRUCTURE.getValue())) {
            document.proteinsWith.add(ProteinsWith.D3_STRUCTURE.getValue());
        }
    }

    private void convertProteomes(UniProtDocument document, UniProtKBCrossReference xref) {
        document.proteomes.add(xref.getId());
        if (xref.hasProperties()) {
            document.proteomeComponents.addAll(
                    xref.getProperties().stream()
                            .map(Property::getValue)
                            .collect(Collectors.toSet()));
        }
    }

    private void convertEmbl(UniProtDocument document, UniProtKBCrossReference xref, String dbname) {
        if (xref.hasProperties()) {
            Optional<String> proteinId =
                    xref.getProperties().stream()
                            .filter(
                                    property ->
                                            property.getKey()
                                                    .equalsIgnoreCase("ProteinId"))
                            .filter(
                                    property ->
                                            !property.getValue().equalsIgnoreCase("-"))
                            .map(Property::getValue)
                            .findFirst();
            proteinId.ifPresent(s -> convertXRefId(document, dbname, s));
        }
    }

    private void convertXRefId(UniProtDocument document, String dbname, String s) {
        List<String> xrefIds = UniProtEntryConverterUtil.getXrefId(s, dbname);
        document.crossRefs.addAll(xrefIds);
    }

    private void convertXrefCount(
            List<UniProtKBCrossReference> references, UniProtDocument document) {
        document.xrefCountMap =
                references.stream()
                        .map(val -> XREF_COUNT + val.getDatabase().getName().toLowerCase())
                        .collect(Collectors.groupingBy(Function.identity(), Collectors.counting()));
    }

    private void convertGoTerm(UniProtKBCrossReference go, UniProtDocument document) {
        String goTerm =
                go.getProperties().stream()
                        .filter(property -> property.getKey().equalsIgnoreCase("GoTerm"))
                        .map(property -> property.getValue().split(":")[1])
                        .collect(Collectors.joining());
        String evType =
                go.getProperties().stream()
                        .filter(property -> property.getKey().equalsIgnoreCase("GoEvidenceType"))
                        .map(property -> property.getValue().split(":")[0].toLowerCase())
                        .collect(Collectors.joining());

        // For default searches, GO ID is covered by document.xrefs. But we still need to add go
        // term to content for default searches.
        document.content.add(goTerm);

        // add go id and term to specific doc fields for advanced search
        addGoterm(evType, go.getId(), goTerm, document);

        addAncestors(evType, go.getId(), document);
    }

    private void addAncestors(String evType, String goTerm, UniProtDocument doc) {
        Set<GeneOntologyEntry> ancestors = goRepo.getAncestors(goTerm, asList(IS_A, PART_OF));
        if (ancestors != null)
            ancestors.forEach(
                    ancestor -> addGoterm(evType, ancestor.getId(), ancestor.getName(), doc));
    }

    private void addGoterm(String evType, String goId, String term, UniProtDocument document) {
        String key = GO + evType;
        Collection<String> values =
                document.goWithEvidenceMaps.computeIfAbsent(key, k -> new HashSet<>());
        String idOnly = goId.substring(3);
        values.add(idOnly);
        values.add(term);

        document.goes.add(idOnly);
        document.goes.add(term);
        document.goIds.add(idOnly);

        suggestions.putIfAbsent(
                UniProtEntryConverterUtil.createSuggestionMapKey(SuggestDictionary.GO, idOnly),
                SuggestDocument.builder()
                        .id(idOnly)
                        .value(term)
                        .dictionary(SuggestDictionary.GO.name())
                        .build());
    }
}
