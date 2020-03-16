package org.uniprot.store.indexer.uniprotkb.converter;

import static java.util.Arrays.asList;
import static org.uniprot.store.indexer.uniprot.go.GoRelationFileRepo.Relationship.IS_A;
import static org.uniprot.store.indexer.uniprot.go.GoRelationFileRepo.Relationship.PART_OF;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.uniprot.core.Property;
import org.uniprot.core.cv.go.GeneOntologyEntry;
import org.uniprot.core.uniprotkb.xdb.UniProtkbCrossReference;
import org.uniprot.store.indexer.uniprot.go.GoRelationRepo;
import org.uniprot.store.search.document.suggest.SuggestDictionary;
import org.uniprot.store.search.document.suggest.SuggestDocument;
import org.uniprot.store.search.document.uniprot.UniProtDocument;

/**
 * @author lgonzales
 * @since 2019-09-04
 */
class UniProtEntryCrossReferenceConverter {

    private static final String GO = "go_";
    private static final String XREF_COUNT = "xref_count_";
    private final GoRelationRepo goRelationRepo;
    private final Map<String, SuggestDocument> suggestions;

    UniProtEntryCrossReferenceConverter(
            GoRelationRepo goRelationRepo, Map<String, SuggestDocument> suggestDocuments) {
        this.goRelationRepo = goRelationRepo;
        this.suggestions = suggestDocuments;
    }

    void convertCrossReferences(
            List<UniProtkbCrossReference> references, UniProtDocument document) {
        convertXref(references, document);
        convertXrefCount(references, document);
    }

    private void convertXref(List<UniProtkbCrossReference> references, UniProtDocument document) {
        boolean d3structure = false;
        for (UniProtkbCrossReference xref : references) {
            if (xref.getDatabase().getName().equalsIgnoreCase("PDB")) {
                d3structure = true;
            }
            String dbname = xref.getDatabase().getName().toLowerCase();
            document.databases.add(dbname);
            document.content.add(dbname);
            String id = xref.getId();
            if (!dbname.equalsIgnoreCase("PIR")) {
                convertXRefId(document, dbname, id);
            }
            switch (dbname.toLowerCase()) {
                case "embl":
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
                        proteinId.ifPresent(
                                s -> {
                                    convertXRefId(document, dbname, s);
                                });
                    }
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
                        properties.forEach(
                                s -> {
                                    convertXRefId(document, dbname, s);
                                });
                    }
                    break;
                case "proteomes":
                    document.proteomes.add(xref.getId());
                    if (xref.hasProperties()) {
                        document.proteomeComponents.addAll(
                                xref.getProperties().stream()
                                        .map(Property::getValue)
                                        .collect(Collectors.toSet()));
                    }
                    break;
                case "go":
                    convertGoTerm(xref, document);
                    break;
                default:
            }
        }
        document.d3structure = d3structure;
        if (d3structure) {
            document.proteinsWith.add("3dstructure");
        }
    }

    private void convertXRefId(UniProtDocument document, String dbname, String s) {
        List<String> xrefIds = UniProtEntryConverterUtil.getXrefId(s, dbname);
        document.xrefs.addAll(xrefIds);
        document.content.addAll(xrefIds);
    }

    private void convertXrefCount(
            List<UniProtkbCrossReference> references, UniProtDocument document) {
        document.xrefCountMap =
                references.stream()
                        .map(val -> XREF_COUNT + val.getDatabase().getName().toLowerCase())
                        .collect(Collectors.groupingBy(Function.identity(), Collectors.counting()));
    }

    private void convertGoTerm(UniProtkbCrossReference go, UniProtDocument document) {
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

        addGoterm(evType, go.getId(), goTerm, document);
        addAncestors(evType, go.getId(), document);
        document.content.add(go.getId().substring(3)); // id
        document.content.add(goTerm); // term
    }

    private void addAncestors(String evType, String goTerm, UniProtDocument doc) {
        Set<GeneOntologyEntry> ancestors =
                goRelationRepo.getAncestors(goTerm, asList(IS_A, PART_OF));
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
