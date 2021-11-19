package org.uniprot.store.spark.indexer.uniprot.converter;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.uniprot.core.Property;
import org.uniprot.core.uniprotkb.xdb.UniProtKBCrossReference;
import org.uniprot.store.search.document.uniprot.ProteinsWith;
import org.uniprot.store.search.document.uniprot.UniProtDocument;

/**
 * @author lgonzales
 * @since 2019-09-04
 */
class UniProtEntryCrossReferenceConverter {

    private static final String GO = "go_";
    private static final String XREF_COUNT = "xref_count_";

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

            if (!"go".equalsIgnoreCase(dbname) && xref.hasProperties()){
                List<String> properties = getCrossRefPropertiesValues(xref);
                document.content.addAll(properties);
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
                        proteinId.ifPresent(s -> convertXRefId(document, dbname, s));
                    }
                    break;
                case "refseq":
                case "pir":
                case "unipathway":
                case "ensembl":
                    if (xref.hasProperties()) {
                        List<String> properties = getCrossRefPropertiesValues(xref);
                        properties.forEach(s -> convertXRefId(document, dbname, s));
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
        if (d3structure && !document.proteinsWith.contains(ProteinsWith.D3_STRUCTURE.getValue())) {
            document.proteinsWith.add(ProteinsWith.D3_STRUCTURE.getValue());
        }
    }

    private List<String> getCrossRefPropertiesValues(UniProtKBCrossReference xref) {
        return xref.getProperties().stream()
                .filter(
                        property ->
                                !property.getValue().equalsIgnoreCase("-"))
                .map(Property::getValue)
                .collect(Collectors.toList());
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
        List<String> evNames =
                go.getProperties().stream()
                        .filter(property -> property.getKey().equalsIgnoreCase("GoEvidenceType"))
                        .map(property -> property.getValue().split(":")[1])
                        .collect(Collectors.toList());

        // For default searches, GO ID is covered by document.xrefs. But we still need to add go
        // term to content for default searches.
        document.content.add(goTerm);
        document.content.addAll(evNames);// add evidence description

        // add go id and term to specific doc fields for advanced search
        addGoterm(evType, go.getId(), goTerm, document);
    }

    private void addGoterm(String evType, String goId, String term, UniProtDocument document) {
        String key = GO + evType;
        Collection<String> values =
                document.goWithEvidenceMaps.computeIfAbsent(key, k -> new HashSet<>());
        String idOnly = goId.substring(3).trim();
        values.add(idOnly);
        values.add(term);

        document.goes.add(idOnly);
        document.goes.add(term);
        document.goIds.add(idOnly);
    }
}
