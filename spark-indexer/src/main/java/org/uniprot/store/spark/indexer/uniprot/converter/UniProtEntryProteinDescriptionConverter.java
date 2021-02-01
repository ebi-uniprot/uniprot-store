package org.uniprot.store.spark.indexer.uniprot.converter;

import static org.uniprot.store.spark.indexer.uniprot.converter.UniProtEntryConverterUtil.truncatedSortValue;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.uniprot.core.Value;
import org.uniprot.core.uniprotkb.description.*;
import org.uniprot.core.util.Utils;
import org.uniprot.store.search.document.uniprot.UniProtDocument;

/**
 * @author lgonzales
 * @since 2019-09-05
 */
public class UniProtEntryProteinDescriptionConverter {

    void convertProteinDescription(
            ProteinDescription proteinDescription, UniProtDocument document) {
        if (Utils.notNull(proteinDescription)) {
            List<String> names = extractProteinDescriptionValues(proteinDescription);
            document.proteinNames.addAll(names);
            document.proteinsNamesSort = truncatedSortValue(String.join(" ", names));

            convertECNumbers(proteinDescription, document);
            convertFragmentNPrecursor(proteinDescription, document);
        }
    }

    private void convertECNumbers(ProteinDescription proteinDescription, UniProtDocument document) {
        document.ecNumbers = extractProteinDescriptionEcs(proteinDescription);
        document.ecNumbersExact = document.ecNumbers;
    }

    private void convertFragmentNPrecursor(
            ProteinDescription proteinDescription, UniProtDocument document) {
        boolean isFragment = false;
        boolean isPrecursor = false;
        if (proteinDescription.hasFlag()) {
            Flag flag = proteinDescription.getFlag();
            switch (flag.getType()) {
                case FRAGMENT:
                case FRAGMENTS:
                    isFragment = true;
                    break;
                case PRECURSOR:
                    isPrecursor = true;
                    break;
                case FRAGMENT_PRECURSOR:
                case FRAGMENTS_PRECURSOR:
                    isPrecursor = true;
                    isFragment = true;
                    break;
            }
        }
        document.fragment = isFragment;
        document.precursor = isPrecursor;
    }

    private List<String> getEcs(List<EC> ecs) {
        return ecs.stream().map(EC::getValue).collect(Collectors.toList());
    }

    private List<String> extractProteinDescriptionValues(ProteinDescription description) {
        List<String> values = new ArrayList<>();
        if (description.hasRecommendedName()) {
            values.addAll(getProteinNameNames(description.getRecommendedName()));
        }
        if (description.hasSubmissionNames()) {
            description.getSubmissionNames().stream()
                    .map(this::getProteinSubNameNames)
                    .forEach(values::addAll);
        }
        if (description.hasAlternativeNames()) {
            description.getAlternativeNames().stream()
                    .map(this::getProteinNameNames)
                    .forEach(values::addAll);
        }
        if (description.hasContains()) {
            description.getContains().stream()
                    .map(this::getProteinSectionValues)
                    .forEach(values::addAll);
        }
        if (description.hasIncludes()) {
            description.getIncludes().stream()
                    .map(this::getProteinSectionValues)
                    .forEach(values::addAll);
        }
        if (description.hasAllergenName()) {
            values.add(description.getAllergenName().getValue());
        }
        if (description.hasBiotechName()) {
            values.add(description.getBiotechName().getValue());
        }
        if (description.hasCdAntigenNames()) {
            description.getCdAntigenNames().stream().map(Value::getValue).forEach(values::add);
        }
        if (description.hasInnNames()) {
            description.getInnNames().stream().map(Value::getValue).forEach(values::add);
        }
        return values;
    }

    private List<String> getProteinSectionValues(ProteinSection proteinSection) {
        List<String> names = new ArrayList<>();
        if (proteinSection.hasRecommendedName()) {
            names.addAll(getProteinNameNames(proteinSection.getRecommendedName()));
        }
        if (proteinSection.hasAlternativeNames()) {
            proteinSection.getAlternativeNames().stream()
                    .map(this::getProteinNameNames)
                    .forEach(names::addAll);
        }
        if (proteinSection.hasCdAntigenNames()) {
            proteinSection.getCdAntigenNames().stream().map(Value::getValue).forEach(names::add);
        }
        if (proteinSection.hasAllergenName()) {
            names.add(proteinSection.getAllergenName().getValue());
        }
        if (proteinSection.hasInnNames()) {
            proteinSection.getInnNames().stream().map(Value::getValue).forEach(names::add);
        }
        if (proteinSection.hasBiotechName()) {
            names.add(proteinSection.getBiotechName().getValue());
        }
        return names;
    }

    private List<String> getProteinNameNames(ProteinName proteinRecName) {
        List<String> names = new ArrayList<>();
        if (proteinRecName.hasFullName()) {
            names.add(proteinRecName.getFullName().getValue());
        }
        if (proteinRecName.hasShortNames()) {
            proteinRecName.getShortNames().stream().map(Name::getValue).forEach(names::add);
        }
        return names;
    }

    private List<String> getProteinSubNameNames(ProteinSubName proteinAltName) {
        List<String> names = new ArrayList<>();
        if (proteinAltName.hasFullName()) {
            names.add(proteinAltName.getFullName().getValue());
        }
        return names;
    }

    public List<String> extractProteinDescriptionEcs(ProteinDescription proteinDescription) {
        List<String> ecs = new ArrayList<>();
        if (proteinDescription.hasRecommendedName()
                && proteinDescription.getRecommendedName().hasEcNumbers()) {
            ecs.addAll(getEcs(proteinDescription.getRecommendedName().getEcNumbers()));
        }
        if (proteinDescription.hasSubmissionNames()) {
            proteinDescription.getSubmissionNames().stream()
                    .filter(ProteinSubName::hasEcNumbers)
                    .flatMap(proteinSubName -> getEcs(proteinSubName.getEcNumbers()).stream())
                    .forEach(ecs::add);
        }
        if (proteinDescription.hasAlternativeNames()) {
            proteinDescription.getAlternativeNames().stream()
                    .filter(ProteinName::hasEcNumbers)
                    .flatMap(proteinAltName -> getEcs(proteinAltName.getEcNumbers()).stream())
                    .forEach(ecs::add);
        }
        if (proteinDescription.hasContains()) {
            proteinDescription.getContains().stream()
                    .flatMap(proteinSection -> getProteinSectionEcs(proteinSection).stream())
                    .forEach(ecs::add);
        }
        if (proteinDescription.hasIncludes()) {
            proteinDescription.getIncludes().stream()
                    .flatMap(proteinSection -> getProteinSectionEcs(proteinSection).stream())
                    .forEach(ecs::add);
        }
        return ecs;
    }

    private List<String> getProteinSectionEcs(ProteinSection proteinSection) {
        List<String> ecs = new ArrayList<>();
        if (proteinSection.hasRecommendedName()
                && proteinSection.getRecommendedName().hasEcNumbers()) {
            ecs.addAll(getEcs(proteinSection.getRecommendedName().getEcNumbers()));
        }
        if (proteinSection.hasAlternativeNames()) {
            proteinSection.getAlternativeNames().stream()
                    .filter(ProteinName::hasEcNumbers)
                    .flatMap(proteinAltName -> getEcs(proteinAltName.getEcNumbers()).stream())
                    .forEach(ecs::add);
        }
        return ecs;
    }
}
