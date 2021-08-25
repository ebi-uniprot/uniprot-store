package org.uniprot.store.indexer.common.utils;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.uniprot.core.uniprotkb.description.EC;
import org.uniprot.core.uniprotkb.description.ProteinDescription;
import org.uniprot.core.uniprotkb.description.ProteinName;
import org.uniprot.core.uniprotkb.description.ProteinSection;
import org.uniprot.core.uniprotkb.description.ProteinSubName;

/**
 * @author sahmad
 * @created 09/08/2021
 */
public class UniProtAARuleUtils {
    @SuppressWarnings("squid:S5852")
    private static final Pattern PATTERN_FAMILY =
            Pattern.compile(
                    "(?:In the .+? section; )?[Bb]elongs to the (.+?family)\\.(?: (.+?family)\\.)?(?: (.+?family)\\.)?(?: Highly divergent\\.)?");

    private UniProtAARuleUtils() {}

    public static List<String> extractProteinDescriptionEcs(ProteinDescription proteinDescription) {
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

    public static String extractFamily(String text) {
        if (!text.endsWith(".")) {
            text += ".";
        }
        StringBuilder line = new StringBuilder();
        Matcher m = PATTERN_FAMILY.matcher(text);
        if (m.matches()) {
            line.append(m.group(1));
            if (m.group(2) != null) line.append(", ").append(m.group(2));
            if (m.group(3) != null) line.append(", ").append(m.group(3));
        }
        return line.toString();
    }

    private static List<String> getProteinSectionEcs(ProteinSection proteinSection) {
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

    private static List<String> getEcs(List<EC> ecs) {
        return ecs.stream().map(EC::getValue).collect(Collectors.toList());
    }
}
