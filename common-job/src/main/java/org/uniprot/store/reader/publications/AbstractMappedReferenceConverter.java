package org.uniprot.store.reader.publications;

import java.util.HashSet;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.uniprot.core.publication.MappedReference;
import org.uniprot.core.util.Utils;

/**
 * Created 02/12/2020
 *
 * @author Edd
 */
abstract class AbstractMappedReferenceConverter<T extends MappedReference>
        implements MappedReferenceConverter<T> {
    private static final Pattern CATEGORY_PATTERN = Pattern.compile("\\[(.*?)]");
    private static final Set<String> CATEGORIES = new HashSet<>();

    static {
        CATEGORIES.add("Expression");
        CATEGORIES.add("Family & Domains");
        CATEGORIES.add("Function");
        CATEGORIES.add("Interaction");
        CATEGORIES.add("Names");
        CATEGORIES.add("Pathology & Biotech");
        CATEGORIES.add("PTM / Processing");
        CATEGORIES.add("Sequences");
        CATEGORIES.add("Subcellular Location");
        CATEGORIES.add("Structure");
    }

    static int injectCategories(String linePart, Set<String> categories) {
        Matcher matcher = CATEGORY_PATTERN.matcher(linePart);
        int prevCatEnd = 0;
        while (matcher.find() && prevCatEnd == matcher.start()) {
            String category = matcher.group(1);
            if (isValidateCategory(category)) {
                categories.add(category);
            }

            prevCatEnd = matcher.end();
        }

        return prevCatEnd;
    }

    @Override
    public T convert(String line) {
        String[] lineFields = line.split("\t");
        RawMappedReference rawMappedReference = new RawMappedReference();
        if (lineFields.length >= 4) {
            Set<String> categories = new HashSet<>();
            String rawAnnotation = "";
            if (lineFields.length >= 5) {
                // first, contiguous [asdf][asdf][asdf] => categories
                if (Utils.notNullNotEmpty(lineFields[4])) {
                    int annotationStartPos = injectCategories(lineFields[4], categories);
                    rawAnnotation = lineFields[4].substring(annotationStartPos).trim();
                    if (rawAnnotation.equals(".")) {
                        rawAnnotation = null;
                    }
                }
            }

            if (!Utils.notNullNotEmpty(lineFields[0])) {
                throw new RawMappedReferenceException("Missing accession");
            }
            if (!Utils.notNullNotEmpty(lineFields[2])) {
                throw new RawMappedReferenceException("Missing reference ID");
            }

            rawMappedReference.accession = lineFields[0];
            rawMappedReference.source = lineFields[1];
            rawMappedReference.sourceId = lineFields[3];
            rawMappedReference.pubMedId = lineFields[2];
            rawMappedReference.categories = categories;
            rawMappedReference.annotation = rawAnnotation;

            return convertRawMappedReference(rawMappedReference);
        } else {
            throw new RawMappedReferenceException(
                    "Could not parse mapped references file: " + line);
        }
    }

    abstract T convertRawMappedReference(RawMappedReference reference);

    private static boolean isValidateCategory(String category) {
        return CATEGORIES.contains(category);
    }
}
