package org.uniprot.store.reader.publications;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

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
                    int annotationStartPos = injectCategories(lineFields[4] ,categories);
                    rawAnnotation = lineFields[4].substring(annotationStartPos);
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

    static int injectCategories(String linePart, Set<String> categories) {
        Matcher matcher = CATEGORY_PATTERN.matcher(linePart);
        int prevCatEnd = 0;
        while (matcher.find() && prevCatEnd == matcher.start()) {
            categories.add(matcher.group(1));
            prevCatEnd = matcher.end();
        }

        return prevCatEnd;
    }

    abstract T convertRawMappedReference(RawMappedReference reference);

    private List<String> getCategories(String matchedCategories) {
        String[] categoriesArray = matchedCategories.split("]");
        return Arrays.stream(categoriesArray)
                .map(category -> category.substring(1))
                .collect(Collectors.toList());
    }
}
