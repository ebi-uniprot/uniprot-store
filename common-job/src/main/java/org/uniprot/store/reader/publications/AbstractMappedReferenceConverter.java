package org.uniprot.store.reader.publications;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.uniprot.core.publication.MappedReference;
import org.uniprot.core.util.Utils;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Created 02/12/2020
 *
 * @author Edd
 */
abstract class AbstractMappedReferenceConverter<T extends MappedReference>
        implements MappedReferenceConverter<T> {
    private static final Pattern CATEGORY_PATTERN = Pattern.compile("^(\\[.*])(.*)");

    @Override
    public T convert(String line) {
        String[] lineFields = line.split("\t");
        RawMappedReference rawMappedReference = new RawMappedReference();
        if (lineFields.length >= 4) {
            Set<String> categories = new HashSet<>();
            String rawAnnotation = "";
            if (lineFields.length >= 5) {
                Matcher matcher = CATEGORY_PATTERN.matcher(lineFields[4]);
                if (matcher.matches()) { // split categories from the rest of the text...
                    String matchedCategories = matcher.group(1);
                    categories.addAll(getCategories(matchedCategories));
                    rawAnnotation = lineFields[4].substring(matchedCategories.length());
                } else {
                    rawAnnotation = lineFields[4];
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
            throw new RawMappedReferenceException("Could not parse mapped references file: "+line);
        }
    }

    abstract T convertRawMappedReference(RawMappedReference reference);

    private List<String> getCategories(String matchedCategories) {
        String[] categoriesArray = matchedCategories.split("]");
        return Arrays.stream(categoriesArray)
                .map(category -> category.substring(1))
                .collect(Collectors.toList());
    }
}
