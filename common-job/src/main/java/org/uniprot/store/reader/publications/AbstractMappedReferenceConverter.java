package org.uniprot.store.reader.publications;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
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

    private final Iterator<String> lines;
    private T nextMappedRef;

    public AbstractMappedReferenceConverter(String filePath) throws IOException {
        lines = Files.lines(Paths.get(filePath)).iterator();
    }

    public List<T> getNext() {
        List<T> mappedReferences = null;
        T currentMappedRef = null;
        if (Objects.nonNull(this.nextMappedRef)) {
            currentMappedRef = this.nextMappedRef;
            mappedReferences = new ArrayList<>();
            mappedReferences.add(currentMappedRef);
            this.nextMappedRef = null;
        } else if (this.lines.hasNext()) {
            currentMappedRef = convert(this.lines.next());
            mappedReferences = new ArrayList<>();
            mappedReferences.add(currentMappedRef);
        }

        while (this.lines.hasNext()) {
            this.nextMappedRef = convert(this.lines.next());
            if (isAccessionPubMedIdPairEqual(currentMappedRef, this.nextMappedRef)) {
                currentMappedRef = this.nextMappedRef;
                mappedReferences.add(currentMappedRef);
            } else {
                break;
            }
        }
        return mappedReferences;
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

    private boolean isAccessionPubMedIdPairEqual(T currentMappedRef, T nextMappedRef) {
        return currentMappedRef
                        .getUniProtKBAccession()
                        .getValue()
                        .equals(nextMappedRef.getUniProtKBAccession().getValue())
                && currentMappedRef.getPubMedId().equals(nextMappedRef.getPubMedId());
    }
}
