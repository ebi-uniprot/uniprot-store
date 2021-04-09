package org.uniprot.store.reader.publications;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

import org.uniprot.core.publication.MappedReference;

/**
 * This class is responsible for reading a list of entries with same accession and pubmed id.
 *
 * @author sahmad
 * @since 21/01/2021
 */
public class MappedReferenceReader<T extends MappedReference> {
    private final Iterator<String> lines;
    private T nextMappedRef;
    private final MappedReferenceConverter<T> mappedReferenceConverter;

    @SuppressWarnings("squid:S2095")
    public MappedReferenceReader(
            MappedReferenceConverter<T> mappedReferenceConverter, String filePath)
            throws IOException {
        this.mappedReferenceConverter = mappedReferenceConverter;
        this.lines = Files.lines(Paths.get(filePath)).iterator();
    }

    public List<T> readNext() {
        List<T> mappedReferences = null;
        T currentMappedRef = null;
        if (Objects.nonNull(this.nextMappedRef)) {
            currentMappedRef = this.nextMappedRef;
            mappedReferences = new ArrayList<>();
            mappedReferences.add(currentMappedRef);
            this.nextMappedRef = null;
        } else if (this.lines.hasNext()) {
            currentMappedRef = this.mappedReferenceConverter.convert(this.lines.next());
            mappedReferences = new ArrayList<>();
            mappedReferences.add(currentMappedRef);
        }

        while (this.lines.hasNext()) {
            this.nextMappedRef = this.mappedReferenceConverter.convert(this.lines.next());
            // keep adding to the list as long as accession-pubmed pair is same
            if (Objects.nonNull(currentMappedRef)
                    && isAccessionPubMedIdPairEqual(currentMappedRef, this.nextMappedRef)) {
                currentMappedRef = this.nextMappedRef;
                mappedReferences.add(currentMappedRef);
            } else {
                break;
            }
        }
        return mappedReferences;
    }

    private boolean isAccessionPubMedIdPairEqual(T currentMappedRef, T nextMappedRef) {
        return currentMappedRef != null
                && currentMappedRef
                        .getUniProtKBAccession()
                        .getValue()
                        .equals(nextMappedRef.getUniProtKBAccession().getValue())
                && currentMappedRef.getCitationId().equals(nextMappedRef.getCitationId());
    }
}
