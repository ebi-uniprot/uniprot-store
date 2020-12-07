package org.uniprot.store.reader.publications;

import org.uniprot.core.publication.MappedReference;

/**
 * Responsible for converting a {@link RawMappedReference} into a {@link MappedReference}.
 *
 * <p>Created
 *
 * <p>02/12/2020
 *
 * @author Edd
 */
public interface MappedReferenceConverter<T extends MappedReference> {
    T convert(String line);
}
