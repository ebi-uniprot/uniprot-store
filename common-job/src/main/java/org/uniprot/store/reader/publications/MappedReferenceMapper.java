package org.uniprot.store.reader.publications;

import org.uniprot.core.publication.MappedReference;

/**
 * Created 02/12/2020
 *
 * @author Edd
 */
public interface MappedReferenceMapper<T extends MappedReference> {
    T convert(String line);
}