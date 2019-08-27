package org.uniprot.store.job.common.converter;

import org.uniprot.store.job.common.DocumentConversionException;
import org.uniprot.store.search.document.Document;

/**
 * Convert from the source object to ONE type of the Document.
 *
 * @param <S> the type of the source object.
 * @param <T> the type of the document that is been generated.
 */
public interface DocumentConverter<S, T extends Document> {

    /**
     * It is necessary to return a list rather than just a single object. For example, if we want to index Feature
     * from a UniProtEntry object, a list of features document will need to be returned for that UniProtEntryObject.
     *
     * @param source the source object
     * @return the list of document that is generated from the source object.
     * @throws DocumentConversionException is thrown when a mapping error occurs between the source being converted,
     *                                     and the document
     */
    T convert(S source);
}
