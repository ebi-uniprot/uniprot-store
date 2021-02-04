package org.uniprot.store.spark.indexer.publication.mapper;

import org.apache.spark.api.java.function.Function;
import org.uniprot.core.publication.MappedReference;
import org.uniprot.store.reader.publications.ComputationallyMappedReferenceConverter;

/**
 * Converts a String to a {@link ComputationallyMappedReferenceConverter}.
 *
 * <p>Created 19/01/2021
 *
 * @author Edd
 */
public class ComputationallyMappedReferenceMapper implements Function<String, MappedReference> {
    private static final long serialVersionUID = -6077802133508264113L;

    @Override
    public MappedReference call(String rawReference) throws Exception {
        ComputationallyMappedReferenceConverter converter =
                new ComputationallyMappedReferenceConverter();
        return converter.convert(rawReference);
    }
}
