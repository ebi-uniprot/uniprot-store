package org.uniprot.store.spark.indexer.publication.mapper;

import org.apache.spark.api.java.function.Function;
import org.uniprot.core.publication.CommunityMappedReference;
import org.uniprot.core.publication.MappedReference;
import org.uniprot.store.reader.publications.CommunityMappedReferenceConverter;

/**
 * Converts a String to a {@link CommunityMappedReference}.
 *
 * <p>Created 19/01/2021
 *
 * @author Edd
 */
public class CommunityMappedReferenceMapper implements Function<String, MappedReference> {
    private static final long serialVersionUID = 951542848775174716L;

    @Override
    public MappedReference call(String rawReference) throws Exception {
        CommunityMappedReferenceConverter converter = new CommunityMappedReferenceConverter();
        return converter.convert(rawReference);
    }
}
