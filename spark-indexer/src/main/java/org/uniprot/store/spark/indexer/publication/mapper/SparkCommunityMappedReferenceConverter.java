package org.uniprot.store.spark.indexer.publication.mapper;

import org.apache.spark.api.java.function.Function;
import org.uniprot.core.publication.MappedReference;
import org.uniprot.store.reader.publications.CommunityMappedReferenceConverter;

/**
 * Created 19/01/2021
 *
 * @author Edd
 */
public class SparkCommunityMappedReferenceConverter implements Function<String, MappedReference> {
    @Override
    public MappedReference call(String rawReference) throws Exception {
        CommunityMappedReferenceConverter converter = new CommunityMappedReferenceConverter();
        return converter.convert(rawReference);
    }
}
