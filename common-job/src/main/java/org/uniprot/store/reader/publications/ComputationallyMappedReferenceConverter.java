package org.uniprot.store.reader.publications;

import org.uniprot.core.publication.ComputationallyMappedReference;
import org.uniprot.core.publication.impl.ComputationallyMappedReferenceBuilder;
import org.uniprot.core.publication.impl.MappedSourceBuilder;

/**
 * Created 02/12/2020
 *
 * @author Edd
 */
public class ComputationallyMappedReferenceConverter
        extends AbstractMappedReferenceConverter<ComputationallyMappedReference> {

    @Override
    ComputationallyMappedReference convertRawMappedReference(RawMappedReference reference) {
        return new ComputationallyMappedReferenceBuilder()
                .uniProtKBAccession(reference.accession)
                .source(
                        new MappedSourceBuilder()
                                .name(reference.source)
                                .id(reference.sourceId)
                                .build())
                .pubMedId(reference.pubMedId)
                .sourceCategoriesSet(reference.categories)
                .annotation(reference.annotation)
                .build();
    }
}
