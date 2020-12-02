package org.uniprot.store.reader.publications;

import org.uniprot.core.publication.ComputationallyMappedReference;
import org.uniprot.core.publication.impl.ComputationallyMappedReferenceBuilder;

/**
 * Created 02/12/2020
 *
 * @author Edd
 */
public class ComputationallyMappedReferenceMapper
        extends AbstractMappedReferenceMapper<ComputationallyMappedReference> {
    @Override
    ComputationallyMappedReference convertRawMappedReference(RawMappedReference reference) {
        return new ComputationallyMappedReferenceBuilder()
                .uniProtKBAccession(reference.accession)
                .source(reference.source)
                .sourceId(reference.sourceId)
                .pubMedId(reference.pubMedId)
                .sourceCategoriesSet(reference.categories)
                .annotation(reference.annotation)
                .build();
    }
}
