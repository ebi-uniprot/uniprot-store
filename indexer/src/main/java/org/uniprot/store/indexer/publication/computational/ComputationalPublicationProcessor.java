package org.uniprot.store.indexer.publication.computational;

import static org.uniprot.core.publication.MappedReferenceType.COMPUTATIONAL;
import static org.uniprot.store.indexer.publication.common.PublicationUtils.asBinary;
import static org.uniprot.store.indexer.publication.common.PublicationUtils.getDocumentId;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.springframework.batch.item.ItemProcessor;
import org.uniprot.core.publication.ComputationallyMappedReference;
import org.uniprot.core.publication.MappedPublications;
import org.uniprot.core.publication.impl.MappedPublicationsBuilder;
import org.uniprot.store.search.document.publication.PublicationDocument;

public class ComputationalPublicationProcessor
        implements ItemProcessor<ComputationallyMappedReference, List<PublicationDocument>> {

    @Override
    public List<PublicationDocument> process(ComputationallyMappedReference reference) {
        List<PublicationDocument> toReturn = new ArrayList<>();
        PublicationDocument.PublicationDocumentBuilder builder = PublicationDocument.builder();
        toReturn.add(
                builder.pubMedId(reference.getPubMedId())
                        .accession(reference.getUniProtKBAccession().getValue())
                        .id(getDocumentId())
                        .categories(reference.getSourceCategories())
                        .types(Collections.singleton(COMPUTATIONAL.getIntValue()))
                        .mainType(COMPUTATIONAL.getIntValue())
                        .publicationMappedReferences(asBinary(createMappedPublications(reference)))
                        .build());

        return toReturn;
    }

    private MappedPublications createMappedPublications(ComputationallyMappedReference reference) {
        return new MappedPublicationsBuilder().computationalMappedReferencesAdd(reference).build();
    }
}
