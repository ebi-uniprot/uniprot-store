package org.uniprot.store.spark.indexer.publication.mapper;

import org.apache.spark.api.java.function.Function;
import org.uniprot.core.publication.*;
import org.uniprot.core.publication.impl.MappedPublicationsBuilder;
import org.uniprot.store.indexer.publication.common.PublicationUtils;
import org.uniprot.store.search.document.publication.PublicationDocument;
import scala.Tuple2;

import java.util.Collections;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

import static org.uniprot.store.spark.indexer.publication.PublicationDocumentsToHDFSWriter.separateJoinKey;

/**
 * Merges an {@link Iterable} of {@link MappedReference} instances to create an instance of {@link
 * PublicationDocument}.
 *
 * <p>Created 19/01/2021
 *
 * @author Edd
 */
public class MappedReferencesToPublicationDocumentConverter
        implements Function<Tuple2<String, Iterable<MappedReference>>, PublicationDocument> {
    @Override
    public PublicationDocument call(Tuple2<String, Iterable<MappedReference>> tuple)
            throws Exception {
        String[] separatedJoinKey = separateJoinKey(tuple._1);
        String accession = separatedJoinKey[0];
        String pubMed =
                separatedJoinKey[1]; // this will be null, for submissions (i.e., no pubmed id)

        PublicationDocument.PublicationDocumentBuilder docBuilder = PublicationDocument.builder();

        MappedPublicationsBuilder mappedPublicationsBuilder = new MappedPublicationsBuilder();
        Set<Integer> types = new HashSet<>();
        Set<String> categories = new HashSet<>();
        for (MappedReference mappedReference : tuple._2) {
            Optional<Integer> type =
                    injectMappedReferenceInfo(
                            mappedReference, docBuilder, mappedPublicationsBuilder);
            type.ifPresent(types::add);
            categories.addAll(mappedReference.getSourceCategories());
        }

        return docBuilder
                .id(PublicationUtils.getDocumentId())
                .accession(accession)
                .pubMedId(pubMed)
                .categories(categories)
                .mainType(Collections.max(types))
                .types(types)
                .publicationMappedReferences(
                        PublicationUtils.asBinary(mappedPublicationsBuilder.build()))
                .build();
    }

    private Optional<Integer> injectMappedReferenceInfo(
            MappedReference ref,
            PublicationDocument.PublicationDocumentBuilder docBuilder,
            MappedPublicationsBuilder mappedPublicationsBuilder) {
        if (ref instanceof UniProtKBMappedReference) {
            docBuilder.refNumber(((UniProtKBMappedReference) ref).getReferenceNumber());
            mappedPublicationsBuilder.reviewedMappedReference((UniProtKBMappedReference) ref);
            return Optional.of(MappedReferenceType.UNIPROTKB_REVIEWED.getIntValue());
        } else if (ref instanceof ComputationallyMappedReference) {
            mappedPublicationsBuilder.computationalMappedReferencesAdd(
                    (ComputationallyMappedReference) ref);
            return Optional.of(MappedReferenceType.COMPUTATIONAL.getIntValue());
        } else if (ref instanceof CommunityMappedReference) {
            mappedPublicationsBuilder.communityMappedReferencesAdd((CommunityMappedReference) ref);
            return Optional.of(MappedReferenceType.COMMUNITY.getIntValue());
        }
        return Optional.empty();
    }
}
