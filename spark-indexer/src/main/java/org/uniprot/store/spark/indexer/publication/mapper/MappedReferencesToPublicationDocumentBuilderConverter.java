package org.uniprot.store.spark.indexer.publication.mapper;

import static org.uniprot.store.spark.indexer.publication.PublicationDocumentsToHDFSWriter.separateJoinKey;

import java.util.*;

import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Column;
import org.uniprot.core.publication.*;
import org.uniprot.core.publication.impl.MappedPublicationsBuilder;
import org.uniprot.store.indexer.publication.common.PublicationUtils;
import org.uniprot.store.search.document.publication.PublicationDocument;
import static org.apache.spark.sql.functions.*;

import scala.Tuple2;

/**
 * Created 21/01/2021
 *
 * @author Edd
 */
public class MappedReferencesToPublicationDocumentBuilderConverter
        implements PairFunction<
                Tuple2<String, Iterable<MappedReference>>, Integer, PublicationDocument.Builder> {
    @Override
    public Tuple2<Integer, PublicationDocument.Builder> call(
            Tuple2<String, Iterable<MappedReference>> tuple) throws Exception {

        String[] separatedJoinKey = separateJoinKey(tuple._1);
        String accession = separatedJoinKey[0];
        String pubMed =
                separatedJoinKey[1]; // this will be null, for submissions (i.e., no pubmed id)

        PublicationDocument.Builder docBuilder = PublicationDocument.builder();

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

        docBuilder
                .id(getUniqueId())
                .accession(accession)
                .pubMedId(pubMed)
                .categories(categories)
                .mainType(Collections.max(types))
                .types(types)
                .publicationMappedReferences(
                        PublicationUtils.asBinary(mappedPublicationsBuilder.build()));
        return new Tuple2<>(getPubMedIdRealOrFake(pubMed), docBuilder);
    }

    private String getUniqueId() {
        return UUID.nameUUIDFromBytes(rand().expr().toString().getBytes()).toString();
    }

    private int getPubMedIdRealOrFake(String pubMed) {

        if (pubMed == null) {
            int fakePubMed = new Random().nextInt();
            if (fakePubMed > 0) {
                fakePubMed = fakePubMed * -1;
            }
            return fakePubMed;
        } else {
            return Integer.parseInt(pubMed);
        }
    }

    private Optional<Integer> injectMappedReferenceInfo(
            MappedReference ref,
            PublicationDocument.Builder docBuilder,
            MappedPublicationsBuilder mappedPublicationsBuilder) {

        if (ref instanceof UniProtKBMappedReference) {
            docBuilder.refNumber(((UniProtKBMappedReference) ref).getReferenceNumber() + 1);
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
