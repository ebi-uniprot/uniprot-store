package org.uniprot.store.spark.indexer.publication.mapper;

import static org.apache.spark.sql.functions.rand;
import static org.uniprot.store.spark.indexer.publication.PublicationDocumentsToHDFSWriter.separateJoinKey;

import java.util.Collections;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

import org.apache.spark.api.java.function.PairFunction;
import org.uniprot.core.publication.CommunityMappedReference;
import org.uniprot.core.publication.ComputationallyMappedReference;
import org.uniprot.core.publication.MappedReference;
import org.uniprot.core.publication.MappedReferenceType;
import org.uniprot.core.publication.UniProtKBMappedReference;
import org.uniprot.core.publication.impl.MappedPublicationsBuilder;
import org.uniprot.core.uniprotkb.UniProtKBEntryType;
import org.uniprot.store.indexer.publication.common.PublicationUtils;
import org.uniprot.store.search.document.publication.PublicationDocument;

import scala.Tuple2;

/**
 * Given a Tuple2 of <accession, Iterable<MappedReference>>, representing all {@link
 * MappedReference}s for the same accession, this class creates a Tuple2 of <citation id,
 * PublicationDocument.Builder>.
 *
 * <p>Created 21/01/2021
 *
 * @author Edd
 */
public class MappedReferencesToPublicationDocumentBuilderConverter
        implements PairFunction<
                Tuple2<String, Iterable<MappedReference>>, String, PublicationDocument.Builder> {
    private static final long serialVersionUID = -5482428304872200536L;
    private static final String UNCLASSIFIED = "Unclassified";

    @Override
    public Tuple2<String, PublicationDocument.Builder> call(
            Tuple2<String, Iterable<MappedReference>> tuple) throws Exception {

        String[] separatedJoinKey = separateJoinKey(tuple._1);
        String accession = separatedJoinKey[0];
        String citationId = separatedJoinKey[1];

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
        if (categories.isEmpty()) {
            categories.add(UNCLASSIFIED);
        }

        docBuilder
                .id(getUniqueId())
                .accession(accession)
                .citationId(citationId)
                .categories(categories)
                .mainType(Collections.max(types))
                .types(types)
                .publicationMappedReferences(
                        PublicationUtils.asBinary(mappedPublicationsBuilder.build()));
        return new Tuple2<>(citationId, docBuilder);
    }

    private String getUniqueId() {
        return UUID.nameUUIDFromBytes(rand().expr().toString().getBytes()).toString();
    }

    private Optional<Integer> injectMappedReferenceInfo(
            MappedReference ref,
            PublicationDocument.Builder docBuilder,
            MappedPublicationsBuilder mappedPublicationsBuilder) {

        if (ref instanceof UniProtKBMappedReference) {
            UniProtKBMappedReference kbRef = (UniProtKBMappedReference) ref;
            docBuilder.refNumber(kbRef.getReferenceNumber() + 1);
            mappedPublicationsBuilder.uniProtKBMappedReference(kbRef);

            boolean isSwissProt =
                    kbRef.getSource().getName().equals(UniProtKBEntryType.SWISSPROT.getName());
            MappedReferenceType type =
                    isSwissProt
                            ? MappedReferenceType.UNIPROTKB_REVIEWED
                            : MappedReferenceType.UNIPROTKB_UNREVIEWED;
            return Optional.of(type.getIntValue());
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
