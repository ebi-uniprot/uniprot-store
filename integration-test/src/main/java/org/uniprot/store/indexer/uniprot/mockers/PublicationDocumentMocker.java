package org.uniprot.store.indexer.uniprot.mockers;

import org.uniprot.core.publication.*;
import org.uniprot.core.publication.impl.*;
import org.uniprot.store.indexer.publication.common.PublicationUtils;
import org.uniprot.store.search.document.publication.PublicationDocument;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Created 08/01/2021
 *
 * @author Edd
 */
public class PublicationDocumentMocker {
    private static final Set<String> CATEGORIES = new HashSet<>();

    static {
        CATEGORIES.add("Expression");
        CATEGORIES.add("Family & Domains");
        CATEGORIES.add("Function");
        CATEGORIES.add("Interaction");
        CATEGORIES.add("Names");
        CATEGORIES.add("Pathology & Biotech");
        CATEGORIES.add("PTM / Processing");
        CATEGORIES.add("Sequences");
        CATEGORIES.add("Subcellular Location");
        CATEGORIES.add("Structure");
    }

    public static PublicationDocument create(int accessionNumber, int pubmedNumber) {
        int randomCategoriesSize = ThreadLocalRandom.current().nextInt(0, CATEGORIES.size() + 1);
        Set<String> randomCategories = new HashSet<>();
        int count = 0;
        for (String category : CATEGORIES) {
            randomCategories.add(category);
            if (++count >= randomCategoriesSize) {
                break;
            }
        }

        long communityMappedProteinCount = ThreadLocalRandom.current().nextLong(0, 11);
        long computationalMappedProteinCount = ThreadLocalRandom.current().nextLong(0, 21);
        long reviewedMappedProteinCount = ThreadLocalRandom.current().nextLong(0, 31);
        long unreviewedMappedProteinCount = ThreadLocalRandom.current().nextLong(0, 11);
        String accession = String.format("P%05d", accessionNumber);
        String pubMedId = String.format("%010d", pubmedNumber);
        return PublicationDocument.builder()
                .accession(accession)
                .pubMedId(pubMedId)
                .categories(randomCategories)
                .mainType(
                        MappedReferenceType.getType(
                                        ThreadLocalRandom.current()
                                                .nextInt(0, MappedReferenceType.values().length))
                                .getIntValue())
                .communityMappedProteinCount(communityMappedProteinCount)
                .computationalMappedProteinCount(computationalMappedProteinCount)
                .reviewedMappedProteinCount(reviewedMappedProteinCount)
                .unreviewedMappedProteinCount(unreviewedMappedProteinCount)
                .isLargeScale(
                        (communityMappedProteinCount
                                        + computationalMappedProteinCount
                                        + unreviewedMappedProteinCount
                                        + reviewedMappedProteinCount)
                                > 50)
                .refNumber(0)
                .type(MappedReferenceType.COMPUTATIONAL.getIntValue())
                .publicationMappedReferences(getMappedPublications(accession, pubMedId))
                .build();
    }

    private static byte[] getMappedPublications(String accession, String pubMedId) {
        CommunityMappedReference communityRef =
                new CommunityMappedReferenceBuilder()
                        .source(new MappedSourceBuilder().name("source 1").id("id 1").build())
                        .pubMedId(pubMedId)
                        .uniProtKBAccession(accession)
                        .sourceCategoriesAdd(
                                CATEGORIES.stream()
                                        .findAny()
                                        .orElseThrow(IllegalStateException::new))
                        .communityAnnotation(
                                new CommunityAnnotationBuilder().proteinOrGene("protein 1").build())
                        .build();

        ComputationallyMappedReference computationalRef =
                new ComputationallyMappedReferenceBuilder()
                        .source(new MappedSourceBuilder().name("source 2").id("id 2").build())
                        .pubMedId(pubMedId)
                        .uniProtKBAccession(accession)
                        .sourceCategoriesAdd(
                                CATEGORIES.stream()
                                        .findAny()
                                        .orElseThrow(IllegalStateException::new))
                        .annotation("annotation 1")
                        .build();

        UniProtKBMappedReference kbRef =
                new UniProtKBMappedReferenceBuilder()
                        .source(new MappedSourceBuilder().name("source 2").id("id 2").build())
                        .pubMedId(pubMedId)
                        .uniProtKBAccession(accession)
                        .sourceCategoriesAdd(
                                CATEGORIES.stream()
                                        .findAny()
                                        .orElseThrow(IllegalStateException::new))
                        .referencePositionsAdd("Reference position 1")
                        .build();

        MappedPublications mappedPublications =
                new MappedPublicationsBuilder()
                        .communityMappedReferencesAdd(communityRef)
                        .computationalMappedReferencesAdd(computationalRef)
                        .reviewedMappedReference(kbRef)
                        .build();

        return PublicationUtils.asBinary(mappedPublications);
    }
}
