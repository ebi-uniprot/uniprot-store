package org.uniprot.store.indexer.uniprot.mockers;

import static org.uniprot.store.indexer.publication.common.PublicationUtils.asBinary;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;

import org.uniprot.core.citation.impl.SubmissionBuilder;
import org.uniprot.core.publication.*;
import org.uniprot.core.publication.impl.*;
import org.uniprot.core.uniprotkb.ReferenceCommentType;
import org.uniprot.core.uniprotkb.impl.ReferenceCommentBuilder;
import org.uniprot.store.search.document.publication.PublicationDocument;

/**
 * Created 08/01/2021
 *
 * @author Edd
 */
public class PublicationDocumentMocker {
    private static final Set<String> CATEGORIES = new HashSet<>();

    private PublicationDocumentMocker(){
    }

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

    public static PublicationDocument createWithoutPubmed(int accessionNumber) {
        UniProtKBMappedReference kbRef =
                new UniProtKBMappedReferenceBuilder()
                        .source(new MappedSourceBuilder().name("source 3").id("id 3").build())
                        .citation(new SubmissionBuilder().title("Submission").build())
                        .uniProtKBAccession(generateAccession(accessionNumber))
                        .sourceCategoriesAdd(
                                CATEGORIES.stream()
                                        .findAny()
                                        .orElseThrow(IllegalStateException::new))
                        .referencePositionsAdd("Reference position 1")
                        .referenceCommentsAdd(
                                new ReferenceCommentBuilder()
                                        .type(ReferenceCommentType.PLASMID)
                                        .build())
                        .build();

        MappedPublications publications =
                new MappedPublicationsBuilder().reviewedMappedReference(kbRef).build();

        return populateDocumentWithoutPubmedOrMappedPublications(accessionNumber)
                .publicationMappedReferences(asBinary(publications))
                .build();
    }

    private static PublicationDocument.PublicationDocumentBuilder
            populateDocumentWithoutPubmedOrMappedPublications(int accessionNumber) {
        int randomCategoriesSize = ThreadLocalRandom.current().nextInt(0, CATEGORIES.size() + 1);
        Set<String> randomCategories = new HashSet<>();
        int count = 0;
        for (String category : CATEGORIES) {
            randomCategories.add(category);
            if (++count >= randomCategoriesSize) {
                break;
            }
        }
        boolean isLargeScale = ThreadLocalRandom.current().nextBoolean();
        String accession = generateAccession(accessionNumber);
        return PublicationDocument.builder()
                .id("" + ThreadLocalRandom.current().nextInt())
                .accession(accession)
                .categories(randomCategories)
                .mainType(
                        MappedReferenceType.getType(
                                        ThreadLocalRandom.current()
                                                .nextInt(0, MappedReferenceType.values().length))
                                .getIntValue())
                .isLargeScale(isLargeScale)
                .refNumber(0)
                .type(MappedReferenceType.COMPUTATIONAL.getIntValue());
    }

    private static String generateAccession(int accessionNumber) {
        return String.format("P%05d", accessionNumber);
    }

    public static PublicationDocument create(int accessionNumber, int pubmedNumber) {
        //
        //        int randomCategoriesSize = ThreadLocalRandom.current().nextInt(0,
        // CATEGORIES.size() + 1);
        //        Set<String> randomCategories = new HashSet<>();
        //        int count = 0;
        //        for (String category : CATEGORIES) {
        //            randomCategories.add(category);
        //            if (++count >= randomCategoriesSize) {
        //                break;
        //            }
        //        }
        //
        //        long communityMappedProteinCount = ThreadLocalRandom.current().nextLong(0, 11);
        //        long computationalMappedProteinCount = ThreadLocalRandom.current().nextLong(0,
        // 21);
        //        long reviewedMappedProteinCount = ThreadLocalRandom.current().nextLong(0, 31);
        //        long unreviewedMappedProteinCount = ThreadLocalRandom.current().nextLong(0, 11);
        //        String accession = generateAccession(accessionNumber);
        //        String pubMedId = generatePubMedId(pubmedNumber);
        //        return PublicationDocument.builder()
        //                .accession(accession)
        //                .pubMedId(pubMedId)
        //                .categories(randomCategories)
        //                .mainType(
        //                        MappedReferenceType.getType(
        //                                        ThreadLocalRandom.current()
        //                                                .nextInt(0,
        // MappedReferenceType.values().length))
        //                                .getIntValue())
        //                .communityMappedProteinCount(communityMappedProteinCount)
        //                .computationalMappedProteinCount(computationalMappedProteinCount)
        //                .reviewedMappedProteinCount(reviewedMappedProteinCount)
        //                .unreviewedMappedProteinCount(unreviewedMappedProteinCount)
        //                .isLargeScale(
        //                        (communityMappedProteinCount
        //                                        + computationalMappedProteinCount
        //                                        + unreviewedMappedProteinCount
        //                                        + reviewedMappedProteinCount)
        //                                > 50)
        //                .refNumber(0)
        //                .type(MappedReferenceType.COMPUTATIONAL.getIntValue())
        //                .publicationMappedReferences(getMappedPublications(accession, pubMedId))
        //                .build();

        return populateDocumentWithoutPubmedOrMappedPublications(accessionNumber)
                .pubMedId(generatePubMedId(pubmedNumber))
                .type(MappedReferenceType.COMPUTATIONAL.getIntValue())
                .publicationMappedReferences(
                        getMappedPublications(
                                generateAccession(accessionNumber), generatePubMedId(pubmedNumber)))
                .build();
    }

    private static String generatePubMedId(int pubmedNumber) {
        return String.format("%010d", pubmedNumber);
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
                        .source(new MappedSourceBuilder().name("source 3").id("id 3").build())
                        .pubMedId(pubMedId)
                        .uniProtKBAccession(accession)
                        .sourceCategoriesAdd(
                                CATEGORIES.stream()
                                        .findAny()
                                        .orElseThrow(IllegalStateException::new))
                        .referencePositionsAdd("Reference position 1")
                        .referenceCommentsAdd(
                                new ReferenceCommentBuilder()
                                        .type(ReferenceCommentType.PLASMID)
                                        .build())
                        .build();

        MappedPublications mappedPublications =
                new MappedPublicationsBuilder()
                        .communityMappedReferencesAdd(communityRef)
                        .computationalMappedReferencesAdd(computationalRef)
                        .reviewedMappedReference(kbRef)
                        .build();

        return asBinary(mappedPublications);
    }
}
