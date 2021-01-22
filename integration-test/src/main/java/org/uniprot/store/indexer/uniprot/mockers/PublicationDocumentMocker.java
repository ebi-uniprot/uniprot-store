package org.uniprot.store.indexer.uniprot.mockers;

import static org.uniprot.store.indexer.publication.common.PublicationUtils.asBinary;

import java.util.*;
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
    private static final List<String> CATEGORIES = new ArrayList<>();

    private PublicationDocumentMocker() {}

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
        String id = generateId();
        PublicationDocument.Builder builder =
                populateDocumentWithoutPubmedOrMappedPublications(
                        id, generateAccession(accessionNumber));

        UniProtKBMappedReference kbRef =
                new UniProtKBMappedReferenceBuilder()
                        .source(new MappedSourceBuilder().name("source 3").id("id 3").build())
                        .citation(new SubmissionBuilder().title("Submission").build())
                        .uniProtKBAccession(generateAccession(accessionNumber))
                        .sourceCategoriesSet(DOC_CATEGORIES.get(id))
                        .referencePositionsAdd("Reference position 1")
                        .referenceCommentsAdd(
                                new ReferenceCommentBuilder()
                                        .type(ReferenceCommentType.PLASMID)
                                        .build())
                        .build();

        MappedPublications publications =
                new MappedPublicationsBuilder().reviewedMappedReference(kbRef).build();

        return builder.publicationMappedReferences(asBinary(publications)).build();
    }

    private static final Map<String, Set<String>> DOC_CATEGORIES = new HashMap<>();

    private static PublicationDocument.Builder populateDocumentWithoutPubmedOrMappedPublications(
            String id, String accession) {
        int randomCategoriesSize = ThreadLocalRandom.current().nextInt(0, CATEGORIES.size() + 1);
        Set<String> randomCategories = new HashSet<>();
        randomCategories.add("Interaction");
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

        DOC_CATEGORIES.put(id, randomCategories);

        return PublicationDocument.builder()
                .id(id)
                .accession(accession)
                .categories(randomCategories)
                .mainType(MappedReferenceType.UNIPROTKB_REVIEWED.getIntValue())
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
                .refNumber(10000)
                .type(MappedReferenceType.COMPUTATIONAL.getIntValue());
    }

    private static String generateId() {
        return "" + ThreadLocalRandom.current().nextInt();
    }

    private static String generateAccession(int accessionNumber) {
        return String.format("P%05d", accessionNumber);
    }

    public static PublicationDocument create(int accessionNumber, int pubmedNumber) {
        String id = generateId();
        String accession = generateAccession(accessionNumber);
        return populateDocumentWithoutPubmedOrMappedPublications(id, accession)
                .pubMedId(generatePubMedId(pubmedNumber))
                .type(MappedReferenceType.COMPUTATIONAL.getIntValue())
                .publicationMappedReferences(
                        getMappedPublications(id, accession, generatePubMedId(pubmedNumber)))
                .build();
    }

    public static PublicationDocument create(String accession, int pubmedNumber) {
        String id = generateId();
        return populateDocumentWithoutPubmedOrMappedPublications(id, accession)
                .pubMedId(generatePubMedId(pubmedNumber))
                .type(MappedReferenceType.COMPUTATIONAL.getIntValue())
                .publicationMappedReferences(
                        getMappedPublications(id, accession, generatePubMedId(pubmedNumber)))
                .build();
    }

    public static PublicationDocument createWithAccAndPubMed(
            String accession, long pubmedNumber, int refNumber) {
        String id = generateId();
        String pubMedId = String.valueOf(pubmedNumber);
        return populateDocumentWithoutPubmedOrMappedPublications(id, accession)
                .pubMedId(pubMedId)
                .type(MappedReferenceType.COMPUTATIONAL.getIntValue())
                .publicationMappedReferences(getMappedPublications(id, accession, pubMedId))
                .refNumber(refNumber)
                .build();
    }

    private static String generatePubMedId(int pubmedNumber) {
        return String.format("%010d", pubmedNumber);
    }

    private static byte[] getMappedPublications(String id, String accession, String pubMedId) {
        CommunityMappedReference communityRef =
                new CommunityMappedReferenceBuilder()
                        .source(
                                new MappedSourceBuilder()
                                        .name("source " + accession)
                                        .id("id " + accession)
                                        .build())
                        .pubMedId(pubMedId)
                        .uniProtKBAccession(accession)
                        .sourceCategoriesAdd("Interaction")
                        .communityAnnotation(
                                new CommunityAnnotationBuilder()
                                        .proteinOrGene("community " + accession)
                                        .build())
                        .build();

        ComputationallyMappedReference computationalRef =
                new ComputationallyMappedReferenceBuilder()
                        .source(
                                new MappedSourceBuilder()
                                        .name("source " + accession)
                                        .id("id " + accession)
                                        .build())
                        .pubMedId(pubMedId)
                        .uniProtKBAccession(accession)
                        .sourceCategoriesAdd(CATEGORIES.get(1))
                        .annotation("computational " + accession)
                        .build();

        UniProtKBMappedReference kbRef =
                new UniProtKBMappedReferenceBuilder()
                        .source(
                                new MappedSourceBuilder()
                                        .name("source " + accession)
                                        .id("id " + accession)
                                        .build())
                        .pubMedId(pubMedId)
                        .uniProtKBAccession(accession)
                        .sourceCategoriesSet(DOC_CATEGORIES.get(id))
                        .referencePositionsAdd("Reference position " + accession)
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
