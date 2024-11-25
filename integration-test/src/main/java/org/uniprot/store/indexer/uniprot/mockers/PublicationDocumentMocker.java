package org.uniprot.store.indexer.uniprot.mockers;

import static org.uniprot.store.indexer.publication.common.PublicationUtils.asBinary;

import java.security.SecureRandom;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.uniprot.core.publication.CommunityMappedReference;
import org.uniprot.core.publication.ComputationallyMappedReference;
import org.uniprot.core.publication.MappedPublications;
import org.uniprot.core.publication.MappedReferenceType;
import org.uniprot.core.publication.UniProtKBMappedReference;
import org.uniprot.core.publication.impl.CommunityAnnotationBuilder;
import org.uniprot.core.publication.impl.CommunityMappedReferenceBuilder;
import org.uniprot.core.publication.impl.ComputationallyMappedReferenceBuilder;
import org.uniprot.core.publication.impl.MappedPublicationsBuilder;
import org.uniprot.core.publication.impl.MappedSourceBuilder;
import org.uniprot.core.publication.impl.UniProtKBMappedReferenceBuilder;
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

    private static final Map<String, Set<String>> DOC_CATEGORIES = new HashMap<>();

    private static PublicationDocument.Builder populateDocumentWithoutPubmedOrMappedPublications(
            String id, String accession) {

        int randomCategoriesSize = getRandomGenerator().nextInt(CATEGORIES.size() + 1);

        Set<String> randomCategories = new HashSet<>();
        randomCategories.add("Interaction");
        int count = 0;
        for (String category : CATEGORIES) {
            randomCategories.add(category);
            if (++count >= randomCategoriesSize) {
                break;
            }
        }
        boolean isLargeScale = getRandomGenerator().nextBoolean();
        DOC_CATEGORIES.put(id, randomCategories);

        return PublicationDocument.builder()
                .id(id)
                .accession(accession)
                .categories(randomCategories)
                .mainType(MappedReferenceType.UNIPROTKB_REVIEWED.getIntValue())
                .isLargeScale(isLargeScale)
                .refNumber(10000)
                .type(MappedReferenceType.COMPUTATIONAL.getIntValue());
    }

    private static String generateId() {
        return "" + getRandomGenerator().nextInt();
    }

    private static String generateAccession(int accessionNumber) {
        return String.format("P%05d", accessionNumber);
    }

    public static PublicationDocument create(int accessionNumber, String citationId) {
        String id = generateId();
        String accession = generateAccession(accessionNumber);
        return populateDocumentWithoutPubmedOrMappedPublications(id, accession)
                .citationId(citationId)
                .type(MappedReferenceType.COMPUTATIONAL.getIntValue())
                .publicationMappedReferences(getMappedPublications(id, accession, citationId))
                .build();
    }

    public static PublicationDocument create(String accession, String citationId) {
        String id = generateId();
        return populateDocumentWithoutPubmedOrMappedPublications(id, accession)
                .citationId(citationId)
                .type(MappedReferenceType.COMPUTATIONAL.getIntValue())
                .publicationMappedReferences(getMappedPublications(id, accession, citationId))
                .build();
    }

    public static PublicationDocument createWithAccAndPubMed(
            String accession, long pubmedNumber, int refNumber) {
        String id = generateId();
        String pubMedId = String.valueOf(pubmedNumber);
        return populateDocumentWithoutPubmedOrMappedPublications(id, accession)
                .citationId(pubMedId)
                .type(MappedReferenceType.COMPUTATIONAL.getIntValue())
                .publicationMappedReferences(getMappedPublications(id, accession, pubMedId))
                .refNumber(refNumber)
                .build();
    }

    private static byte[] getMappedPublications(String id, String accession, String citationId) {
        CommunityMappedReference communityRef =
                new CommunityMappedReferenceBuilder()
                        .source(
                                new MappedSourceBuilder()
                                        .name("source " + accession)
                                        .id("id " + accession)
                                        .build())
                        .citationId(citationId)
                        .uniProtKBAccession(accession)
                        .sourceCategoriesAdd("Interaction")
                        .communityAnnotation(
                                new CommunityAnnotationBuilder()
                                        .proteinOrGene("community " + accession)
                                        .submissionDate(LocalDate.of(2024, 9, 16))
                                        .build())
                        .build();

        ComputationallyMappedReference computationalRef =
                new ComputationallyMappedReferenceBuilder()
                        .source(
                                new MappedSourceBuilder()
                                        .name("source " + accession)
                                        .id("id " + accession)
                                        .build())
                        .citationId(citationId)
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
                        .citationId(citationId)
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
                        .uniProtKBMappedReference(kbRef)
                        .build();

        return asBinary(mappedPublications);
    }

    private static SecureRandom getRandomGenerator() {
        SecureRandom random = new SecureRandom();
        byte[] bytes = new byte[20];
        random.nextBytes(bytes);
        return random;
    }
}
