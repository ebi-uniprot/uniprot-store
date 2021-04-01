package org.uniprot.store.spark.indexer.publication;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.uniprot.core.publication.MappedReferenceType.*;

import java.io.IOException;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.jupiter.api.Test;
import org.uniprot.core.Value;
import org.uniprot.core.json.parser.publication.MappedPublicationsJsonConfig;
import org.uniprot.core.publication.*;
import org.uniprot.store.search.document.publication.PublicationDocument;
import org.uniprot.store.spark.indexer.common.JobParameter;
import org.uniprot.store.spark.indexer.common.util.SparkUtils;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Created 19/01/2021
 *
 * @author Edd
 */
class PublicationDocumentsToHDFSWriterTest {
    private static final String LARGE_SCALE_STUDY_PUBMED_ID = "55555555";

    @Test
    void writeIndexDocumentsToHDFS() throws IOException {
        // given
        ResourceBundle application = SparkUtils.loadApplicationProperty();
        try (JavaSparkContext sparkContext = SparkUtils.loadSparkContext(application)) {
            JobParameter parameter =
                    JobParameter.builder()
                            .applicationConfig(application)
                            .releaseName("2020_02")
                            .sparkContext(sparkContext)
                            .build();

            FakePublicationDocumentsToHDFSWriter writer =
                    new FakePublicationDocumentsToHDFSWriter(parameter);

            // when
            writer.writeIndexDocumentsToHDFS();

            // then
            List<PublicationDocument> savedDocuments = writer.getSavedDocuments();

            // ... and then
            assertNotNull(savedDocuments);
            checkComputationalDocuments(savedDocuments);
            checkCommunityDocuments(savedDocuments);
            checkUniProtKBDocuments(savedDocuments);
            checkDocumentMerging(savedDocuments);
            checkLargeScaleStudySetCorrectly(savedDocuments);
        }
    }

    private void checkLargeScaleStudySetCorrectly(List<PublicationDocument> savedDocuments) {
        // check all small scale study docs
        List<PublicationDocument> smallScaleStudyDocs =
                savedDocuments.stream()
                        .filter(doc -> !doc.isLargeScale())
                        .collect(Collectors.toList());
        assertThat(
                smallScaleStudyDocs.stream()
                        .map(PublicationDocument::getAccession)
                        .collect(Collectors.toSet()),
                containsInAnyOrder(
                        "Q9EPI6", "COMM00", "COMM01", "COMM02", "COMM03", "COMM04", "COMM05",
                        "COMM06", "COMP00", "COMP01", "COMP02", "COMP03"));
        smallScaleStudyDocs.forEach(doc -> assertThat(doc.isLargeScale(), is(false)));

        // check all large scale study docs
        List<PublicationDocument> largeScaleStudyDocs =
                savedDocuments.stream()
                        .filter(PublicationDocument::isLargeScale)
                        .collect(Collectors.toList());

        // 30 computational, 20 community, 1 uniprotkb reviewed
        assertThat(largeScaleStudyDocs, hasSize(51));
        largeScaleStudyDocs.forEach(
                doc -> {
                    assertThat(doc.isLargeScale(), is(true));
                    assertThat(doc.getCitationId(), is(LARGE_SCALE_STUDY_PUBMED_ID));
                });

        Map<MappedReferenceType, Long> referenceTypeCountMap =
                largeScaleStudyDocs.stream()
                        .collect(
                                Collectors.groupingBy(
                                        doc -> getType(doc.getMainType()), Collectors.counting()));
        assertThat(referenceTypeCountMap, hasEntry(UNIPROTKB_REVIEWED, 1L));
        assertThat(referenceTypeCountMap, hasEntry(COMPUTATIONAL, 30L));
        assertThat(referenceTypeCountMap, hasEntry(COMMUNITY, 20L));
    }

    private void checkDocumentMerging(List<PublicationDocument> savedDocuments) throws IOException {
        List<PublicationDocument> kbDocs =
                savedDocuments.stream()
                        .filter(
                                doc ->
                                        doc.getAccession().equals("Q9EPI6")
                                                && doc.getCitationId() != null
                                                && doc.getCitationId().equals("15018815"))
                        .collect(Collectors.toList());

        assertThat(kbDocs, hasSize(1));

        PublicationDocument kbRN4Doc = kbDocs.get(0);

        assertThat(kbRN4Doc.getCitationId(), is("15018815"));
        assertThat(kbRN4Doc.getMainType(), is(UNIPROTKB_REVIEWED.getIntValue()));
        assertThat(
                kbRN4Doc.getTypes(),
                containsInAnyOrder(
                        COMPUTATIONAL.getIntValue(),
                        COMMUNITY.getIntValue(),
                        UNIPROTKB_REVIEWED.getIntValue()));
        assertThat(
                kbRN4Doc.getCategories(),
                containsInAnyOrder("Sequences", "Expression", "Interaction"));

        MappedPublications mappedPubsForKbRN4 = extractObject(kbRN4Doc);

        assertThat(mappedPubsForKbRN4.getCommunityMappedReferences(), hasSize(1));
        assertThat(mappedPubsForKbRN4.getComputationallyMappedReferences(), hasSize(2));
        assertThat(mappedPubsForKbRN4.getUniProtKBMappedReference(), is(notNullValue()));

        // check uniprotkb ref within mapped reference
        UniProtKBMappedReference kbRN4Ref = mappedPubsForKbRN4.getUniProtKBMappedReference();

        assertThat(kbRN4Ref.getReferencePositions(), contains("TISSUE SPECIFICITY"));
        assertThat(kbRN4Ref.getReferenceComments(), is(empty()));
        assertThat(kbRN4Ref.getSource().getName(), is("UniProtKB reviewed (Swiss-Prot)"));
        assertThat(kbRN4Ref.getSource().getId(), is(nullValue()));
        assertThat(kbRN4Ref.getCitationId(), is("15018815"));
        assertThat(kbRN4Ref.getSourceCategories(), contains("Expression"));

        // check community ref within mapped reference
        assertThat(
                mappedPubsForKbRN4.getCommunityMappedReferences().get(0).getSource().getId(),
                is("0000-0000-0000-0001"));
        assertThat(
                mappedPubsForKbRN4.getCommunityMappedReferences().get(0).getSourceCategories(),
                contains("Sequences"));

        // check computational refs within mapped reference
        assertThat(
                mappedPubsForKbRN4.getComputationallyMappedReferences().stream()
                        .map(ref -> ref.getSource().getId())
                        .collect(Collectors.toList()),
                contains("100002", "100003"));
        assertThat(
                mappedPubsForKbRN4.getComputationallyMappedReferences().stream()
                        .map(MappedReference::getSourceCategories)
                        .flatMap(Collection::stream)
                        .collect(Collectors.toSet()),
                contains("Interaction"));
        assertThat(
                mappedPubsForKbRN4.getComputationallyMappedReferences().stream()
                        .map(ComputationallyMappedReference::getAnnotation)
                        .collect(Collectors.toList()),
                contains("An interaction again.", "An interaction again 2."));
    }

    private void checkUniProtKBDocuments(List<PublicationDocument> savedDocuments)
            throws IOException {
        List<PublicationDocument> kbDocs =
                savedDocuments.stream()
                        .filter(doc -> doc.getAccession().equals("Q9EPI6"))
                        .collect(Collectors.toList());

        assertThat(kbDocs, hasSize(7));

        // all accessions in file were used
        assertThat(
                kbDocs.stream().map(PublicationDocument::getAccession).collect(Collectors.toSet()),
                contains("Q9EPI6"));

        // all pubmed ids in file were used
        assertThat(
                kbDocs.stream()
                        .map(PublicationDocument::getCitationId)
                        .filter(Objects::nonNull)
                        .collect(Collectors.toList()),
                containsInAnyOrder(
                        "CI-A0GQN29STRAFN",
                        "CI-L4RDPFJS30T",
                        "15489334",
                        "15018815",
                        LARGE_SCALE_STUDY_PUBMED_ID,
                        "19608740",
                        "21364755"));

        // check RN 1, and that it is a submission
        PublicationDocument kbRN1Doc = extractValue(kbDocs, PublicationDocument::getRefNumber, 1);

        assertThat(kbRN1Doc.getCitationId(), is("CI-L4RDPFJS30T"));
        assertThat(kbRN1Doc.getCategories(), contains("Sequence"));

        MappedPublications mappedPubsForKbRN1 = extractObject(kbRN1Doc);
        assertThat(mappedPubsForKbRN1.getCommunityMappedReferences(), hasSize(0));
        assertThat(mappedPubsForKbRN1.getComputationallyMappedReferences(), hasSize(0));
        assertThat(mappedPubsForKbRN1.getUniProtKBMappedReference(), is(notNullValue()));

        UniProtKBMappedReference kbRN1Ref = mappedPubsForKbRN1.getUniProtKBMappedReference();

        assertThat(
                kbRN1Ref.getReferencePositions(),
                contains("NUCLEOTIDE SEQUENCE [MRNA] (ISOFORMS 1; 2 AND 4)"));
        assertThat(
                kbRN1Ref.getReferenceComments().stream()
                        .map(Value::getValue)
                        .collect(Collectors.toList()),
                contains("Sprague-Dawley", "Brain"));
        assertThat(kbRN1Ref.getSource().getName(), is("UniProtKB reviewed (Swiss-Prot)"));
        assertThat(kbRN1Ref.getSource().getId(), is(nullValue()));
        assertThat(kbRN1Ref.getCitationId(), is("CI-L4RDPFJS30T"));
        assertThat(kbRN1Ref.getSourceCategories(), contains("Sequence"));

        // check RN 3
        PublicationDocument kbRN3Doc = extractValue(kbDocs, PublicationDocument::getRefNumber, 3);

        assertThat(kbRN3Doc.getCitationId(), is("15489334"));
        assertThat(kbRN3Doc.getMainType(), is(UNIPROTKB_REVIEWED.getIntValue()));
        assertThat(kbRN3Doc.getTypes(), contains(UNIPROTKB_REVIEWED.getIntValue()));
        assertThat(kbRN3Doc.getCategories(), containsInAnyOrder("Sequence"));

        MappedPublications mappedPubsForKbRN3 = extractObject(kbRN3Doc);
        assertThat(mappedPubsForKbRN3.getCommunityMappedReferences(), hasSize(0));
        assertThat(mappedPubsForKbRN3.getComputationallyMappedReferences(), hasSize(0));
        assertThat(mappedPubsForKbRN3.getUniProtKBMappedReference(), is(notNullValue()));

        UniProtKBMappedReference kbRN4Ref = mappedPubsForKbRN3.getUniProtKBMappedReference();

        assertThat(
                kbRN4Ref.getReferencePositions(),
                contains("NUCLEOTIDE SEQUENCE [LARGE SCALE MRNA] (ISOFORM 1)"));
        assertThat(
                kbRN4Ref.getReferenceComments().stream()
                        .map(Value::getValue)
                        .collect(Collectors.toList()),
                contains("Brain"));
        assertThat(kbRN4Ref.getSource().getName(), is("UniProtKB reviewed (Swiss-Prot)"));
        assertThat(kbRN4Ref.getSource().getId(), is(nullValue()));
        assertThat(kbRN4Ref.getCitationId(), is("15489334"));
        assertThat(kbRN4Ref.getSourceCategories(), contains("Sequence"));
    }

    private void checkCommunityDocuments(List<PublicationDocument> savedDocuments)
            throws IOException {
        List<PublicationDocument> communityDocs =
                savedDocuments.stream()
                        .filter(doc -> doc.getAccession().startsWith("COMM"))
                        .collect(Collectors.toList());

        assertThat(communityDocs, hasSize(7));

        // all accessions in file were used
        assertThat(
                communityDocs.stream()
                        .map(PublicationDocument::getAccession)
                        .collect(Collectors.toList()),
                containsInAnyOrder(
                        "COMM01", "COMM02", "COMM03", "COMM04", "COMM05", "COMM06", "COMM00"));

        // all pubmed ids in file were used
        assertThat(
                communityDocs.stream()
                        .map(PublicationDocument::getCitationId)
                        .collect(Collectors.toList()),
                containsInAnyOrder(
                        "00000001",
                        "00000002",
                        "00000003",
                        "00000004",
                        "00000005",
                        "00000006",
                        "00000000"));

        // This document is one created from two lines in the community publication text file
        PublicationDocument comm00Doc =
                extractValue(communityDocs, PublicationDocument::getAccession, "COMM00");

        assertThat(comm00Doc.getCitationId(), is("00000000"));
        assertThat(
                comm00Doc.getCategories(),
                containsInAnyOrder("Expression", "Function", "Sequences"));
        assertThat(comm00Doc.getMainType(), is(COMMUNITY.getIntValue()));
        assertThat(comm00Doc.getTypes(), contains(COMMUNITY.getIntValue()));

        MappedPublications mappedPubsForComm00Doc = extractObject(comm00Doc);
        assertThat(mappedPubsForComm00Doc.getCommunityMappedReferences(), hasSize(2));
        assertThat(mappedPubsForComm00Doc.getComputationallyMappedReferences(), hasSize(0));
        assertThat(mappedPubsForComm00Doc.getUniProtKBMappedReference(), is(nullValue()));

        CommunityMappedReference ref0 =
                extractValue(
                        mappedPubsForComm00Doc.getCommunityMappedReferences(),
                        ref -> ref.getCommunityAnnotation().getProteinOrGene(),
                        "Protein 0.");
        assertThat(ref0.getSourceCategories(), containsInAnyOrder("Function", "Expression"));
        assertThat(ref0.getSource().getName(), is("ORCID"));
        assertThat(ref0.getSource().getId(), is("0000-0000-0000-0000"));
        assertThat(ref0.getCommunityAnnotation().getFunction(), is(nullValue()));
        assertThat(ref0.getCommunityAnnotation().getComment(), is(nullValue()));
        assertThat(ref0.getCommunityAnnotation().getDisease(), is(nullValue()));

        CommunityMappedReference ref1 =
                extractValue(
                        mappedPubsForComm00Doc.getCommunityMappedReferences(),
                        ref -> ref.getCommunityAnnotation().getProteinOrGene(),
                        "Protein 1.");
        assertThat(ref1.getSourceCategories(), contains("Sequences"));
        assertThat(ref1.getSource().getName(), is("ORCID"));
        assertThat(ref1.getSource().getId(), is("0000-0000-0000-0001"));
        assertThat(ref1.getCommunityAnnotation().getFunction(), is("A function."));
        assertThat(ref1.getCommunityAnnotation().getComment(), is("A comment."));
        assertThat(ref1.getCommunityAnnotation().getDisease(), is("A disease."));
    }

    private void checkComputationalDocuments(List<PublicationDocument> savedDocuments)
            throws IOException {
        List<PublicationDocument> compDocs =
                savedDocuments.stream()
                        .filter(doc -> doc.getAccession().startsWith("COMP"))
                        .collect(Collectors.toList());

        assertThat(compDocs, hasSize(4));

        // all accessions in file were used
        assertThat(
                compDocs.stream()
                        .map(PublicationDocument::getAccession)
                        .collect(Collectors.toList()),
                containsInAnyOrder("COMP01", "COMP02", "COMP03", "COMP00"));

        // all pubmed ids in file were used
        assertThat(
                compDocs.stream()
                        .map(PublicationDocument::getCitationId)
                        .collect(Collectors.toList()),
                containsInAnyOrder("10000001", "10000002", "10000003", "10000000"));

        // This document is one created from three lines in the computational publication text file
        PublicationDocument comp00Doc =
                extractValue(compDocs, PublicationDocument::getAccession, "COMP00");

        assertThat(comp00Doc.getCitationId(), is("10000000"));
        assertThat(
                comp00Doc.getCategories(),
                containsInAnyOrder("Pathology & Biotech", "Sequences", "Interaction"));
        assertThat(comp00Doc.getMainType(), is(COMPUTATIONAL.getIntValue()));
        assertThat(comp00Doc.getTypes(), contains(COMPUTATIONAL.getIntValue()));

        MappedPublications mappedPubsForComp00Doc = extractObject(comp00Doc);
        assertThat(mappedPubsForComp00Doc.getCommunityMappedReferences(), hasSize(0));
        assertThat(mappedPubsForComp00Doc.getComputationallyMappedReferences(), hasSize(3));
        assertThat(mappedPubsForComp00Doc.getUniProtKBMappedReference(), is(nullValue()));

        ComputationallyMappedReference ref0 =
                extractValue(
                        mappedPubsForComp00Doc.getComputationallyMappedReferences(),
                        ComputationallyMappedReference::getAnnotation,
                        "A pathology & biotech.");
        assertThat(ref0.getSourceCategories(), containsInAnyOrder("Pathology & Biotech"));
        assertThat(ref0.getSource().getName(), is("GAD"));
        assertThat(ref0.getSource().getId(), is("100000"));

        ComputationallyMappedReference ref1 =
                extractValue(
                        mappedPubsForComp00Doc.getComputationallyMappedReferences(),
                        ComputationallyMappedReference::getAnnotation,
                        "An interaction.");
        assertThat(ref1.getSourceCategories(), containsInAnyOrder("Interaction"));
        assertThat(ref1.getSource().getName(), is("GAD"));
        assertThat(ref1.getSource().getId(), is("100001"));

        ComputationallyMappedReference ref2 =
                extractValue(
                        mappedPubsForComp00Doc.getComputationallyMappedReferences(),
                        ComputationallyMappedReference::getAnnotation,
                        null);
        assertThat(ref2.getSourceCategories(), containsInAnyOrder("Sequences"));
        assertThat(ref2.getSource().getName(), is("GAD"));
        assertThat(ref2.getSource().getId(), is("100002"));
    }

    private <T, V> T extractValue(List<T> docs, Function<T, V> docStringGetter, V shouldEqual) {
        return docs.stream()
                .filter(
                        doc -> {
                            if (shouldEqual == null) {
                                return docStringGetter.apply(doc) == null;
                            } else {
                                return docStringGetter.apply(doc).equals(shouldEqual);
                            }
                        })
                .findFirst()
                .orElseThrow(() -> new AssertionError("Could not find document"));
    }

    private static class FakePublicationDocumentsToHDFSWriter
            extends PublicationDocumentsToHDFSWriter {
        private List<PublicationDocument> documents;

        public FakePublicationDocumentsToHDFSWriter(JobParameter parameter) {
            super(parameter);
        }

        @Override
        void saveToHDFS(JavaRDD<PublicationDocument> publicationDocumentRDD) {
            documents = publicationDocumentRDD.collect();
        }

        List<PublicationDocument> getSavedDocuments() {
            return documents;
        }
    }

    private static final ObjectMapper OBJECT_MAPPER =
            MappedPublicationsJsonConfig.getInstance().getFullObjectMapper();

    static MappedPublications extractObject(PublicationDocument document) throws IOException {
        return OBJECT_MAPPER.readValue(
                document.getPublicationMappedReferences(), MappedPublications.class);
    }
}
