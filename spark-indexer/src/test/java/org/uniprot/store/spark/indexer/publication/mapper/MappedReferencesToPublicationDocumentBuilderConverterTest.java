package org.uniprot.store.spark.indexer.publication.mapper;

import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.uniprot.core.publication.MappedReference;
import org.uniprot.core.publication.impl.*;
import org.uniprot.core.uniprotkb.ReferenceCommentType;
import org.uniprot.core.uniprotkb.UniProtKBEntryType;
import org.uniprot.core.uniprotkb.impl.ReferenceCommentBuilder;
import org.uniprot.store.search.document.publication.PublicationDocument;

import scala.Tuple2;

/**
 * @author lgonzales
 * @since 03/02/2021
 */
class MappedReferencesToPublicationDocumentBuilderConverterTest {

    @Test
    void mapCommunityMappedReference() throws Exception {
        MappedReferencesToPublicationDocumentBuilderConverter mapper =
                new MappedReferencesToPublicationDocumentBuilderConverter();
        String accPub = "P21802_100";
        List<MappedReference> mappedReferences = new ArrayList<>();
        mappedReferences.add(
                new CommunityMappedReferenceBuilder()
                        .citationId("100")
                        .uniProtKBAccession("P21802")
                        .source(new MappedSourceBuilder().id("CMNT_ID").name("CMNT_NAME").build())
                        .sourceCategoriesAdd("Interaction")
                        .communityAnnotation(
                                new CommunityAnnotationBuilder().comment("cmValue").build())
                        .build());

        Tuple2<String, Iterable<MappedReference>> tuple = new Tuple2<>(accPub, mappedReferences);
        Tuple2<String, PublicationDocument.Builder> result = mapper.call(tuple);
        assertNotNull(result);
        assertNotNull(result._1);
        assertNotNull(result._2);
        PublicationDocument doc = result._2.build();
        assertEquals("P21802", doc.getAccession());
        assertEquals("100", doc.getCitationId());
        assertEquals("P21802", doc.getAccession());
        assertTrue(doc.getCategories().contains("Interaction"));
        assertTrue(doc.getTypes().contains(0));
        assertNotNull(doc.getPublicationMappedReferences());
        assertEquals(0, doc.getMainType());
    }

    @Test
    void mapComputationallyMappedReference() throws Exception {
        MappedReferencesToPublicationDocumentBuilderConverter mapper =
                new MappedReferencesToPublicationDocumentBuilderConverter();
        String accPub = "P21802_100";
        List<MappedReference> mappedReferences = new ArrayList<>();
        mappedReferences.add(
                new ComputationallyMappedReferenceBuilder()
                        .citationId("100")
                        .uniProtKBAccession("P21802")
                        .source(new MappedSourceBuilder().id("CMNT_ID").name("CMNT_NAME").build())
                        .sourceCategoriesAdd("Interaction")
                        .annotation("AnnotationValue")
                        .build());

        Tuple2<String, Iterable<MappedReference>> tuple = new Tuple2<>(accPub, mappedReferences);
        Tuple2<String, PublicationDocument.Builder> result = mapper.call(tuple);
        assertNotNull(result);
        assertNotNull(result._1);
        assertNotNull(result._2);
        PublicationDocument doc = result._2.build();
        assertEquals("P21802", doc.getAccession());
        assertEquals("100", doc.getCitationId());
        assertEquals("P21802", doc.getAccession());
        assertTrue(doc.getCategories().contains("Interaction"));
        assertTrue(doc.getTypes().contains(1));
        assertNotNull(doc.getPublicationMappedReferences());
        assertEquals(1, doc.getMainType());
    }

    @Test
    void mapUniProtKBTremblMappedReference() throws Exception {
        MappedReferencesToPublicationDocumentBuilderConverter mapper =
                new MappedReferencesToPublicationDocumentBuilderConverter();
        String accPub = "P21802_CI-ASDKJIU12";
        List<MappedReference> mappedReferences = new ArrayList<>();
        mappedReferences.add(
                new UniProtKBMappedReferenceBuilder()
                        .uniProtKBAccession("P21802")
                        .source(
                                new MappedSourceBuilder()
                                        .id("CMNT_ID")
                                        .name(UniProtKBEntryType.TREMBL.getName())
                                        .build())
                        .citationId("CI-ASDKJIU12")
                        .sourceCategoriesAdd("Interaction")
                        .sourceCategoriesAdd("Function")
                        .referencePositionsAdd("rpValue")
                        .referenceCommentsAdd(
                                new ReferenceCommentBuilder()
                                        .type(ReferenceCommentType.PLASMID)
                                        .value("Rcvalue")
                                        .build())
                        .referenceNumber(10)
                        .build());

        Tuple2<String, Iterable<MappedReference>> tuple = new Tuple2<>(accPub, mappedReferences);
        Tuple2<String, PublicationDocument.Builder> result = mapper.call(tuple);
        assertNotNull(result);
        assertNotNull(result._1);
        assertNotNull(result._2);
        PublicationDocument doc = result._2.build();
        assertEquals("P21802", doc.getAccession());
        assertEquals("CI-ASDKJIU12", doc.getCitationId());
        assertEquals("P21802", doc.getAccession());
        assertTrue(doc.getCategories().contains("Interaction"));
        assertTrue(doc.getTypes().contains(3));
        assertNotNull(doc.getPublicationMappedReferences());
        assertEquals(3, doc.getMainType());
        assertEquals(11, doc.getRefNumber());
    }

    @Test
    void mapUniProtKBSwissProtMappedReference() throws Exception {
        MappedReferencesToPublicationDocumentBuilderConverter mapper =
                new MappedReferencesToPublicationDocumentBuilderConverter();
        String accPub = "P21802_CI-ASDKJIU12";
        List<MappedReference> mappedReferences = new ArrayList<>();
        mappedReferences.add(
                new UniProtKBMappedReferenceBuilder()
                        .uniProtKBAccession("P21802")
                        .citationId("CI-ASDKJIU12")
                        .source(
                                new MappedSourceBuilder()
                                        .id("CMNT_ID")
                                        .name(UniProtKBEntryType.SWISSPROT.getName())
                                        .build())
                        .sourceCategoriesAdd("Interaction")
                        .sourceCategoriesAdd("Function")
                        .referencePositionsAdd("rpValue")
                        .referenceCommentsAdd(
                                new ReferenceCommentBuilder()
                                        .type(ReferenceCommentType.TISSUE)
                                        .value("Rcvalue")
                                        .build())
                        .referenceNumber(10)
                        .build());

        Tuple2<String, Iterable<MappedReference>> tuple = new Tuple2<>(accPub, mappedReferences);
        Tuple2<String, PublicationDocument.Builder> result = mapper.call(tuple);
        assertNotNull(result);
        assertNotNull(result._1);
        assertNotNull(result._2);
        PublicationDocument doc = result._2.build();
        assertEquals("P21802", doc.getAccession());
        assertEquals("CI-ASDKJIU12", doc.getCitationId());
        assertEquals("P21802", doc.getAccession());
        assertTrue(doc.getCategories().contains("Interaction"));
        assertTrue(doc.getTypes().contains(2));
        assertNotNull(doc.getPublicationMappedReferences());
        assertEquals(2, doc.getMainType());
        assertEquals(11, doc.getRefNumber());
    }

    @Test
    void mapMultipleMappedReferenceWithoutCategory() throws Exception {
        MappedReferencesToPublicationDocumentBuilderConverter mapper =
                new MappedReferencesToPublicationDocumentBuilderConverter();
        String accPub = "P21802_100";
        List<MappedReference> mappedReferences = new ArrayList<>();
        mappedReferences.add(
                new ComputationallyMappedReferenceBuilder()
                        .citationId("100")
                        .uniProtKBAccession("P21802")
                        .source(new MappedSourceBuilder().id("COMP_ID").name("COMP_NAME").build())
                        .annotation("AnnotationValue")
                        .build());

        mappedReferences.add(
                new CommunityMappedReferenceBuilder()
                        .citationId("100")
                        .uniProtKBAccession("P21802")
                        .source(new MappedSourceBuilder().id("CMNT_ID").name("CMNT_NAME").build())
                        .communityAnnotation(
                                new CommunityAnnotationBuilder().comment("cmValue").build())
                        .build());


        Tuple2<String, Iterable<MappedReference>> tuple = new Tuple2<>(accPub, mappedReferences);
        Tuple2<String, PublicationDocument.Builder> result = mapper.call(tuple);
        assertNotNull(result);
        assertNotNull(result._1);
        assertNotNull(result._2);
        PublicationDocument doc = result._2.build();
        assertEquals("P21802", doc.getAccession());
        assertEquals("100", doc.getCitationId());
        assertEquals("P21802", doc.getAccession());
        assertTrue(doc.getCategories().contains("Unclassified"));
        assertTrue(doc.getTypes().contains(0));
        assertTrue(doc.getTypes().contains(1));
        assertNotNull(doc.getPublicationMappedReferences());
        assertEquals(1, doc.getMainType());
    }

}
