package indexer.uniprot.converter;

import static org.junit.jupiter.api.Assertions.*;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.uniprot.core.builder.SequenceBuilder;
import org.uniprot.core.flatfile.parser.impl.cc.CcLineTransformer;
import org.uniprot.core.uniprot.UniProtEntry;
import org.uniprot.core.uniprot.UniProtEntryType;
import org.uniprot.core.uniprot.builder.UniProtEntryBuilder;
import org.uniprot.core.uniprot.comment.Comment;
import org.uniprot.store.search.document.uniprot.UniProtDocument;

/**
 * @author lgonzales
 * @since 2019-09-11
 */
class UniProtEntryCommentsConverterTest {

    private static final String CC_ALTERNATIVE_PRODUCTS_FIELD = "cc_alternative_products";
    private static final String CCEV_ALTERNATIVE_PRODUCTS_FIELD = "ccev_alternative_products";
    private static final String CC_COFACTOR_FIELD = "cc_cofactor";
    private static final String CCEV_COFACTOR_FIELD = "ccev_cofactor";
    private static final String CC_BIOPHYSICOCHEMICAL_PROPERTIES_FIELD =
            "cc_biophysicochemical_properties";
    private static final String CCEV_BIOPHYSICOCHEMICAL_PROPERTIES_FIELD =
            "ccev_biophysicochemical_properties";
    private static final String CC_SEQUENCE_CAUTION_FIELD = "cc_sequence_caution";
    private static final String CCEV_SEQUENCE_CAUTION_FIELD = "ccev_sequence_caution";
    private static final String CC_SUBCELLULAR_LOCATION_FIELD = "cc_subcellular_location";
    private static final String CCEV_SUBCELLULAR_LOCATION_FIELD = "ccev_subcellular_location";
    private static final String CCEV_CATALYTIC_ACTIVITY = "ccev_catalytic_activity";
    private static final CcLineTransformer ccLineTransformer =
            new CcLineTransformer("disease/humdisease.txt", "subcell/subcell.txt");

    @Test
    void testCatalyticActivityCommentConvertProperlyToDocument() {
        String catalyticActivityLine =
                "CC   -!- CATALYTIC ACTIVITY:\n"
                        + "CC       Reaction=ATP + L-tyrosyl-[protein] = ADP + H(+) + O-phospho-L-\n"
                        + "CC         tyrosyl-[protein]; Xref=Rhea:RHEA:10596, Rhea:RHEA-COMP:10136,\n"
                        + "CC         Rhea:RHEA-COMP:10137, ChEBI:CHEBI:15378, ChEBI:CHEBI:30616,\n"
                        + "CC         ChEBI:CHEBI:46858, ChEBI:CHEBI:82620, ChEBI:CHEBI:456216;\n"
                        + "CC         EC=2.7.10.1; Evidence={ECO:0000255|PROSITE-ProRule:PRU10028,\n"
                        + "CC         ECO:0000269|PubMed:16844695, ECO:0000269|PubMed:18056630,\n"
                        + "CC         ECO:0000269|PubMed:19410646, ECO:0000269|PubMed:21454610};";

        UniProtEntry entry = createUniProtEntryFromCommentLine(catalyticActivityLine);

        UniProtEntryCommentsConverter converter =
                new UniProtEntryCommentsConverter(new HashMap<>());
        UniProtDocument document = new UniProtDocument();
        converter.convertCommentToDocument(entry.getComments(), document);
        assertNotNull(document);

        String expectedIndexed =
                "CATALYTIC ACTIVITY:\n"
                        + "Reaction=ATP + L-tyrosyl-[protein] = ADP + H(+) + O-phospho-L-tyrosyl-[protein]; Xref=Rhea:RHEA:10596, "
                        + "Rhea:RHEA-COMP:10136, Rhea:RHEA-COMP:10137, ChEBI:CHEBI:15378, ChEBI:CHEBI:30616, ChEBI:CHEBI:46858, "
                        + "ChEBI:CHEBI:82620, ChEBI:CHEBI:456216; EC=2.7.10.1;";
        assertTrue(document.commentMap.containsKey("cc_catalytic_activity"));
        assertEquals(9, document.commentMap.get("cc_catalytic_activity").size());
        assertTrue(document.commentMap.get("cc_catalytic_activity").contains(expectedIndexed));
        assertTrue(document.commentMap.get("cc_catalytic_activity").contains("RHEA-COMP:10136"));
        assertTrue(document.commentMap.get("cc_catalytic_activity").contains("RHEA:10596"));
        assertTrue(document.commentMap.get("cc_catalytic_activity").contains("CHEBI:456216"));

        assertTrue(document.content.contains(expectedIndexed));

        assertTrue(document.commentEvMap.containsKey(CCEV_CATALYTIC_ACTIVITY));
        assertEquals(4, document.commentEvMap.get(CCEV_CATALYTIC_ACTIVITY).size());
        assertTrue(document.commentEvMap.get(CCEV_CATALYTIC_ACTIVITY).contains("ECO_0000269"));
        assertTrue(document.commentEvMap.get(CCEV_CATALYTIC_ACTIVITY).contains("ECO_0000255"));
        assertTrue(document.commentEvMap.get(CCEV_CATALYTIC_ACTIVITY).contains("experimental"));
        assertTrue(document.commentEvMap.get(CCEV_CATALYTIC_ACTIVITY).contains("manual"));

        assertEquals(1, document.proteinsWith.size());
        assertTrue(document.proteinsWith.contains("catalytic_activity"));
    }

    @Test
    void testPathwayCommentConvertProperlyToDocument() {
        Map<String, String> pathway = new HashMap<>();
        pathway.put("Lipid metabolism; glycerolipid metabolism", "PW-1233");
        pathway.put("Sphingolipid metabolism", "PW-1234");

        String pathwayLine =
                "CC   -!- PATHWAY: Lipid metabolism; glycerolipid metabolism.\n"
                        + "CC       {ECO:0000269|PubMed:15252046}.\n"
                        + "CC   -!- PATHWAY: Protein modification; protein glycosylation.\n"
                        + "CC   -!- PATHWAY: Sphingolipid metabolism.";

        UniProtEntry entry = createUniProtEntryFromCommentLine(pathwayLine);

        UniProtEntryCommentsConverter converter = new UniProtEntryCommentsConverter(pathway);
        UniProtDocument document = new UniProtDocument();
        converter.convertCommentToDocument(entry.getComments(), document);
        assertNotNull(document);

        assertEquals(Arrays.asList("PW-1233", "PW-1234"), document.pathway);

        assertEquals(1, document.proteinsWith.size());
        assertTrue(document.proteinsWith.contains("pathway"));

        assertTrue(document.commentMap.containsKey("cc_pathway"));
        assertEquals(3, document.commentMap.get("cc_pathway").size());
        assertTrue(
                document.commentMap
                        .get("cc_pathway")
                        .contains("PATHWAY: Lipid metabolism; glycerolipid metabolism."));
        assertTrue(
                document.commentMap
                        .get("cc_pathway")
                        .contains("PATHWAY: Protein modification; protein glycosylation."));
        assertTrue(
                document.commentMap
                        .get("cc_pathway")
                        .contains("PATHWAY: Sphingolipid metabolism."));

        assertEquals(1, document.commentEvMap.size());
        assertTrue(document.commentEvMap.containsKey("ccev_pathway"));
        assertEquals(3, document.commentEvMap.get("ccev_pathway").size());
        assertTrue(document.commentEvMap.get("ccev_pathway").contains("ECO_0000269"));
        assertTrue(document.commentEvMap.get("ccev_pathway").contains("experimental"));
        assertTrue(document.commentEvMap.get("ccev_pathway").contains("manual"));

        assertTrue(
                document.content.contains("PATHWAY: Lipid metabolism; glycerolipid metabolism."));
        assertTrue(
                document.content.contains("PATHWAY: Protein modification; protein glycosylation."));
        assertTrue(document.content.contains("PATHWAY: Sphingolipid metabolism."));
        assertTrue(document.content.contains("PW-1233"));
        assertTrue(document.content.contains("PW-1234"));
    }

    @Test
    void testInteractionCommentConvertProperlyToDocument() {
        String interactionLine =
                "CC   -!- INTERACTION:\n"
                        + "CC       Self; NbExp=106; IntAct=EBI-77613, EBI-77613;\n"
                        + "CC       Q306T3:- (xeno); NbExp=3; IntAct=EBI-77613, EBI-8294101;\n"
                        + "CC       P31696:AGRN (xeno); NbExp=3; IntAct=EBI-2431589, EBI-457650;\n"
                        + "CC       Q02410:APBA1; NbExp=4; IntAct=EBI-77613, EBI-368690;\n"
                        + "CC       Q99767:APBA2; NbExp=2; IntAct=EBI-77613, EBI-81711;";

        String interactionIndexedString =
                "INTERACTION:\n"
                        + "Self; NbExp=106; IntAct=EBI-77613, EBI-77613;\n"
                        + "Q306T3:- (xeno); NbExp=3; IntAct=EBI-77613, EBI-8294101;\n"
                        + "P31696:AGRN (xeno); NbExp=3; IntAct=EBI-2431589, EBI-457650;\n"
                        + "Q02410:APBA1; NbExp=4; IntAct=EBI-77613, EBI-368690;\n"
                        + "Q99767:APBA2; NbExp=2; IntAct=EBI-77613, EBI-81711;";

        UniProtEntry entry = createUniProtEntryFromCommentLine(interactionLine);

        UniProtEntryCommentsConverter converter =
                new UniProtEntryCommentsConverter(new HashMap<>());
        UniProtDocument document = new UniProtDocument();
        converter.convertCommentToDocument(entry.getComments(), document);
        assertNotNull(document);

        assertEquals(10, document.interactors.size());
        assertTrue(document.interactors.contains("EBI-2431589"));
        assertTrue(document.interactors.contains("EBI-368690"));
        assertTrue(document.interactors.contains("Q99767"));
        assertTrue(document.interactors.contains("P31696"));
        assertTrue(document.interactors.contains("EBI-81711"));

        assertEquals(1, document.proteinsWith.size());
        assertTrue(document.proteinsWith.contains("interaction"));

        assertTrue(document.commentMap.containsKey("cc_interaction"));
        assertEquals(1, document.commentMap.get("cc_interaction").size());
        assertTrue(document.commentMap.get("cc_interaction").contains(interactionIndexedString));
        assertEquals(1, document.commentEvMap.size());
        assertTrue(document.commentEvMap.containsKey("ccev_interaction"));
        assertEquals(
                0,
                document.commentEvMap
                        .get("ccev_interaction")
                        .size()); // interaction do not have evidence

        assertTrue(document.content.contains(interactionIndexedString));
    }

    @Test
    void testSimilarityCommentConvertProperlyToDocument() {
        String similarityLine =
                "CC   -!- SIMILARITY: Belongs to the potassium channel family. C (Shaw) (TC\n"
                        + "CC       1.A.1.2) subfamily. Kv3.2/KCNC2 sub-subfamily. {ECO:0000305}.";

        String similarityIndexedString =
                "SIMILARITY: Belongs to the potassium channel family. "
                        + "C (Shaw) (TC 1.A.1.2) subfamily. Kv3.2/KCNC2 sub-subfamily.";

        UniProtEntry entry = createUniProtEntryFromCommentLine(similarityLine);

        UniProtEntryCommentsConverter converter =
                new UniProtEntryCommentsConverter(new HashMap<>());
        UniProtDocument document = new UniProtDocument();
        converter.convertCommentToDocument(entry.getComments(), document);
        assertNotNull(document);

        assertEquals(1, document.familyInfo.size());
        assertTrue(
                document.familyInfo.contains(
                        "potassium channel family, C (Shaw) (TC 1.A.1.2) subfamily, Kv3.2/KCNC2 sub-subfamily"));

        assertEquals(0, document.proteinsWith.size()); // similarity is filtered out

        assertTrue(document.commentMap.containsKey("cc_similarity"));
        assertEquals(1, document.commentMap.get("cc_similarity").size());
        assertTrue(document.commentMap.get("cc_similarity").contains(similarityIndexedString));
        assertEquals(1, document.commentEvMap.size());
        assertTrue(document.commentEvMap.containsKey("ccev_similarity"));
        assertEquals(2, document.commentEvMap.get("ccev_similarity").size());
        assertTrue(document.commentEvMap.get("ccev_similarity").contains("ECO_0000305"));
        assertTrue(document.commentEvMap.get("ccev_similarity").contains("manual"));
        assertTrue(document.content.contains(similarityIndexedString));
    }

    @Test
    void testAlternativeProductsCommentConvertProperlyToDocument() {
        String alternativeLine =
                "CC   -!- ALTERNATIVE PRODUCTS:\n"
                        + "CC       Event=Alternative promoter usage, Alternative initiation; Named isoforms=3;\n"
                        + "CC       Name=Genome polyprotein;\n"
                        + "CC         IsoId=Q672I1-1; Sequence=Displayed;\n"
                        + "CC         Note=Produced from the genomic RNA.;\n"
                        + "CC       Name=Subgenomic capsid protein; Synonyms=VP1;\n"
                        + "CC         IsoId=Q672I1-2; Sequence=VSP_034391;\n"
                        + "CC         Note=Produced from the subgenomic RNA by alternative promoter\n"
                        + "CC         usage.;\n"
                        + "CC       Name=Uncharacterized protein VP3;\n"
                        + "CC         IsoId=Q672I0-1; Sequence=External;\n"
                        + "CC         Note=Produced by alternative initiation from the subgenomic\n"
                        + "CC         RNA.;";

        String alternativeProductsLine =
                "ALTERNATIVE PRODUCTS:\n"
                        + "Event=Alternative promoter usage, Alternative initiation; Named isoforms=3;\n"
                        + "Name=Genome polyprotein;\n"
                        + "IsoId=Q672I1-1; Sequence=Displayed;\n"
                        + "Note=Produced from the genomic RNA.;\n"
                        + "Name=Subgenomic capsid protein; Synonyms=VP1;\n"
                        + "IsoId=Q672I1-2; Sequence=VSP_034391;\n"
                        + "Note=Produced from the subgenomic RNA by alternative promoter usage.;\n"
                        + "Name=Uncharacterized protein VP3;\n"
                        + "IsoId=Q672I0-1; Sequence=External;\n"
                        + "Note=Produced by alternative initiation from the subgenomic RNA.;";
        UniProtEntry entry = createUniProtEntryFromCommentLine(alternativeLine);

        UniProtEntryCommentsConverter converter =
                new UniProtEntryCommentsConverter(new HashMap<>());
        UniProtDocument document = new UniProtDocument();
        converter.convertCommentToDocument(entry.getComments(), document);
        assertNotNull(document);

        assertEquals(1, document.proteinsWith.size());
        assertTrue(document.proteinsWith.contains("alternative_products"));

        assertTrue(document.commentMap.containsKey(CC_ALTERNATIVE_PRODUCTS_FIELD));
        assertTrue(
                document.commentMap
                        .get(CC_ALTERNATIVE_PRODUCTS_FIELD)
                        .contains(alternativeProductsLine));

        assertEquals(1, document.commentEvMap.size());
        assertTrue(document.commentEvMap.containsKey(CCEV_ALTERNATIVE_PRODUCTS_FIELD));
        assertEquals(0, document.commentEvMap.get(CCEV_ALTERNATIVE_PRODUCTS_FIELD).size());

        assertEquals(5, document.ap.size());
        assertTrue(document.ap.contains("Alternative promoter usage"));
        assertTrue(document.ap.contains("Produced from the genomic RNA."));
        assertEquals(0, document.apEv.size());
        assertEquals(3, document.apApu.size());
        assertTrue(document.apApu.contains("Produced from the genomic RNA."));
        assertTrue(
                document.apApu.contains(
                        "Produced by alternative initiation from the subgenomic RNA."));
        assertEquals(0, document.apApuEv.size());

        assertTrue(document.content.contains(alternativeProductsLine));
    }

    @Test
    void testAlternativeProductsRibosomalFrameshiftingCommentConvertProperlyToDocument() {
        String alternativeLine =
                "CC   -!- ALTERNATIVE PRODUCTS:\n"
                        + "CC       Event=Ribosomal frameshifting; Named isoforms=2;\n"
                        + "CC       Name=Genome polyprotein;\n"
                        + "CC         IsoId=P12296-1; Sequence=Displayed;\n"
                        + "CC         Note=Produced by conventional translation.\n"
                        + "CC         {ECO:0000269|PubMed:22025686};\n"
                        + "CC       Name=2B*;\n"
                        + "CC         IsoId=P0DJX8-1; Sequence=External;\n"
                        + "CC         Note=Produced by -1 ribosomal frameshifting. The N-terminus is\n"
                        + "CC         translated following a ribosomal skip event.\n"
                        + "CC         {ECO:0000269|PubMed:22025686};";

        String alternativeProductsLine =
                "ALTERNATIVE PRODUCTS:\n"
                        + "Event=Ribosomal frameshifting; Named isoforms=2;\n"
                        + "Name=Genome polyprotein;\n"
                        + "IsoId=P12296-1; Sequence=Displayed;\n"
                        + "Note=Produced by conventional translation.;\n"
                        + "Name=2B*;\n"
                        + "IsoId=P0DJX8-1; Sequence=External;\n"
                        + "Note=Produced by -1 ribosomal frameshifting. The N-terminus is translated following a ribosomal skip event.;";
        UniProtEntry entry = createUniProtEntryFromCommentLine(alternativeLine);

        UniProtEntryCommentsConverter converter =
                new UniProtEntryCommentsConverter(new HashMap<>());
        UniProtDocument document = new UniProtDocument();
        converter.convertCommentToDocument(entry.getComments(), document);
        assertNotNull(document);

        assertEquals(1, document.proteinsWith.size());
        assertTrue(document.proteinsWith.contains("alternative_products"));

        assertTrue(document.commentMap.containsKey(CC_ALTERNATIVE_PRODUCTS_FIELD));
        assertTrue(
                document.commentMap
                        .get(CC_ALTERNATIVE_PRODUCTS_FIELD)
                        .contains(alternativeProductsLine));

        assertEquals(1, document.commentEvMap.size());
        assertTrue(document.commentEvMap.containsKey(CCEV_ALTERNATIVE_PRODUCTS_FIELD));
        assertEquals(0, document.commentEvMap.get(CCEV_ALTERNATIVE_PRODUCTS_FIELD).size());

        assertEquals(3, document.ap.size());
        assertTrue(document.ap.contains("Ribosomal frameshifting"));
        assertTrue(document.ap.contains("Produced by conventional translation."));
        assertEquals(3, document.apEv.size());
        assertTrue(document.apEv.contains("ECO_0000269"));
        assertEquals(2, document.apRf.size());
        assertTrue(document.apRf.contains("Produced by conventional translation."));
        assertTrue(
                document.apRf.contains(
                        "Produced by -1 ribosomal frameshifting. The N-terminus is translated following a ribosomal skip event."));
        assertEquals(3, document.apRfEv.size());
        assertTrue(document.apRfEv.contains("ECO_0000269"));

        assertTrue(document.content.contains(alternativeProductsLine));
    }

    @Test
    void testCofactorCommentConvertProperlyToDocument() {
        String cofactorLine =
                "CC   -!- COFACTOR: [RNA-directed RNA polymerase]:\n"
                        + "CC       Name=Mg(2+); Xref=ChEBI:CHEBI:18420;\n"
                        + "CC         Evidence={ECO:0000250|UniProtKB:P03313};\n"
                        + "CC       Note=Requires the presence of 3CDpro or 3CPro.\n"
                        + "CC       {ECO:0000250|UniProtKB:P03313};";
        String cofactorLineValue =
                "COFACTOR: [RNA-directed RNA polymerase]:\n"
                        + "Name=Mg(2+); Xref=ChEBI:CHEBI:18420;\n"
                        + "Note=Requires the presence of 3CDpro or 3CPro.;";

        UniProtEntry entry = createUniProtEntryFromCommentLine(cofactorLine);
        UniProtEntryCommentsConverter converter =
                new UniProtEntryCommentsConverter(new HashMap<>());
        UniProtDocument document = new UniProtDocument();
        converter.convertCommentToDocument(entry.getComments(), document);
        assertNotNull(document);

        assertEquals(1, document.proteinsWith.size());
        assertTrue(document.proteinsWith.contains("cofactor"));

        assertEquals(1, document.commentMap.keySet().size());

        assertTrue(document.commentMap.containsKey(CC_COFACTOR_FIELD));
        assertTrue(document.commentMap.get(CC_COFACTOR_FIELD).contains(cofactorLineValue));
        assertEquals(1, document.commentEvMap.size());
        assertTrue(document.commentEvMap.containsKey(CCEV_COFACTOR_FIELD));
        assertEquals(0, document.commentEvMap.get(CCEV_COFACTOR_FIELD).size());

        assertEquals(3, document.cofactorChebi.size());
        assertTrue(document.cofactorChebi.contains("Mg(2+)"));
        assertEquals(2, document.cofactorChebiEv.size());
        assertTrue(document.cofactorChebiEv.contains("manual"));

        assertEquals(1, document.cofactorNote.size());
        assertTrue(document.cofactorNote.contains("Requires the presence of 3CDpro or 3CPro."));
        assertEquals(2, document.cofactorNoteEv.size());
        assertTrue(document.cofactorNoteEv.contains("ECO_0000250"));

        assertTrue(
                document.content.contains(
                        "COFACTOR: [RNA-directed RNA polymerase]:\n"
                                + "Name=Mg(2+); Xref=ChEBI:CHEBI:18420;\n"
                                + "Note=Requires the presence of 3CDpro or 3CPro.;"));
    }

    @Test
    void testBPCPCommentConvertProperlyToDocument() {
        String bpcpLine =
                "CC   -!- BIOPHYSICOCHEMICAL PROPERTIES:\n"
                        + "CC       pH dependence:\n"
                        + "CC         Optimum pH is 5.0 for protease activity.\n"
                        + "CC         {ECO:0000269|PubMed:16603535};\n"
                        + "CC   -!- BIOPHYSICOCHEMICAL PROPERTIES:\n"
                        + "CC       Absorption:\n"
                        + "CC         Abs(max)=550 nm {ECO:0000269|PubMed:10510276};\n"
                        + "CC         Note=Shoulder at 335 nm (at pH 7.5 and 30 degrees Celsius).\n"
                        + "CC         {ECO:0000269|PubMed:22547782};\n"
                        + "CC       Kinetic parameters:\n"
                        + "CC         KM=9 uM for AMP (at pH 5.5 and 25 degrees Celsius)\n"
                        + "CC         {ECO:0000269|PubMed:10510276};\n"
                        + "CC         KM=9 uM for pyrophosphate (at pH 5.5 and 25 degrees Celsius)\n"
                        + "CC         {ECO:0000269|PubMed:10510276};\n"
                        + "CC         KM=30 uM for beta-glycerophosphate (at pH 5.5 and 25 degrees\n"
                        + "CC         Celsius) {ECO:0000269|PubMed:10510276};\n"
                        + "CC   -!- BIOPHYSICOCHEMICAL PROPERTIES:\n"
                        + "CC       Kinetic parameters:\n"
                        + "CC         KM=27 mM for L-proline (at 25 degrees Celsius)\n"
                        + "CC         {ECO:0000269|PubMed:17344208};\n"
                        + "CC         KM=4 mM for 3,4-dehydro-L-proline (at 25 degrees Celsius)\n"
                        + "CC         {ECO:0000269|PubMed:17344208};\n"
                        + "CC         Vmax=20.5 umol/min/mg enzyme for L-proline (at 25 degrees\n"
                        + "CC         Celsius) {ECO:0000269|PubMed:17344208};\n"
                        + "CC         Vmax=119 umol/min/mg enzyme for 3,4-dehydro-L-proline (at 25\n"
                        + "CC         degrees Celsius) {ECO:0000269|PubMed:17344208};\n"
                        + "CC         Note=kcat is 13 s(-1) for L-proline. kcat is 75 s(-1) for 3,4-\n"
                        + "CC         dehydro-L-proline. {ECO:0000269|PubMed:17344208};\n"
                        + "CC       Redox potential:\n"
                        + "CC         E(0) is -75 mV. {ECO:0000269|PubMed:17344208};\n"
                        + "CC       Temperature dependence:\n"
                        + "CC         Highly thermostable. Exhibits over 85% or 60% of activity after.\n"
                        + "CC         {ECO:0000269|PubMed:17344208};";

        String phdependenceLineValue =
                "BIOPHYSICOCHEMICAL PROPERTIES:\n"
                        + "pH dependence:\n"
                        + "Optimum pH is 5.0 for protease activity.;";

        UniProtEntry entry = createUniProtEntryFromCommentLine(bpcpLine);
        UniProtEntryCommentsConverter converter =
                new UniProtEntryCommentsConverter(new HashMap<>());
        UniProtDocument document = new UniProtDocument();
        converter.convertCommentToDocument(entry.getComments(), document);
        assertNotNull(document);
        assertEquals(3, document.commentMap.get(CC_BIOPHYSICOCHEMICAL_PROPERTIES_FIELD).size());

        assertEquals(1, document.proteinsWith.size());
        assertTrue(document.proteinsWith.contains("biophysicochemical_properties"));

        assertTrue(document.commentMap.containsKey(CC_BIOPHYSICOCHEMICAL_PROPERTIES_FIELD));
        assertTrue(
                document.commentMap
                        .get(CC_BIOPHYSICOCHEMICAL_PROPERTIES_FIELD)
                        .contains(phdependenceLineValue));

        assertEquals(1, document.commentEvMap.size());
        assertTrue(document.commentEvMap.containsKey(CCEV_BIOPHYSICOCHEMICAL_PROPERTIES_FIELD));
        assertEquals(0, document.commentEvMap.get(CCEV_BIOPHYSICOCHEMICAL_PROPERTIES_FIELD).size());

        assertEquals(19, document.bpcp.size());
        assertTrue(document.bpcp.contains("550"));
        assertEquals(3, document.bpcpEv.size());
        assertTrue(document.bpcpEv.contains("ECO_0000269"));

        assertEquals(2, document.bpcpAbsorption.size());
        assertTrue(
                document.bpcpAbsorption.contains(
                        "Shoulder at 335 nm (at pH 7.5 and 30 degrees Celsius)."));
        assertEquals(3, document.bpcpAbsorptionEv.size());
        assertTrue(document.bpcpAbsorptionEv.contains("experimental"));

        assertEquals(14, document.bpcpKinetics.size());
        assertTrue(
                document.bpcpKinetics.contains(
                        "kcat is 13 s(-1) for L-proline. kcat is 75 s(-1) for 3,4-dehydro-L-proline."));
        assertEquals(3, document.bpcpKineticsEv.size());
        assertTrue(document.bpcpKineticsEv.contains("manual"));

        assertEquals(1, document.bpcpPhDependence.size());
        assertTrue(document.bpcpPhDependence.contains("Optimum pH is 5.0 for protease activity."));
        assertEquals(3, document.bpcpPhDependenceEv.size());
        assertTrue(document.bpcpPhDependenceEv.contains("ECO_0000269"));

        assertEquals(1, document.bpcpRedoxPotential.size());
        assertTrue(document.bpcpRedoxPotential.contains("E(0) is -75 mV."));
        assertEquals(3, document.bpcpRedoxPotentialEv.size());
        assertTrue(document.bpcpRedoxPotentialEv.contains("experimental"));

        assertEquals(1, document.bpcpTempDependence.size());
        assertTrue(
                document.bpcpTempDependence.contains(
                        "Highly thermostable. Exhibits over 85% or 60% of activity after."));
        assertEquals(3, document.bpcpTempDependenceEv.size());
        assertTrue(document.bpcpTempDependenceEv.contains("manual"));

        assertEquals(3, document.content.size());
        assertTrue(document.content.contains(phdependenceLineValue));
    }

    @Test
    void testSequenceCautionCommentConvertProperlyToDocument() throws Exception {
        String sequenceCautionLine =
                "CC   -!- SEQUENCE CAUTION:\n"
                        + "CC       Sequence=CAB59730.1; Type=Frameshift; Evidence={ECO:0000305};\n"
                        + "CC   -!- SEQUENCE CAUTION:\n"
                        + "CC       Sequence=AAA42785.1; Type=Erroneous gene model prediction; Evidence={ECO:0000305};\n"
                        + "CC   -!- SEQUENCE CAUTION:\n"
                        + "CC       Sequence=AAA03332.1; Type=Erroneous initiation; Evidence={ECO:0000305};\n"
                        + "CC   -!- SEQUENCE CAUTION:\n"
                        + "CC       Sequence=AAB25832.2; Type=Erroneous translation; Note=Wrong choice of frame.; Evidence={ECO:0000305};\n"
                        + "CC   -!- SEQUENCE CAUTION:\n"
                        + "CC       Sequence=BAB43866.1; Type=Miscellaneous discrepancy; Note=Chimeric cDNA. It is a chimera between Dox-A3 and PPO2.; Evidence={ECO:0000305};\n"
                        + "CC   -!- SEQUENCE CAUTION:\n"
                        + "CC       Sequence=CAH10679.1; Type=Erroneous termination; Note=Translated as Trp.; Evidence={ECO:0000305};";

        String sequenceCautionLineValue =
                "SEQUENCE CAUTION:\n" + "Sequence=CAB59730.1; Type=Frameshift;";
        UniProtEntry entry = createUniProtEntryFromCommentLine(sequenceCautionLine);

        UniProtEntryCommentsConverter converter =
                new UniProtEntryCommentsConverter(new HashMap<>());
        UniProtDocument document = new UniProtDocument();
        converter.convertCommentToDocument(entry.getComments(), document);

        assertNotNull(document);
        assertEquals(6, document.commentMap.get(CC_SEQUENCE_CAUTION_FIELD).size());

        assertEquals(0, document.proteinsWith.size());

        assertTrue(document.commentMap.containsKey(CC_SEQUENCE_CAUTION_FIELD));
        assertTrue(
                document.commentMap
                        .get(CC_SEQUENCE_CAUTION_FIELD)
                        .contains(sequenceCautionLineValue));
        assertEquals(1, document.commentEvMap.size());
        assertTrue(document.commentEvMap.containsKey(CCEV_SEQUENCE_CAUTION_FIELD));
        assertEquals(0, document.commentEvMap.get(CCEV_SEQUENCE_CAUTION_FIELD).size());

        assertEquals(9, document.seqCaution.size());
        assertTrue(document.seqCaution.contains("Translated as Trp."));
        assertTrue(document.seqCaution.contains("Frameshift"));
        assertTrue(document.seqCaution.contains("Erroneous initiation"));

        assertEquals(2, document.seqCautionEv.size());
        assertTrue(document.seqCautionEv.contains("ECO_0000305"));

        assertEquals(1, document.seqCautionErInit.size());
        assertTrue(document.seqCautionErInit.contains("true"));

        assertEquals(1, document.seqCautionErPred.size());
        assertTrue(document.seqCautionErPred.contains("true"));

        assertEquals(1, document.seqCautionErTerm.size());
        assertTrue(document.seqCautionErTerm.contains("Translated as Trp."));

        assertEquals(1, document.seqCautionErTran.size());
        assertTrue(document.seqCautionErTran.contains("Wrong choice of frame."));

        assertEquals(1, document.seqCautionFrameshift.size());
        assertTrue(document.seqCautionFrameshift.contains("true"));

        assertEquals(1, document.seqCautionMisc.size());
        assertTrue(
                document.seqCautionMisc.contains(
                        "Chimeric cDNA. It is a chimera between Dox-A3 and PPO2."));

        assertEquals(2, document.seqCautionMiscEv.size());
        assertTrue(document.seqCautionMiscEv.contains("manual"));

        assertEquals(6, document.content.size());
        assertTrue(document.content.contains(sequenceCautionLineValue));
    }

    @Test
    void testSubcellularLocationCommentConvertProperlyToDocument() throws Exception {
        String subcellularLocationLine =
                "CC   -!- SUBCELLULAR LOCATION: [Capsid protein]: Virion. Host cytoplasm.\n"
                        + "CC   -!- SUBCELLULAR LOCATION: [Small envelope protein M]: Virion membrane\n"
                        + "CC       {ECO:0000250|UniProtKB:P03314}; Multi-pass membrane protein\n"
                        + "CC       {ECO:0000250|UniProtKB:P03314}. Host endoplasmic reticulum\n"
                        + "CC       membrane {ECO:0000250|UniProtKB:P03314}; Multi-pass membrane\n"
                        + "CC       protein {ECO:0000255}. Note=ER membrane retention is mediated by\n"
                        + "CC       the transmembrane domains. {ECO:0000250|UniProtKB:P03314}.\n"
                        + "CC   -!- SUBCELLULAR LOCATION: Cell membrane {ECO:0000305|PubMed:22512337};\n"
                        + "CC       Lipid-anchor {ECO:0000250|UniProtKB:Q7M759}; Cytoplasmic side\n"
                        + "CC       {ECO:0000305|PubMed:22512337}. Cytoplasmic vesicle membrane\n"
                        + "CC       {ECO:0000305|PubMed:22512337}; Lipid-anchor\n"
                        + "CC       {ECO:0000250|UniProtKB:Q7M759}; Cytoplasmic side\n"
                        + "CC       {ECO:0000305|PubMed:22512337}. Note=In neurons, localizes to the\n"
                        + "CC       sensory endings and to cytoplasmic punctate structures.";

        String subcellularLocationLineValue =
                "SUBCELLULAR LOCATION: [Capsid protein]: Virion. Host cytoplasm.";
        UniProtEntry entry = createUniProtEntryFromCommentLine(subcellularLocationLine);

        UniProtEntryCommentsConverter converter =
                new UniProtEntryCommentsConverter(new HashMap<>());
        UniProtDocument document = new UniProtDocument();
        converter.convertCommentToDocument(entry.getComments(), document);

        assertNotNull(document);
        assertEquals(3, document.commentMap.get(CC_SUBCELLULAR_LOCATION_FIELD).size());

        assertEquals(1, document.proteinsWith.size());
        assertTrue(document.proteinsWith.contains("subcellular_location"));

        assertTrue(document.commentMap.containsKey(CC_SUBCELLULAR_LOCATION_FIELD));
        assertTrue(
                document.commentMap
                        .get(CC_SUBCELLULAR_LOCATION_FIELD)
                        .contains(subcellularLocationLineValue));
        assertEquals(1, document.commentEvMap.size());
        assertTrue(document.commentEvMap.containsKey(CCEV_SUBCELLULAR_LOCATION_FIELD));
        assertEquals(0, document.commentEvMap.get(CCEV_SUBCELLULAR_LOCATION_FIELD).size());

        assertEquals(18, document.subcellLocationTerm.size());
        assertTrue(document.subcellLocationTerm.contains("Host cytoplasm"));
        assertTrue(document.subcellLocationTerm.contains("SL-0381"));

        assertEquals(4, document.subcellLocationTermEv.size());
        assertTrue(document.subcellLocationTermEv.contains("ECO_0000255"));

        assertEquals(2, document.subcellLocationNote.size());
        assertTrue(
                document.subcellLocationNote.contains(
                        "ER membrane retention is mediated by the transmembrane domains"));

        assertEquals(2, document.subcellLocationNoteEv.size());
        assertTrue(document.subcellLocationNoteEv.contains("ECO_0000250"));

        assertTrue(document.content.contains(subcellularLocationLineValue));
        assertTrue(document.content.contains("SL-0390"));
    }

    @Test
    void testDiseaseCommentConvertProperlyToDocument() throws Exception {
        String diseaseCommentLine =
                "CC   -!- DISEASE: Jackson-Weiss syndrome (JWS) [MIM:123150]: An autosomal\n"
                        + "CC       dominant craniosynostosis syndrome characterized by craniofacial\n"
                        + "CC       abnormalities and abnormality of the feet: broad great toes with\n"
                        + "CC       medial deviation and tarsal-metatarsal coalescence.\n"
                        + "CC       {ECO:0000269|PubMed:7874170, ECO:0000269|PubMed:8528214,\n"
                        + "CC       ECO:0000269|PubMed:8644708, ECO:0000269|PubMed:9385368,\n"
                        + "CC       ECO:0000269|PubMed:9677057}. Note=The disease is caused by\n"
                        + "CC       mutations affecting the gene represented in this entry.";

        String indexedDiseaseComment =
                "DISEASE: Jackson-Weiss syndrome (JWS) [MIM:123150]: An autosomal dominant "
                        + "craniosynostosis syndrome characterized by craniofacial abnormalities and abnormality of the feet: "
                        + "broad great toes with medial deviation and tarsal-metatarsal coalescence. Note=The disease is caused "
                        + "by mutations affecting the gene represented in this entry.";

        UniProtEntry entry = createUniProtEntryFromCommentLine(diseaseCommentLine);

        UniProtEntryCommentsConverter converter =
                new UniProtEntryCommentsConverter(new HashMap<>());
        UniProtDocument document = new UniProtDocument();
        converter.convertCommentToDocument(entry.getComments(), document);
        assertNotNull(document);

        assertEquals(1, document.proteinsWith.size());
        assertTrue(document.proteinsWith.contains("disease"));

        assertTrue(document.commentMap.containsKey("cc_disease"));
        assertEquals(2, document.commentMap.get("cc_disease").size());
        assertTrue(document.commentMap.get("cc_disease").contains(indexedDiseaseComment));
        assertTrue(document.commentMap.get("cc_disease").contains("DI-00602"));
        assertEquals(1, document.commentEvMap.size());
        assertTrue(document.commentEvMap.containsKey("ccev_disease"));
        assertEquals(3, document.commentEvMap.get("ccev_disease").size());
        assertTrue(document.commentEvMap.get("ccev_disease").contains("ECO_0000269"));
        assertTrue(document.commentEvMap.get("ccev_disease").contains("experimental"));
        assertTrue(document.commentEvMap.get("ccev_disease").contains("manual"));

        assertTrue(document.content.contains(indexedDiseaseComment));
        assertTrue(document.content.contains("DI-00602"));
    }

    @Test
    void testRNACommentConvertProperlyToDocument() throws Exception {
        String rnaEditingCommentLine =
                "CC   -!- RNA EDITING: Modified_positions=2179 {ECO:0000269}; Note=The stop\n"
                        + "CC       codon (UAA) at position 2179 is created by RNA editing. Apo B-48,\n"
                        + "CC       derived from the fully edited RNA, is produced only in the\n"
                        + "CC       intestine and is found in chylomicrons. Apo B-48 is a shortened\n"
                        + "CC       form of apo B-100 which lacks the LDL-receptor region. The\n"
                        + "CC       unedited version (apo B-100) is produced by the liver and is found\n"
                        + "CC       in the VLDL and LDL (By similarity). {ECO:0000250};";

        String indexedRnaEditingComment =
                "RNA EDITING: Modified_positions=2179; Note=The stop codon (UAA) at "
                        + "position 2179 is created by RNA editing. Apo B-48, derived from the fully edited RNA, is produced "
                        + "only in the intestine and is found in chylomicrons. Apo B-48 is a shortened form of apo B-100 which "
                        + "lacks the LDL-receptor region. The unedited version (apo B-100) is produced by the liver and "
                        + "is found in the VLDL and LDL (By similarity).;";

        UniProtEntry entry = createUniProtEntryFromCommentLine(rnaEditingCommentLine);

        UniProtEntryCommentsConverter converter =
                new UniProtEntryCommentsConverter(new HashMap<>());
        UniProtDocument document = new UniProtDocument();
        converter.convertCommentToDocument(entry.getComments(), document);
        assertNotNull(document);

        assertEquals(1, document.proteinsWith.size());
        assertTrue(document.proteinsWith.contains("rna_editing"));

        assertTrue(document.commentMap.containsKey("cc_rna_editing"));
        assertEquals(1, document.commentMap.get("cc_rna_editing").size());
        assertTrue(document.commentMap.get("cc_rna_editing").contains(indexedRnaEditingComment));
        assertEquals(1, document.commentEvMap.size());
        assertTrue(document.commentEvMap.containsKey("ccev_rna_editing"));
        assertEquals(4, document.commentEvMap.get("ccev_rna_editing").size());
        assertTrue(document.commentEvMap.get("ccev_rna_editing").contains("ECO_0000250"));
        assertTrue(document.commentEvMap.get("ccev_rna_editing").contains("manual"));
        assertTrue(document.commentEvMap.get("ccev_rna_editing").contains("ECO_0000269"));
        assertTrue(document.commentEvMap.get("ccev_rna_editing").contains("experimental"));

        assertTrue(document.content.contains(indexedRnaEditingComment));
    }

    @Test
    void testMassSpectometryCommentConvertProperlyToDocument() throws Exception {
        String massSpectrometryCommentLine =
                "CC   -!- MASS SPECTROMETRY: Mass=8891.4; Method=Electrospray;\n"
                        + "CC       Note=Strain BALB/c. Without methionine sulfoxide.;\n"
                        + "CC       Evidence={ECO:0000269|PubMed:16876491};";

        String indexedMassSpectrometryComment =
                "MASS SPECTROMETRY: Mass=8891.4; Method=Electrospray; "
                        + "Note=Strain BALB/c. Without methionine sulfoxide.; Evidence={ECO:0000269|PubMed:16876491};";

        UniProtEntry entry = createUniProtEntryFromCommentLine(massSpectrometryCommentLine);

        UniProtEntryCommentsConverter converter =
                new UniProtEntryCommentsConverter(new HashMap<>());
        UniProtDocument document = new UniProtDocument();
        converter.convertCommentToDocument(entry.getComments(), document);
        assertNotNull(document);

        assertEquals(1, document.proteinsWith.size());
        assertTrue(document.proteinsWith.contains("mass_spectrometry"));

        assertTrue(document.commentMap.containsKey("cc_mass_spectrometry"));
        assertEquals(1, document.commentMap.get("cc_mass_spectrometry").size());
        assertTrue(
                document.commentMap
                        .get("cc_mass_spectrometry")
                        .contains(indexedMassSpectrometryComment));
        assertEquals(1, document.commentEvMap.size());
        assertTrue(document.commentEvMap.containsKey("ccev_mass_spectrometry"));
        assertEquals(3, document.commentEvMap.get("ccev_mass_spectrometry").size());
        assertTrue(document.commentEvMap.get("ccev_mass_spectrometry").contains("manual"));
        assertTrue(document.commentEvMap.get("ccev_mass_spectrometry").contains("ECO_0000269"));
        assertTrue(document.commentEvMap.get("ccev_mass_spectrometry").contains("experimental"));

        assertTrue(document.content.contains(indexedMassSpectrometryComment));
    }

    private UniProtEntry createUniProtEntryFromCommentLine(String commentLine) {
        List<Comment> comments = ccLineTransformer.transformNoHeader(commentLine);
        return new UniProtEntryBuilder("P12345", "P12345_ID", UniProtEntryType.TREMBL)
                .commentsSet(comments)
                .sequence(new SequenceBuilder("AAAA").build())
                .build();
    }
}
