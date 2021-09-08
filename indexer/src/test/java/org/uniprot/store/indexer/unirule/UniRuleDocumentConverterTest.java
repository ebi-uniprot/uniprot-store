package org.uniprot.store.indexer.unirule;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.uniprot.core.unirule.UniRuleEntry;
import org.uniprot.core.xml.jaxb.unirule.UniRuleType;
import org.uniprot.store.search.document.unirule.UniRuleDocument;

class UniRuleDocumentConverterTest {
    private static UniRuleDocumentConverter docConverter;
    private static final String filePath = "src/test/resources/aa/sample-unirule.xml";
    private static final String filePathWithEC = "src/test/resources/aa/sample-unirule-ec.xml";
    private static UniRuleXmlEntryReader reader;

    @BeforeAll
    static void setUp() {
        docConverter = new UniRuleDocumentConverter();
    }

    @Test
    void testConvert() throws Exception {
        reader = new UniRuleXmlEntryReader(filePath);
        UniRuleType xmlObj = reader.read();
        long reviewedProteinCount = ThreadLocalRandom.current().nextLong();
        long unreviewedProteinCount = ThreadLocalRandom.current().nextLong();
        docConverter.setReviewedProteinCount(reviewedProteinCount);
        docConverter.setUnreviewedProteinCount(unreviewedProteinCount);
        UniRuleDocument solrDoc = docConverter.convert(xmlObj);
        verifySolrDoc(solrDoc, reviewedProteinCount, unreviewedProteinCount);
    }

    @Test
    void testConvertWithMoreThanOneECFamilyAndNote() throws Exception {
        reader = new UniRuleXmlEntryReader(filePathWithEC);
        UniRuleType xmlObj = reader.read();
        long reviewedProteinCount = ThreadLocalRandom.current().nextLong();
        long unreviewedProteinCount = ThreadLocalRandom.current().nextLong();
        docConverter.setReviewedProteinCount(reviewedProteinCount);
        docConverter.setUnreviewedProteinCount(unreviewedProteinCount);
        UniRuleDocument solrDoc = docConverter.convert(xmlObj);
        // verify ec
        Set<String> ecs = solrDoc.getEcNumbers();
        assertNotNull(ecs);
        assertEquals(3, ecs.size());
        assertEquals("[1.3.1.6, 1.3.5.4, 1.3.99.33]", ecs.toString());
        // verify note
        Set<String> notes = solrDoc.getCommentTypeValues().get("cc_cofactor_note");
        assertNotNull(notes);
        assertEquals(1, notes.size());
        assertEquals(
                "[Binds 1 Mg(2+) ion per subunit. Can also utilize other divalent metal cations, such as Ca(2+), Mn(2+) and Co(2+)]",
                notes.toString());
        // verify family
        Set<String> families = solrDoc.getFamilies();
        assertNotNull(families);
        assertEquals(1, families.size());
        assertEquals(
                "[FAD-dependent oxidoreductase 2 family, FRD/SDH subfamily]", families.toString());
    }

    private void verifySolrDoc(
            UniRuleDocument solrDoc, long reviewedProteinCount, long unreviewedProteinCount)
            throws IOException {
        assertNotNull(solrDoc);
        assertEquals("UR001229753", solrDoc.getUniRuleId());
        assertEquals(5, solrDoc.getConditionValues().size());
        assertEquals(
                "[Archaea, Human adenovirus, Bacteria, Human mastadenovirus, PIRSF018063]",
                solrDoc.getConditionValues().toString());
        assertEquals(3, solrDoc.getFeatureTypes().size());
        assertEquals("[site, transmembrane, signal]", solrDoc.getFeatureTypes().toString());
        assertEquals(12, solrDoc.getKeywords().size());
        assertEquals(
                "[Unknown, KW-0408, Oxidoreductase, Iron storage, KW-0409, KW-0000, KW-0560, Iron, Metal-binding, KW-0963, Cytoplasm, KW-0479]",
                solrDoc.getKeywords().toString());
        assertEquals(2, solrDoc.getGeneNames().size());
        assertEquals("[MP, atpA]", solrDoc.getGeneNames().toString());
        assertEquals(5, solrDoc.getGoTerms().size());
        assertEquals(
                "[GO:0006879, GO:0009295, GO:0008199, GO:0005737, GO:0016491]",
                solrDoc.getGoTerms().toString());
        assertEquals(4, solrDoc.getProteinNames().size());
        assertEquals(
                "[Sep-tRNA:Sec-tRNA synthase, Selenocysteine synthase, O-phosphoseryl-tRNA(Sec) selenium transferase, Selenocysteinyl-tRNA(Sec) synthase]",
                solrDoc.getProteinNames().toString());
        assertEquals(2, solrDoc.getOrganismNames().size());
        assertEquals(
                "[Human adenovirus, Human mastadenovirus]", solrDoc.getOrganismNames().toString());
        assertEquals(2, solrDoc.getTaxonomyNames().size());
        assertEquals("[Archaea, Bacteria]", solrDoc.getTaxonomyNames().toString());
        assertEquals(7, solrDoc.getCommentTypeValues().size());
        assertEquals(
                "[Golgi apparatus membrane, Cytoplasmic side, Cytoplasmic vesicle, COPI-coated vesicle membrane, Peripheral membrane protein, Cytoplasm]",
                solrDoc.getCommentTypeValues().get("cc_subcellular_location").toString());
        assertEquals(
                "[CHEBI:15378, CHEBI:15377, 2 Fe(2+) + 2 H(+) + H2O2 = 2 Fe(3+) + 2 H2O, RHEA:48712, CHEBI:29034, CHEBI:16240, CHEBI:29033]",
                solrDoc.getCommentTypeValues().get("cc_catalytic_activity").toString());
        assertEquals(
                "[Homododecamer. The 12 identical subunits form a hollow sphere into which the\n"
                        + "                            mineral iron core of up to 300 Fe(3+) can be deposited.\n"
                        + "                        ]",
                solrDoc.getCommentTypeValues().get("cc_subunit").toString());
        assertEquals(
                "[Mg(2+), CHEBI:29108, CHEBI:29035, CHEBI:18420, Co(2+), Ca(2+), CHEBI:48828, Mn(2+)]",
                solrDoc.getCommentTypeValues().get("cc_cofactor").toString());
        assertEquals(
                "[Binds 1 Mg(2+) ion per subunit. Can also utilize other divalent metal cations, such as Ca(2+), Mn(2+) and Co(2+)]",
                solrDoc.getCommentTypeValues().get("cc_cofactor_note").toString());
        assertEquals(
                "[Belongs to the Dps family]",
                solrDoc.getCommentTypeValues().get("cc_similarity").toString());
        assertEquals(
                "[Protects DNA from oxidative damage by sequestering intracellular Fe2+ ion and\n"
                        + "                            storing it in the form of Fe3+ oxyhydroxide mineral. One hydrogen peroxide oxidizes two Fe2+\n"
                        + "                            ions, which prevents hydroxyl radical production by the Fenton reaction.\n"
                        + "                        ]",
                solrDoc.getCommentTypeValues().get("cc_function").toString());
        assertEquals(1, solrDoc.getEcNumbers().size());
        assertEquals("[2.9.1.2]", solrDoc.getEcNumbers().toString());
        assertEquals(1, solrDoc.getFamilies().size());
        assertEquals("[Dps family]", solrDoc.getFamilies().toString());

        verifyUniRuleObject(solrDoc.getUniRuleObj(), reviewedProteinCount, unreviewedProteinCount);
    }

    private void verifyUniRuleObject(
            ByteBuffer byteBuffer, long reviewedProteinCount, long unreviewedProteinCount)
            throws IOException {
        UniRuleEntry uniRuleObj =
                docConverter.getObjectMapper().readValue(byteBuffer.array(), UniRuleEntry.class);
        assertNotNull(uniRuleObj);
        assertEquals("UR001229753", uniRuleObj.getUniRuleId().getValue());
        assertNotNull(uniRuleObj.getStatistics());
        assertEquals(reviewedProteinCount, uniRuleObj.getStatistics().getReviewedProteinCount());
        assertEquals(
                unreviewedProteinCount, uniRuleObj.getStatistics().getUnreviewedProteinCount());
    }
}
