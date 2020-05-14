package org.uniprot.store.indexer.unirule;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.ThreadLocalRandom;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.uniprot.core.unirule.UniRuleEntry;
import org.uniprot.core.xml.jaxb.unirule.UniRuleType;
import org.uniprot.store.search.document.unirule.UniRuleDocument;

public class UniRuleDocumentConverterTest {
    private static UniRuleDocumentConverter docConverter;
    private static final String filePath = "src/test/resources/unirule/sample-unirule.xml";
    private static UniRuleXmlEntryReader reader;

    @BeforeAll
    static void setUp() {
        docConverter = new UniRuleDocumentConverter();
        reader = new UniRuleXmlEntryReader(filePath);
    }

    @Test
    void testConvert() throws Exception {
        UniRuleType xmlObj = reader.read();
        long proteinCount = ThreadLocalRandom.current().nextLong();
        docConverter.setProteinsAnnotatedCount(proteinCount);
        UniRuleDocument solrDoc = docConverter.convert(xmlObj);
        verifySolrDoc(solrDoc, proteinCount);
    }

    private void verifySolrDoc(UniRuleDocument solrDoc, long proteinCount) throws IOException {
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
        assertEquals(6, solrDoc.getCommentTypeValues().size());
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
                "[Mg(2+), Binds 1 Mg(2+) ion per subunit. Can also utilize other divalent metal cations, such as Ca(2+), Mn(2+) and Co(2+), CHEBI:29108, CHEBI:29035, CHEBI:18420, Co(2+), Ca(2+), CHEBI:48828, Mn(2+)]",
                solrDoc.getCommentTypeValues().get("cc_cofactor").toString());
        assertEquals(
                "[Belongs to the Dps family]",
                solrDoc.getCommentTypeValues().get("cc_similarity").toString());
        assertEquals(
                "[Protects DNA from oxidative damage by sequestering intracellular Fe2+ ion and\n"
                        + "                            storing it in the form of Fe3+ oxyhydroxide mineral. One hydrogen peroxide oxidizes two Fe2+\n"
                        + "                            ions, which prevents hydroxyl radical production by the Fenton reaction.\n"
                        + "                        ]",
                solrDoc.getCommentTypeValues().get("cc_function").toString());

        assertEquals(55, solrDoc.getContent().size());
        verifyUniRuleObject(solrDoc.getUniRuleObj(), proteinCount);
    }

    private void verifyUniRuleObject(ByteBuffer byteBuffer, long proteinCount) throws IOException {
        UniRuleEntry uniRuleObj =
                docConverter.getObjectMapper().readValue(byteBuffer.array(), UniRuleEntry.class);
        assertNotNull(uniRuleObj);
        assertEquals("UR001229753", uniRuleObj.getUniRuleId().getValue());
        assertEquals(proteinCount, uniRuleObj.getProteinsAnnotatedCount());
    }
}
