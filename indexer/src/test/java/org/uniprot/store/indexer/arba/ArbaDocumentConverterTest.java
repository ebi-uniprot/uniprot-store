package org.uniprot.store.indexer.arba;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.ThreadLocalRandom;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.uniprot.core.Range;
import org.uniprot.core.unirule.*;
import org.uniprot.core.unirule.impl.*;
import org.uniprot.core.xml.jaxb.unirule.UniRuleType;
import org.uniprot.store.indexer.unirule.UniRuleXmlEntryReader;
import org.uniprot.store.search.document.arba.ArbaDocument;

/**
 * @author lgonzales
 * @since 20/07/2021
 */
class ArbaDocumentConverterTest {
    private static ArbaDocumentConverter docConverter;
    private static final String filePath = "src/test/resources/aa/sample-arba.xml";
    private static UniRuleXmlEntryReader reader;

    @BeforeAll
    static void setUp() {
        docConverter = new ArbaDocumentConverter();
        reader = new UniRuleXmlEntryReader(filePath);
    }

    @Test
    void testConvert() throws Exception {
        UniRuleType xmlObj = reader.read();
        long proteinCount = ThreadLocalRandom.current().nextLong();
        docConverter.setProteinsAnnotatedCount(proteinCount);
        ArbaDocument solrDoc = docConverter.convert(xmlObj);
        verifySolrDoc(solrDoc, proteinCount);
    }

    @Test
    void testSamFeatureSetIllegalArgument() {
        UniRuleEntry entry =
                getBasicUniRuleBuilder()
                        .samFeatureSetsAdd(
                                new SamFeatureSetBuilder(new SamTriggerBuilder().build()).build())
                        .build();
        assertThrows(IllegalArgumentException.class, () -> docConverter.convertToDocument(entry));
    }

    @Test
    void testPositionFeatureSetIllegalArgument() {
        UniRuleEntry entry =
                getBasicUniRuleBuilder()
                        .positionFeatureSetsAdd(
                                new PositionFeatureSetBuilder(
                                                new PositionalFeatureBuilder(new Range(1, 2))
                                                        .build())
                                        .build())
                        .build();
        assertThrows(IllegalArgumentException.class, () -> docConverter.convertToDocument(entry));
    }

    private void verifySolrDoc(ArbaDocument solrDoc, long proteinCount) throws IOException {
        assertNotNull(solrDoc);
        assertEquals("ARBA00000001", solrDoc.getRuleId());
        assertEquals(3, solrDoc.getConditionValues().size());
        assertEquals("[organismName, IPR040234, Metazoa]", solrDoc.getConditionValues().toString());
        assertEquals(4, solrDoc.getKeywords().size());
        assertEquals("[Unknown, KW-0000, KW-0001, kName]", solrDoc.getKeywords().toString());
        assertEquals(1, solrDoc.getGeneNames().size());
        assertEquals("[gName]", solrDoc.getGeneNames().toString());
        assertEquals(1, solrDoc.getGoTerms().size());
        assertEquals("[GO:0000001]", solrDoc.getGoTerms().toString());
        assertEquals(1, solrDoc.getProteinNames().size());
        assertEquals("[recName]", solrDoc.getProteinNames().toString());
        assertEquals(1, solrDoc.getOrganismNames().size());
        assertEquals("[organismName]", solrDoc.getOrganismNames().toString());
        assertEquals(1, solrDoc.getTaxonomyNames().size());
        assertEquals("[Metazoa]", solrDoc.getTaxonomyNames().toString());
        assertEquals(2, solrDoc.getCommentTypeValues().size());
        assertEquals(
                "[RHEA-COMP:11736, RHEA-COMP:11846, CHEBI:87215, CHEBI:64722, RHEA:23652, CHEBI:28938, N-terminal L-glutaminyl-[peptide] = N-terminal 5-oxo-L-prolyl-[peptide] + NH4(+)]",
                solrDoc.getCommentTypeValues().get("cc_catalytic_activity").toString());
        assertEquals(1, solrDoc.getEcNumbers().size());
        assertEquals("[2.3.2.5]", solrDoc.getEcNumbers().toString());
        assertEquals(1, solrDoc.getFamilies().size());
        assertEquals("[nonaspanin (TM9SF) (TC 9.A.2) family]", solrDoc.getFamilies().toString());
        verifyUniRuleObject(solrDoc.getRuleObj(), proteinCount);
    }

    private void verifyUniRuleObject(ByteBuffer byteBuffer, long proteinCount) throws IOException {
        UniRuleEntry uniRuleObj =
                docConverter.getObjectMapper().readValue(byteBuffer.array(), UniRuleEntry.class);
        assertNotNull(uniRuleObj);
        assertEquals("ARBA00000001", uniRuleObj.getUniRuleId().getValue());
        assertNotNull(uniRuleObj.getStatistics());
        assertEquals(proteinCount, uniRuleObj.getStatistics().getUnreviewedProteinCount());
    }

    private UniRuleEntryBuilder getBasicUniRuleBuilder() {
        UniRuleId ruleId = new UniRuleIdBuilder("ARBA_ID").build();
        Information information = new InformationBuilder("10").build();
        Condition taxonCondition = new ConditionBuilder("taxon").build();

        Rule rule =
                new RuleBuilder(new ConditionSetBuilder(taxonCondition).build())
                        .annotationsAdd(new AnnotationBuilder().build())
                        .build();
        return new UniRuleEntryBuilder(ruleId, information, rule);
    }
}
