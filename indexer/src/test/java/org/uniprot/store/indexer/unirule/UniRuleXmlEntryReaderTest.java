package org.uniprot.store.indexer.unirule;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import org.junit.jupiter.api.Test;
import org.uniprot.core.xml.jaxb.unirule.UniRuleType;

public class UniRuleXmlEntryReaderTest {

    @Test
    void testReadXmlFile() throws Exception {
        String filePath = "src/test/resources/aa/sample-unirule.xml";
        UniRuleXmlEntryReader reader = new UniRuleXmlEntryReader(filePath);
        assertNotNull(reader);
        int count = 0;
        UniRuleType uniRuleType;
        while ((uniRuleType = reader.read()) != null) {
            count++;
            verifyUniRuleEntry(uniRuleType);
        }

        assertEquals(2, count);
    }

    private void verifyUniRuleEntry(UniRuleType uniRuleType) {
        assertNotNull(uniRuleType, "UniRule entry is null");
        assertNotNull(uniRuleType.getId(), "id is null");
        assertNotNull(uniRuleType.getStatus(), "status is null");
        assertNotNull(uniRuleType.getInformation(), "information is null");
        assertNotNull(uniRuleType.getMain(), "main rule is null");
    }
}
