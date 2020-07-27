package org.uniprot.store.datastore.member.uniref;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import org.junit.jupiter.api.Test;
import org.uniprot.core.xml.jaxb.uniref.MemberType;

public class UniRefMemberXmlEntryReaderTest {

    @Test
    void testReadXmlFile() throws Exception {
        String filePath = "src/test/resources/uniref/50_Q9EPS7_Q95604.xml";
        UniRefMemberXmlEntryReader reader = new UniRefMemberXmlEntryReader(filePath);
        assertNotNull(reader);
        int count = 0;
        MemberType memberType;
        while ((memberType = reader.read()) != null) {
            count++;
            verifyUniRuleEntry(memberType);
        }

        assertEquals(5487, count);
    }

    private void verifyUniRuleEntry(MemberType memberType) {
        assertNotNull(memberType, "UniRule entry is null");
        assertNotNull(memberType.getDbReference(), "DbReference is null");
    }
}
