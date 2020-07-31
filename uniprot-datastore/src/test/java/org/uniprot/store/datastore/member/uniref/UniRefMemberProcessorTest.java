package org.uniprot.store.datastore.member.uniref;

import java.io.IOException;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.uniprot.core.uniref.RepresentativeMember;
import org.uniprot.core.xml.jaxb.uniref.MemberType;

import static org.junit.jupiter.api.Assertions.*;

class UniRefMemberProcessorTest {
    private static UniRef100MemberProcessor memberProcessor;
    private static final String filePath = "src/test/resources/uniref/50_Q9EPS7_Q95604.xml";
    private static UniRefMemberXmlEntryReader reader;

    @BeforeAll
    static void setUp() {
        memberProcessor = new UniRef100MemberProcessor();
        reader = new UniRefMemberXmlEntryReader(filePath);
    }

    @Test
    void testProcess() throws Exception {
        MemberType xmlObj = reader.read();
        assertNotNull(xmlObj, "memberType is null");
        RepresentativeMember uniObj = memberProcessor.process(xmlObj);
        verifyUniObj(uniObj, xmlObj);
    }

    private void verifyUniObj(RepresentativeMember uniObj, MemberType memberType)
            throws IOException {
        assertNotNull(uniObj);
        assertNull(uniObj.isSeed(), "seed is not null");
        assertNotNull(uniObj.getMemberIdType());
        assertNotNull(uniObj.getMemberId());
        assertNotNull(uniObj.getOrganismName());
        assertNotEquals(0L, uniObj.getOrganismTaxId());
    }
}
