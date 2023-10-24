package org.uniprot.store.indexer.search.uniparc;

import java.util.GregorianCalendar;

import javax.xml.datatype.DatatypeFactory;
import javax.xml.datatype.XMLGregorianCalendar;

import org.uniprot.core.xml.jaxb.uniparc.*;

/** Holds several utility methods that aid in the UniParc tests */
final class TestUtils {
    private static final ObjectFactory xmlFactory = new ObjectFactory();

    private TestUtils() {}

    static Entry createDefaultUniParcEntry() {

        Entry entry = xmlFactory.createEntry();
        entry.setAccession("UPI0000000001");
        entry.getDbReference().add(createDefaultXref());
        entry.setSequence(
                createSequence(
                        "MPLIYMNIMLAFTISLLGMLVYRSHLMSSLLCLEGMMLSLFIMATLMTLNTHSLLANIVP IAMLVFAACEAAVGLALLVSISNTYGLDYVHNLSLLQC",
                        "24B91F1DDC40BE22"));
        entry.setUniProtKBExclusion("P99999");

        return entry;
    }

    private static DbReferenceType createDefaultXref() {
        return createXref("UniProtKB/TrEMBL", "P000001", "Y");
    }

    static DbReferenceType createXref(String dbType, String id, String active) {
        return createXref(dbType, id, active, 0);
    }

    static DbReferenceType createXref(String dbType, String id, String active, Integer version) {
        DbReferenceType xref = xmlFactory.createDbReferenceType();
        xref.setActive(active);
        xref.setType(dbType);
        xref.setId(id);
        xref.setVersion(version);
        GregorianCalendar gcal = new GregorianCalendar();
        try {
            XMLGregorianCalendar xgcal =
                    DatatypeFactory.newInstance().newXMLGregorianCalendar(gcal);
            xref.setCreated(xgcal);
            xref.setLast(xgcal);
        } catch (Exception e) {

        }
        return xref;
    }

    static PropertyType createProperty(String type, String value) {
        PropertyType pr = xmlFactory.createPropertyType();
        pr.setType(type);
        pr.setValue(value);
        return pr;
    }

    static Sequence createSequence(String sequenceText, String checkSum) {

        Sequence sequence = xmlFactory.createSequence();
        sequence.setContent(sequenceText);
        sequence.setChecksum(checkSum);
        sequence.setLength(sequenceText.length());

        return sequence;
    }
}
