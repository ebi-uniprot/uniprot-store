package org.uniprot.store.spark.indexer.uniref.converter;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.*;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.junit.jupiter.api.Test;
import org.uniprot.core.uniref.*;

import scala.collection.JavaConverters;
import scala.collection.Seq;

/**
 * @author lgonzales
 * @since 2019-11-22
 */
class DatasetUnirefEntryConverterTest {

    @Test
    void testFullUniRefEntry() throws Exception {
        Row row = getFullUnirefRow();

        DatasetUnirefEntryConverter mapper = new DatasetUnirefEntryConverter(UniRefType.UniRef100);

        UniRefEntry entry = mapper.call(row);

        assertNotNull(entry);

        assertEquals("UniRef100_P21802", entry.getId().getValue());
        assertEquals("Cluster Name", entry.getName());
        assertEquals(UniRefType.UniRef100, entry.getEntryType());
        assertEquals("2019-10-01", entry.getUpdated().toString());

        // properties
        assertEquals("common taxon Value", entry.getCommonTaxon());
        assertEquals(9606, entry.getCommonTaxonId());
        assertEquals(10, entry.getMemberCount());
        assertEquals(3, entry.getGoTerms().size());
        GoTerm goTerm = entry.getGoTerms().get(0);
        assertEquals(GoTermType.FUNCTION, goTerm.getType());
        assertEquals("Function", goTerm.getId());

        // representative member
        assertNotNull(entry.getRepresentativeMember());
        RepresentativeMember representativeMember = entry.getRepresentativeMember();
        assertEquals("MVSWGRFICLVVVTMATLSLAR", representativeMember.getSequence().getValue());
        assertEquals("62C549AB5E41E99D", representativeMember.getSequence().getCrc64());
        assertNotNull(representativeMember.getSequence().getMd5());
        assertEquals(22, representativeMember.getSequence().getLength());
        assertEquals(2454, representativeMember.getSequence().getMolWeight());
        validateUnirefMember(representativeMember);

        // members
        assertEquals(2, entry.getMembers().size());

        Optional<UniRefMember> uniparcMember =
                entry.getMembers().stream()
                        .filter(
                                uniRefMember ->
                                        uniRefMember
                                                .getMemberIdType()
                                                .equals(UniRefMemberIdType.UNIPARC))
                        .findFirst();
        assertTrue(uniparcMember.isPresent());
        UniRefMember member = uniparcMember.get();
        assertEquals("UPI0003447082", member.getMemberId());
        assertEquals("UniRef50_P21802", member.getUniRef50Id().getValue());
        assertEquals("UniRef90_P21802", member.getUniRef90Id().getValue());
        assertEquals("UniRef100_P21802", member.getUniRef100Id().getValue());

        Optional<UniRefMember> uniprotMember =
                entry.getMembers().stream()
                        .filter(
                                uniRefMember ->
                                        uniRefMember
                                                .getMemberIdType()
                                                .equals(UniRefMemberIdType.UNIPROTKB))
                        .findFirst();

        assertTrue(uniprotMember.isPresent());
        validateUnirefMember(uniprotMember.get());
    }

    @Test
    void testRequiredOnlyUniRef() throws Exception {
        List<Object> entryValues = new ArrayList<>();
        entryValues.add("UniRef100_P21802"); // _id
        entryValues.add("2019-10-01"); // _updated
        entryValues.add(null); // member
        entryValues.add("Cluster Name"); // name
        entryValues.add(null); // property
        entryValues.add(getRepresentativeMemberRow()); // representativeMember

        Row row =
                new GenericRowWithSchema(
                        entryValues.toArray(), DatasetUnirefEntryConverter.getUniRefXMLSchema());

        DatasetUnirefEntryConverter mapper = new DatasetUnirefEntryConverter(UniRefType.UniRef90);
        UniRefEntry entry = mapper.call(row);

        assertNotNull(entry);
    }

    private void validateUnirefMember(UniRefMember uniRefMember) {
        assertEquals(UniRefMemberIdType.UNIPROTKB, uniRefMember.getMemberIdType());
        assertEquals("FGFR2_HUMAN", uniRefMember.getMemberId());
        assertEquals("Homo sapiens (Human)", uniRefMember.getOrganismName());
        assertEquals(9606, uniRefMember.getOrganismTaxId());
        assertEquals(80, uniRefMember.getSequenceLength());
        assertEquals("Fibroblast growth factor receptor 2", uniRefMember.getProteinName());
        assertEquals(1, uniRefMember.getUniProtAccessions().size());
        assertEquals("P12345", uniRefMember.getUniProtAccessions().get(0).getValue());
        assertEquals("UniRef50_P12345", uniRefMember.getUniRef50Id().getValue());
        assertEquals("UniRef90_P12345", uniRefMember.getUniRef90Id().getValue());
        assertEquals("UniRef100_P12345", uniRefMember.getUniRef100Id().getValue());
        assertEquals("UPI000012A72A", uniRefMember.getUniParcId().getValue());
        assertTrue(uniRefMember.isSeed());

        assertNotNull(uniRefMember.getOverlapRegion());
        OverlapRegion overlapRegion = uniRefMember.getOverlapRegion();
        assertEquals(10, overlapRegion.getStart());
        assertEquals(20, overlapRegion.getEnd());
    }

    private Row getFullUnirefRow() {
        List<Object> entryValues = new ArrayList<>();
        entryValues.add("UniRef100_P21802"); // _id
        entryValues.add("2019-10-01"); // _updated
        entryValues.add(getMemberRowSeq()); // member
        entryValues.add("Cluster Name"); // name
        entryValues.add(getMainPropertiesSeq()); // property
        entryValues.add(getRepresentativeMemberRow()); // representativeMember

        return new GenericRowWithSchema(
                entryValues.toArray(), DatasetUnirefEntryConverter.getUniRefXMLSchema());
    }

    private Seq getMainPropertiesSeq() {
        List<Object> properties = new ArrayList<>();
        properties.add(getPropertyRow("member count", "10"));
        properties.add(getPropertyRow("common taxon", "common taxon Value"));
        properties.add(getPropertyRow("common taxon ID", "9606"));
        properties.add(getPropertyRow("GO Molecular Function", "Function"));
        properties.add(getPropertyRow("GO Cellular Component", "Component"));
        properties.add(getPropertyRow("GO Biological Process", "Process"));
        return (Seq)
                JavaConverters.asScalaIteratorConverter(properties.iterator()).asScala().toSeq();
    }

    private Seq getMemberPropertiesSeq() {
        List<Object> properties = new ArrayList<>();
        properties.add(getPropertyRow("UniProtKB accession", "P12345"));
        properties.add(getPropertyRow("UniParc ID", "UPI000012A72A"));
        properties.add(getPropertyRow("UniRef50 ID", "UniRef50_P12345"));
        properties.add(getPropertyRow("UniRef90 ID", "UniRef90_P12345"));
        properties.add(getPropertyRow("UniRef100 ID", "UniRef100_P12345"));
        properties.add(getPropertyRow("overlap region", "10-20"));
        properties.add(getPropertyRow("protein name", "Fibroblast growth factor receptor 2"));
        properties.add(getPropertyRow("source organism", "Homo sapiens (Human)"));
        properties.add(getPropertyRow("NCBI taxonomy", "9606"));
        properties.add(getPropertyRow("length", "80"));
        properties.add(getPropertyRow("isSeed", "true"));
        return (Seq)
                JavaConverters.asScalaIteratorConverter(properties.iterator()).asScala().toSeq();
    }

    private Row getRepresentativeMemberRow() {
        List<Object> sequenceValues = new ArrayList<>();
        sequenceValues.add("MVSWGRFICLVVVTMATLSLAR");
        sequenceValues.add("6CD5001C960ED82F");
        sequenceValues.add("821");
        Row sequenceRow =
                new GenericRowWithSchema(
                        sequenceValues.toArray(), DatasetUnirefEntryConverter.getSequenceSchema());

        List<Object> representativeMembers = new ArrayList<>();
        representativeMembers.add(
                getDBReferenceRow("UniProtKB ID", "FGFR2_HUMAN", getMemberPropertiesSeq()));
        representativeMembers.add(sequenceRow);

        return new GenericRowWithSchema(
                representativeMembers.toArray(),
                DatasetUnirefEntryConverter.getRepresentativeMemberSchema());
    }

    private Row getPropertyRow(String name, Object value) {
        List<Object> propertyValues = new ArrayList<>();
        propertyValues.add("_VALUE");
        propertyValues.add(name);
        propertyValues.add(value);
        return new GenericRowWithSchema(
                propertyValues.toArray(), DatasetUnirefEntryConverter.getPropertySchema());
    }

    private Row getMemberRowSeq(String type, String id, Seq memberProperties) {
        Row dbReference = getDBReferenceRow(type, id, memberProperties);
        return new GenericRowWithSchema(
                Collections.singletonList(dbReference).toArray(),
                DatasetUnirefEntryConverter.getMemberSchema());
    }

    private Row getDBReferenceRow(String type, String id, Seq memberProperties) {
        List<Object> dbReferenceValue = new ArrayList<>();
        dbReferenceValue.add(id);
        dbReferenceValue.add(type);
        dbReferenceValue.add(memberProperties);
        return new GenericRowWithSchema(
                dbReferenceValue.toArray(), DatasetUnirefEntryConverter.getDBReferenceSchema());
    }

    private Seq getMemberRowSeq() {
        List<Object> membersValues = new ArrayList<>();
        membersValues.add(getMemberRowSeq("UniProtKB ID", "FGFR2_HUMAN", getMemberPropertiesSeq()));

        List<Object> uniparcProperties = new ArrayList<>();
        uniparcProperties.add(getPropertyRow("UniRef50 ID", "UniRef50_P21802"));
        uniparcProperties.add(getPropertyRow("UniRef90 ID", "UniRef90_P21802"));
        uniparcProperties.add(getPropertyRow("UniRef100 ID", "UniRef100_P21802"));
        Seq uniparcProps =
                (Seq)
                        JavaConverters.asScalaIteratorConverter(uniparcProperties.iterator())
                                .asScala()
                                .toSeq();

        membersValues.add(getMemberRowSeq("UniParc ID", "UPI0003447082", uniparcProps));
        return (Seq)
                JavaConverters.asScalaIteratorConverter(membersValues.iterator()).asScala().toSeq();
    }
}
