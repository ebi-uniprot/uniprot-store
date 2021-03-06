package org.uniprot.store.spark.indexer.uniref.converter;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.uniprot.core.cv.go.GeneOntologyEntry;
import org.uniprot.core.cv.go.GoAspect;
import org.uniprot.core.uniprotkb.taxonomy.Organism;
import org.uniprot.core.uniref.UniRefEntryLight;
import org.uniprot.core.uniref.UniRefMemberIdType;
import org.uniprot.core.uniref.UniRefType;
import org.uniprot.store.spark.indexer.common.util.RowUtils;
import org.uniprot.store.spark.indexer.uniref.UniRefXmlUtils;

import scala.collection.JavaConverters;
import scala.collection.Seq;

/**
 * Created 07/07/2020
 *
 * @author Edd
 */
class DatasetUniRefEntryLightConverterTest {

    private DatasetUniRefEntryLightConverter mapper;

    @BeforeEach
    void setUp() {
        mapper = new DatasetUniRefEntryLightConverter(UniRefType.UniRef100);
    }

    @Test
    void testFullUniRefEntry() throws Exception {
        Row row = getFullUnirefRow();

        UniRefEntryLight entry = mapper.call(row);

        assertNotNull(entry);

        assertEquals("UniRef100_P21802", entry.getId().getValue());
        assertEquals("Cluster: Fibroblast growth factor receptor 2", entry.getName());
        assertEquals(UniRefType.UniRef100, entry.getEntryType());
        assertEquals("2019-10-01", entry.getUpdated().toString());

        // properties
        assertEquals("common taxon Value", entry.getCommonTaxon().getScientificName());
        assertEquals(9606, entry.getCommonTaxon().getTaxonId());
        assertEquals(10, entry.getMemberCount());
        assertEquals("FGFR2_HUMAN,P12345", entry.getSeedId());

        assertEquals(3, entry.getGoTerms().size());
        GeneOntologyEntry goTerm = entry.getGoTerms().get(0);
        assertEquals(GoAspect.FUNCTION, goTerm.getAspect());
        assertEquals("Function", goTerm.getId());

        // members
        assertThat(entry.getMembers(), contains("R12345,0", "P12345,0", "UPI0003447082,3"));

        // representative
        assertNotNull(entry.getRepresentativeMember());

        // organism info
        assertNotNull(entry.getOrganisms());
        assertThat(entry.getOrganisms().size(), is(1));
        Organism result =
                entry.getOrganisms().stream().findFirst().orElseThrow(AssertionError::new);
        assertThat(result.getTaxonId(), is(9606L));
        assertThat(result.getScientificName(), is("Homo sapiens"));
        assertThat(result.getCommonName(), is("Human"));

        // uniparc presence
        assertThat(
                entry.getMemberIdTypes(),
                containsInAnyOrder(
                        UniRefMemberIdType.UNIPARC, UniRefMemberIdType.UNIPROTKB_SWISSPROT));
    }

    private Row getFullUnirefRow() {
        List<Object> entryValues = new ArrayList<>();
        entryValues.add("UniRef100_P21802"); // _id
        entryValues.add("2019-10-01"); // _updated
        entryValues.add(getMemberRowSeq()); // member
        entryValues.add("Cluster: Fibroblast growth factor receptor 2"); // name
        entryValues.add(getMainPropertiesSeq()); // property
        entryValues.add(getRepresentativeMemberRow()); // representativeMember

        return new GenericRowWithSchema(entryValues.toArray(), UniRefXmlUtils.getUniRefXMLSchema());
    }

    private Row getDBReferenceRow(String type, String id, Seq memberProperties) {
        List<Object> dbReferenceValue = new ArrayList<>();
        dbReferenceValue.add(id);
        dbReferenceValue.add(type);
        dbReferenceValue.add(memberProperties);
        return new GenericRowWithSchema(
                dbReferenceValue.toArray(), RowUtils.getDBReferenceSchema());
    }

    private Row getRepresentativeMemberRow() {
        List<Object> sequenceValues = new ArrayList<>();
        sequenceValues.add("MVSWGRFICLVVVTMATLSLAR");
        sequenceValues.add("6CD5001C960ED82F");
        sequenceValues.add("821");
        Row sequenceRow =
                new GenericRowWithSchema(sequenceValues.toArray(), RowUtils.getSequenceSchema());

        List<Object> representativeMembers = new ArrayList<>();
        representativeMembers.add(
                getDBReferenceRow("UniProtKB ID", "FGFR2_HUMAN", getMemberPropertiesSeq("R12345")));
        representativeMembers.add(sequenceRow);

        return new GenericRowWithSchema(
                representativeMembers.toArray(), UniRefXmlUtils.getRepresentativeMemberSchema());
    }

    private Row getPropertyRow(String name, Object value) {
        List<Object> propertyValues = new ArrayList<>();
        propertyValues.add("_VALUE");
        propertyValues.add(name);
        propertyValues.add(value);
        return new GenericRowWithSchema(propertyValues.toArray(), RowUtils.getPropertySchema());
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

    private Seq getMemberPropertiesSeq(String uniprotKBAcc) {
        List<Object> properties = new ArrayList<>();
        properties.add(getPropertyRow("UniProtKB accession", uniprotKBAcc));
        properties.add(getPropertyRow("UniProtKB accession", uniprotKBAcc + "-secondary"));
        properties.add(getPropertyRow("UniParc ID", "UPI000012A72A"));
        properties.add(getPropertyRow("UniRef50 ID", "UniRef50_" + uniprotKBAcc));
        properties.add(getPropertyRow("UniRef90 ID", "UniRef90_" + uniprotKBAcc));
        properties.add(getPropertyRow("UniRef100 ID", "UniRef100_" + uniprotKBAcc));
        properties.add(getPropertyRow("overlap region", "10-20"));
        properties.add(getPropertyRow("protein name", "Fibroblast growth factor receptor 2"));
        properties.add(getPropertyRow("source organism", "Homo sapiens (Human)"));
        properties.add(getPropertyRow("NCBI taxonomy", "9606"));
        properties.add(getPropertyRow("length", "80"));
        properties.add(getPropertyRow("isSeed", "true"));
        return (Seq)
                JavaConverters.asScalaIteratorConverter(properties.iterator()).asScala().toSeq();
    }

    private Seq getMemberRowSeq() {
        List<Object> membersValues = new ArrayList<>();
        membersValues.add(
                getMemberRowSeq("UniProtKB ID", "FGFR2_HUMAN", getMemberPropertiesSeq("P12345")));

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

    private Row getMemberRowSeq(String type, String id, Seq memberProperties) {
        Row dbReference = getDBReferenceRow(type, id, memberProperties);
        return new GenericRowWithSchema(
                Collections.singletonList(dbReference).toArray(), UniRefXmlUtils.getMemberSchema());
    }
}
