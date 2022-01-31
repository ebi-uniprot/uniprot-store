package org.uniprot.store.spark.indexer.chebi.mapper;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;
import org.uniprot.core.cv.chebi.ChebiEntry;

import scala.Tuple2;

/**
 * @author lgonzales
 * @since 2020-01-24
 */
class ChebiFileMapperTest {

    @Test
    void testChebiFileMapperWithoutInchKey() throws Exception {
        String input =
                "[Term]\n"
                        + "id: CHEBI:36347\n"
                        + "name: nuclear particle\n"
                        + "subset: 3_STAR\n"
                        + "def: \"A nucleus or any of its constituents in any of their energy states.\" []\n"
                        + "synonym: \"nuclear particle\" EXACT IUPAC_NAME [IUPAC]\n"
                        + "is_a: CHEBI:36342";

        ChebiFileMapper mapper = new ChebiFileMapper();
        Tuple2<Long, ChebiEntry> result = mapper.call(input);
        assertNotNull(result);
        assertEquals(36347L, result._1);
        ChebiEntry chebi = result._2;

        assertEquals("36347", chebi.getId());
        assertEquals("nuclear particle", chebi.getName());
        assertNull(chebi.getInchiKey());
    }

    @Test
    void testChebiFileMapperWithInchKey() throws Exception {
        String input =
                "[Term]\n"
                        + "id: CHEBI:30151\n"
                        + "name: aluminide(1-)\n"
                        + "subset: 3_STAR\n"
                        + "synonym: \"Aluminum anion\" RELATED [NIST_Chemistry_WebBook]\n"
                        + "synonym: \"Al(-)\" RELATED [IUPAC]\n"
                        + "synonym: \"aluminide(1-)\" EXACT IUPAC_NAME [IUPAC]\n"
                        + "synonym: \"aluminide(-I)\" EXACT IUPAC_NAME [IUPAC]\n"
                        + "property_value: http://purl.obolibrary.org/obo/chebi/formula \"Al\" xsd:string\n"
                        + "property_value: http://purl.obolibrary.org/obo/chebi/charge \"-1\" xsd:string\n"
                        + "property_value: http://purl.obolibrary.org/obo/chebi/monoisotopicmass \"26.98209\" xsd:string\n"
                        + "property_value: http://purl.obolibrary.org/obo/chebi/mass \"26.98154\" xsd:string\n"
                        + "property_value: http://purl.obolibrary.org/obo/chebi/inchi \"InChI=1S/Al/q-1\" xsd:string\n"
                        + "property_value: http://purl.obolibrary.org/obo/chebi/smiles \"[Al-]\" xsd:string\n"
                        + "property_value: http://purl.obolibrary.org/obo/chebi/inchikey \"SBLSYFIUPXRQRY-UHFFFAOYSA-N\" xsd:string\n"
                        + "xref: CAS:22325-47-9 \"NIST Chemistry WebBook\"\n"
                        + "is_a: CHEBI:33429\n"
                        + "is_a: CHEBI:33627";

        ChebiFileMapper mapper = new ChebiFileMapper();
        Tuple2<Long, ChebiEntry> result = mapper.call(input);
        assertNotNull(result);
        assertEquals(30151L, result._1);
        ChebiEntry chebi = result._2;

        assertEquals("30151", chebi.getId());
        assertEquals("aluminide(1-)", chebi.getName());
        assertEquals("SBLSYFIUPXRQRY-UHFFFAOYSA-N", chebi.getInchiKey());
    }
}
