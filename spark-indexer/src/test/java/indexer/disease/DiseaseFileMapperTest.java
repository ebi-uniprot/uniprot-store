package indexer.disease;

import org.junit.jupiter.api.Test;
import org.uniprot.core.cv.disease.Disease;
import scala.Tuple2;

import static org.junit.jupiter.api.Assertions.*;

/**
 * @author lgonzales
 * @since 2019-11-14
 */
class DiseaseFileMapperTest {

    @Test
    void testValidDiseaseEntry() throws Exception{
        String diseaseLines =
                "ID   Jackson-Weiss syndrome.\n" +
                "AC   DI-00602\n" +
                "AR   JWS.\n" +
                "DE   An autosomal dominant craniosynostosis syndrome characterized by\n" +
                "DE   craniofacial abnormalities and abnormality of the feet: broad great\n" +
                "DE   toes with medial deviation and tarsal-metatarsal coalescence.\n" +
                "SY   Craniosynostosis-midfacial hypoplasia-foot abnormalities.\n" +
                "DR   MIM; 123150; phenotype.\n" +
                "DR   MedGen; C0795998.\n" +
                "DR   MeSH; D003398.\n" +
                "DR   MeSH; D005532.\n" +
                "KW   KW-0989:Craniosynostosis\n" +
                "//";

        DiseaseFileMapper mapper = new DiseaseFileMapper();

        Tuple2<String, Disease> tuple = mapper.call(diseaseLines);
        assertNotNull(tuple);

        assertEquals("Jackson-Weiss syndrome", tuple._1);
        Disease disease = tuple._2;
        assertNotNull(disease);
        assertEquals("DI-00602", disease.getAccession());
    }


    @Test
    void testInvalidDisease() throws Exception {
        DiseaseFileMapper mapper = new DiseaseFileMapper();
        assertThrows(RuntimeException.class, () -> {
            mapper.call("INVALID DATA");
        }, "ERROR PARSING DiseaseFileMapper");
    }
}