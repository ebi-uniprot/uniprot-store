package org.uniprot.store.spark.indexer.rhea;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;
import org.uniprot.store.spark.indexer.rhea.model.RheaComp;

import scala.Tuple2;

class RheaCompFileMapperTest {

    @Test
    void canMapRheaComp() throws Exception {
        String line = "RHEA-COMP:12851\tcytidine(32) in tRNA(Ser)";
        RheaCompFileMapper mapper = new RheaCompFileMapper();
        Tuple2<String, RheaComp> result = mapper.call(line);
        assertNotNull(result);
        assertEquals("RHEA-COMP:12851", result._1);
        assertNotNull(result._2);
        RheaComp rheaComp = result._2;
        assertEquals("RHEA-COMP:12851", rheaComp.getId());
        assertEquals("cytidine(32) in tRNA(Ser)", rheaComp.getName());
    }
}
