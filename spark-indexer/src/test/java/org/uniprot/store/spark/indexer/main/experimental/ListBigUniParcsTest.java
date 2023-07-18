package org.uniprot.store.spark.indexer.main.experimental;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;
import static org.uniprot.store.spark.indexer.common.util.CommonVariables.SPARK_LOCAL_MASTER;

class ListBigUniParcsTest {

    @Test
    void main() {
        String[] args = new String[3];
        args[0] = "2020_02";
        args[1] = SPARK_LOCAL_MASTER;
        args[2] = "/Users/lgonzales/temp/uniparcResult";
        assertDoesNotThrow(() -> ListBigUniParcs.main(args));
    }
}