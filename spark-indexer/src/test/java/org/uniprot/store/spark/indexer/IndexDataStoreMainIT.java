package org.uniprot.store.spark.indexer;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;
import org.uniprot.store.spark.indexer.common.exception.IndexDataStoreException;
import org.uniprot.store.spark.indexer.main.IndexDataStoreMain;

/**
 * @author lgonzales
 * @since 24/04/2020
 */
class IndexDataStoreMainIT {

    @Test
    void testIndexDataStoreMainITInvalidArgumentIndex() {
        assertThrows(IllegalArgumentException.class, () -> IndexDataStoreMain.main(new String[0]));
    }

    @Test
    void testIndexDataStoreMainITInvalidCollection() {
        String[] args = {"invalid", "invalid"};
        assertThrows(IndexDataStoreException.class, () -> IndexDataStoreMain.main(args));
    }

    /*    @Test
    void testIndexDataStoreMainIT() {
        ///
        // Users/lgonzales/IdeaProjectsMaster/uniprot-store/spark-indexer/src/test/resources/uniref/UniRef50_A0A1I1BEK1.xml
        String[] args = {"uniref", "uniref"};
        assertThrows(IndexDataStoreException.class, () -> IndexDataStoreMain.main(args));
    }*/
}
