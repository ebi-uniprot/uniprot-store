package org.uniprot.store.spark.indexer.util;

import org.apache.spark.sql.Row;
import org.uniprot.core.util.Utils;

/**
 * @author lgonzales
 * @since 2019-10-11
 */
public class RowUtils {

    public static boolean hasFieldName(String fieldName, Row row) {
        try {
            return Utils.notNull(row.get(row.fieldIndex(fieldName)));
        } catch (IllegalArgumentException iae) {
            return false;
        }
    }
}
