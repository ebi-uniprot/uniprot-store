package indexer.util;

import org.apache.spark.sql.Row;

/**
 * @author lgonzales
 * @since 2019-10-11
 */
public class RowUtils {

    public static boolean hasFieldName(String fieldName, Row row) {
        try {
            return row.get(row.fieldIndex(fieldName)) != null;
        } catch (IllegalArgumentException iae) {
            return false;
        }
    }
}
