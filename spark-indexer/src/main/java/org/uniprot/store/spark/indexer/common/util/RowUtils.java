package org.uniprot.store.spark.indexer.common.util;

import java.util.*;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.uniprot.core.Sequence;
import org.uniprot.core.impl.SequenceBuilder;
import org.uniprot.core.util.Utils;

/**
 * @author lgonzales
 * @since 2019-10-11
 */
public class RowUtils {

    private static final String TYPE = "_type";
    private static final String PROPERTY = "property";
    private static final String VALUE = "_VALUE";
    private static final String VALUE_ATTRIBUTE = "_value";

    private RowUtils() {}

    public static boolean hasFieldName(String fieldName, Row row) {
        try {
            return Utils.notNull(row.get(row.fieldIndex(fieldName)));
        } catch (IllegalArgumentException iae) {
            return false;
        }
    }

    public static StructType getDBReferenceSchema() {
        StructType dbReference = new StructType();
        dbReference = dbReference.add("_id", DataTypes.StringType, true);
        dbReference = dbReference.add(TYPE, DataTypes.StringType, true);
        dbReference =
                dbReference.add(PROPERTY, DataTypes.createArrayType(getPropertySchema()), true);
        return dbReference;
    }

    public static StructType getPropertySchema() {
        StructType structType = new StructType();
        structType = structType.add(VALUE, DataTypes.StringType, true);
        structType = structType.add(TYPE, DataTypes.StringType, true);
        structType = structType.add(VALUE_ATTRIBUTE, DataTypes.StringType, true);
        return structType;
    }

    public static StructType getSequenceSchema() {
        StructType structType = new StructType();
        structType = structType.add(VALUE, DataTypes.StringType, true);
        structType = structType.add("_checksum", DataTypes.StringType, true);
        structType = structType.add("_length", DataTypes.LongType, true);
        return structType;
    }

    public static Map<String, List<String>> convertProperties(Row rowValue) {
        List<Row> properties = rowValue.getList(rowValue.fieldIndex(PROPERTY));
        if (properties == null) {
            Row member = (Row) rowValue.get(rowValue.fieldIndex(PROPERTY));
            properties = Collections.singletonList(member);
        }
        Map<String, List<String>> propertyMap = new HashMap<>();
        properties.forEach(
                property -> {
                    if (hasFieldName(TYPE, property) && hasFieldName(VALUE_ATTRIBUTE, property)) {
                        String type = property.getString(property.fieldIndex(TYPE));
                        String value = property.getString(property.fieldIndex(VALUE_ATTRIBUTE));
                        propertyMap.putIfAbsent(type, new ArrayList<>());
                        propertyMap.get(type).add(value);
                    }
                });
        return propertyMap;
    }

    public static Sequence convertSequence(Row row) {
        if (hasFieldName(VALUE, row)) {
            String sequenceValue = row.getString(row.fieldIndex(VALUE));
            SequenceBuilder builder = new SequenceBuilder(sequenceValue);
            return builder.build();
        }
        return null;
    }
}
