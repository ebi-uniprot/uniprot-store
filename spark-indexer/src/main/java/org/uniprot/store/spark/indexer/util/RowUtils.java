package org.uniprot.store.spark.indexer.util;

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
        dbReference = dbReference.add("_type", DataTypes.StringType, true);
        dbReference =
                dbReference.add("property", DataTypes.createArrayType(getPropertySchema()), true);
        return dbReference;
    }

    public static StructType getPropertySchema() {
        StructType structType = new StructType();
        structType = structType.add("_VALUE", DataTypes.StringType, true);
        structType = structType.add("_type", DataTypes.StringType, true);
        structType = structType.add("_value", DataTypes.StringType, true);
        return structType;
    }

    public static StructType getSequenceSchema() {
        StructType structType = new StructType();
        structType = structType.add("_VALUE", DataTypes.StringType, true);
        structType = structType.add("_checksum", DataTypes.StringType, true);
        structType = structType.add("_length", DataTypes.LongType, true);
        return structType;
    }

    public static Map<String, List<String>> convertProperties(Row rowValue) {
        List<Row> properties = rowValue.getList(rowValue.fieldIndex("property"));
        if (properties == null) {
            Row member = (Row) rowValue.get(rowValue.fieldIndex("property"));
            properties = Collections.singletonList(member);
        }
        Map<String, List<String>> propertyMap = new HashMap<>();
        properties.forEach(
                property -> {
                    if (hasFieldName("_type", property) && hasFieldName("_value", property)) {
                        String type = property.getString(property.fieldIndex("_type"));
                        String value = property.getString(property.fieldIndex("_value"));
                        propertyMap.putIfAbsent(type, new ArrayList<>());
                        propertyMap.get(type).add(value);
                    }
                });
        return propertyMap;
    }

    public static Sequence convertSequence(Row row) {
        if (hasFieldName("_VALUE", row)) {
            String sequenceValue = row.getString(row.fieldIndex("_VALUE"));
            SequenceBuilder builder = new SequenceBuilder(sequenceValue);
            return builder.build();
        }
        return null;
    }
}
