package org.uniprot.store.spark.indexer.proteome.converter;

import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

/**
 * @author sahmad
 * @created 21/08/2020
 */
public class ProteomeXMLSchema {
    public static StructType geProteomeXMLSchema() {
        StructType structType = new StructType();
        structType = structType.add("_upid", DataTypes.StringType, true);
        structType = structType.add("_modified", DataTypes.StringType, true);
        structType = structType.add("_taxonomy", DataTypes.StringType, true);
        structType = structType.add("_source", DataTypes.StringType, true);
        structType = structType.add("_sourceTaxonomy", DataTypes.StringType, true);
        structType = structType.add("_superregnum", DataTypes.StringType, true);
        structType = structType.add("name", DataTypes.StringType, true);
        structType = structType.add("description", DataTypes.StringType, true);
        return structType;
    }
}
