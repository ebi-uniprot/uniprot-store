package org.uniprot.store.spark.indexer.uniparc.converter;

import static org.uniprot.store.spark.indexer.common.util.RowUtils.hasFieldName;

import java.io.Serializable;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.util.List;
import java.util.Map;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.uniprot.core.uniparc.*;
import org.uniprot.core.uniparc.impl.InterProGroupBuilder;
import org.uniprot.core.uniparc.impl.SequenceFeatureBuilder;
import org.uniprot.core.uniparc.impl.SequenceFeatureLocationBuilder;
import org.uniprot.store.spark.indexer.common.util.RowUtils;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public abstract class BaseUniParcEntryConverter<T> implements MapFunction<Row, T>, Serializable {
    protected static final String UNIPROTKB_EXCLUSION = "_UniProtKB_exclusion";
    protected static final String ACCESSION = "accession";
    protected static final String DB_REFERENCE = "dbReference";
    protected static final String SIGNATURE_SEQUENCE_MATCH = "signatureSequenceMatch";
    protected static final String SEQUENCE = "sequence";
    protected static final String ID = "_id";
    protected static final String DATABASE = "_database";
    protected static final String IPR = "ipr";
    protected static final String LCN = "lcn";
    protected static final String NAME = "_name";
    protected static final String START = "_start";
    protected static final String END = "_end";
    protected static final String ALIGNMENT = "_alignment";
    protected static final String TYPE = "_type";
    protected static final String VERSION_I = "_version_i";
    protected static final String ACTIVE = "_active";
    protected static final String VERSION = "_version";
    protected static final String CREATED = "_created";
    protected static final String LAST = "_last";
    protected static final String PROPERTY = "property";
    protected static final String PROPERTY_GENE_NAME = "gene_name";
    protected static final String PROPERTY_PROTEIN_NAME = "protein_name";
    protected static final String PROPERTY_CHAIN = "chain";
    protected static final String PROPERTY_NCBI_GI = "NCBI_GI";
    protected static final String PROPERTY_PROTEOMEID_COMPONENT = "proteomeid_component";
    protected static final String PROPERTY_NCBI_TAXONOMY_ID = "NCBI_taxonomy_id";
    protected static final String PROPERTY_UNIPROTKB_ACCESSION = "UniProtKB_accession";

    protected SequenceFeature convertSequenceFeatures(Row rowValue) {
        SequenceFeatureBuilder builder = new SequenceFeatureBuilder();
        if (hasFieldName(ID, rowValue)) {
            builder.signatureDbId(rowValue.getString(rowValue.fieldIndex(ID)));
        }
        if (hasFieldName(DATABASE, rowValue)) {
            String database = rowValue.getString(rowValue.fieldIndex(DATABASE));
            builder.signatureDbType(SignatureDbType.typeOf(database));
        }
        if (hasFieldName(IPR, rowValue)) {
            Row interproGroup = (Row) rowValue.get(rowValue.fieldIndex(IPR));
            builder.interproGroup(convertInterproGroup(interproGroup));
        }
        if (hasFieldName(LCN, rowValue)) {
            List<Row> locations = rowValue.getList(rowValue.fieldIndex(LCN));
            locations.stream().map(this::convertLocation).forEach(builder::locationsAdd);
        }
        return builder.build();
    }

    protected InterProGroup convertInterproGroup(Row rowValue) {
        InterProGroupBuilder builder = new InterProGroupBuilder();
        if (hasFieldName(ID, rowValue)) {
            builder.id(rowValue.getString(rowValue.fieldIndex(ID)));
        }
        if (hasFieldName(NAME, rowValue)) {
            builder.name(rowValue.getString(rowValue.fieldIndex(NAME)));
        }
        return builder.build();
    }

    protected SequenceFeatureLocation convertLocation(Row rowValue) {
        SequenceFeatureLocationBuilder builder = new SequenceFeatureLocationBuilder();
        if (hasFieldName(START, rowValue)) {
            builder.start((int) rowValue.getLong(rowValue.fieldIndex(START)));
        }
        if (hasFieldName(END, rowValue)) {
            builder.end((int) rowValue.getLong(rowValue.fieldIndex(END)));
        }
        if (hasFieldName(ALIGNMENT, rowValue)) {
            builder.alignment(rowValue.getString(rowValue.fieldIndex(ALIGNMENT)));
        }
        return builder.build();
    }

    protected DateTimeFormatter getDateTimeFormatter() {
        return new DateTimeFormatterBuilder()
                .parseCaseInsensitive()
                .append(DateTimeFormatter.ofPattern("yyyy-MM-dd"))
                .toFormatter();
    }

    protected LocalDate parseDate(Row rowValue, String fieldName) {
        if (hasFieldName(fieldName, rowValue)) {
            String dateStr = rowValue.getString(rowValue.fieldIndex(fieldName));
            return LocalDate.parse(dateStr, getDateTimeFormatter());
        }
        return null;
    }

    protected String getTaxonomyId(Row rowValue) {
        String taxonId = null;
        if (hasFieldName(PROPERTY, rowValue)) {
            Map<String, List<String>> propertyMap = RowUtils.convertProperties(rowValue);
            if (propertyMap.containsKey(PROPERTY_NCBI_TAXONOMY_ID)) {
                taxonId = propertyMap.get(PROPERTY_NCBI_TAXONOMY_ID).get(0);
                propertyMap.remove(PROPERTY_NCBI_TAXONOMY_ID);
            }
        }
        return taxonId;
    }

    public static StructType getUniParcXMLSchema() {
        StructType structType = new StructType();
        structType = structType.add("_dataset", DataTypes.StringType, true);
        structType = structType.add(UNIPROTKB_EXCLUSION, DataTypes.StringType, true);
        structType = structType.add(ACCESSION, DataTypes.StringType, true);
        ArrayType dbReferences = DataTypes.createArrayType(getDbReferenceSchema());
        structType = structType.add(DB_REFERENCE, dbReferences, true);
        structType = structType.add(SEQUENCE, RowUtils.getSequenceSchema(), true);
        ArrayType signature = DataTypes.createArrayType(getSignatureSchema());
        structType = structType.add(SIGNATURE_SEQUENCE_MATCH, signature, true);

        return structType;
    }

    static StructType getSignatureSchema() {
        StructType structType = new StructType();

        structType = structType.add(ID, DataTypes.StringType, true);
        structType = structType.add(DATABASE, DataTypes.StringType, true);

        structType = structType.add(IPR, getSeqFeatureGroupSchema(), true);
        ArrayType location = DataTypes.createArrayType(getLocationSchema());
        structType = structType.add(LCN, location, true);
        return structType;
    }

    static StructType getLocationSchema() {
        StructType structType = new StructType();
        structType = structType.add(START, DataTypes.LongType, true);
        structType = structType.add(END, DataTypes.LongType, true);
        structType = structType.add(ALIGNMENT, DataTypes.StringType, true);
        return structType;
    }

    static StructType getSeqFeatureGroupSchema() {
        StructType structType = new StructType();
        structType = structType.add(ID, DataTypes.StringType, true);
        structType = structType.add(NAME, DataTypes.StringType, true);
        return structType;
    }

    static StructType getDbReferenceSchema() {
        StructType structType = new StructType();
        structType = structType.add(ID, DataTypes.StringType, true);
        structType = structType.add(TYPE, DataTypes.StringType, true);
        structType = structType.add(VERSION_I, DataTypes.LongType, true);
        structType = structType.add(ACTIVE, DataTypes.StringType, true);
        structType = structType.add(VERSION, DataTypes.LongType, true);
        structType = structType.add(CREATED, DataTypes.StringType, true);
        structType = structType.add(LAST, DataTypes.StringType, true);

        ArrayType property = DataTypes.createArrayType(RowUtils.getPropertySchema());
        structType = structType.add(PROPERTY, property, true);
        return structType;
    }
}
