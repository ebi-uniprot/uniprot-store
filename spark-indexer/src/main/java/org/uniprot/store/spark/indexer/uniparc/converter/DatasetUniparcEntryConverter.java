package org.uniprot.store.spark.indexer.uniparc.converter;

import static org.uniprot.store.spark.indexer.util.RowUtils.hasFieldName;

import java.io.Serializable;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.util.*;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.uniprot.core.Location;
import org.uniprot.core.Property;
import org.uniprot.core.uniparc.*;
import org.uniprot.core.uniparc.builder.InterProGroupBuilder;
import org.uniprot.core.uniparc.builder.SequenceFeatureBuilder;
import org.uniprot.core.uniparc.builder.UniParcDBCrossReferenceBuilder;
import org.uniprot.core.uniparc.builder.UniParcEntryBuilder;
import org.uniprot.core.uniprot.taxonomy.builder.TaxonomyBuilder;
import org.uniprot.store.spark.indexer.util.RowUtils;

/**
 * This class convert XML Row result to a UniParcEntry
 *
 * @author lgonzales
 * @since 2020-02-13
 */
public class DatasetUniparcEntryConverter implements MapFunction<Row, UniParcEntry>, Serializable {

    private static final long serialVersionUID = -510915540437314415L;

    @Override
    public UniParcEntry call(Row rowValue) throws Exception {
        UniParcEntryBuilder builder = new UniParcEntryBuilder();
        if (hasFieldName("_UniProtKB_exclusion", rowValue)) {
            builder.uniprotExclusionReason(
                    rowValue.getString(rowValue.fieldIndex("_UniProtKB_exclusion")));
        }
        if (hasFieldName("accession", rowValue)) {
            builder.uniParcId(rowValue.getString(rowValue.fieldIndex("accession")));
        }
        if (hasFieldName("dbReference", rowValue)) {
            List<Row> dbReferences = rowValue.getList(rowValue.fieldIndex("dbReference"));
            Set<Long> taxIds = new HashSet<>();
            for (Row dbReference : dbReferences) {
                UniParcDBCrossReference xref = convertDbReference(dbReference);
                builder.databaseCrossReferencesAdd(xref);
                xref.getProperties().stream()
                        .filter(property -> isTaxonomyProperty(property.getKey()))
                        .map(Property::getValue)
                        .map(Long::parseLong)
                        .forEach(taxIds::add);
            }
            taxIds.stream()
                    .map(
                            taxId -> {
                                return new TaxonomyBuilder().taxonId(taxId).build();
                            })
                    .forEach(builder::taxonomiesAdd);
        }
        if (hasFieldName("signatureSequenceMatch", rowValue)) {
            List<Row> sequenceFeatures =
                    rowValue.getList(rowValue.fieldIndex("signatureSequenceMatch"));
            sequenceFeatures.stream()
                    .map(this::convertSequenceFeatures)
                    .forEach(builder::sequenceFeaturesAdd);
        }
        if (hasFieldName("sequence", rowValue)) {
            Row sequence = (Row) rowValue.get(rowValue.fieldIndex("sequence"));
            builder.sequence(RowUtils.convertSequence(sequence));
        }
        return builder.build();
    }

    private boolean isTaxonomyProperty(String key) {
        return key.equalsIgnoreCase(UniParcDBCrossReference.PROPERTY_NCBI_TAXONOMY_ID);
    }

    public static StructType getUniParcXMLSchema() {
        StructType structType = new StructType();
        structType = structType.add("_dataset", DataTypes.StringType, true);
        structType = structType.add("_UniProtKB_exclusion", DataTypes.StringType, true);
        structType = structType.add("accession", DataTypes.StringType, true);
        ArrayType dbReferences = DataTypes.createArrayType(getDbReferenceSchema());
        structType = structType.add("dbReference", dbReferences, true);
        structType = structType.add("sequence", RowUtils.getSequenceSchema(), true);
        ArrayType signature = DataTypes.createArrayType(getSignatureSchema());
        structType = structType.add("signatureSequenceMatch", signature, true);

        return structType;
    }

    private SequenceFeature convertSequenceFeatures(Row rowValue) {
        SequenceFeatureBuilder builder = new SequenceFeatureBuilder();
        if (hasFieldName("_id", rowValue)) {
            builder.signatureDbId(rowValue.getString(rowValue.fieldIndex("_id")));
        }
        if (hasFieldName("_database", rowValue)) {
            String database = rowValue.getString(rowValue.fieldIndex("_database"));
            builder.signatureDbType(SignatureDbType.typeOf(database));
        }
        if (hasFieldName("ipr", rowValue)) {
            Row interproGroup = (Row) rowValue.get(rowValue.fieldIndex("ipr"));
            builder.interproGroup(convertInterproGroup(interproGroup));
        }
        if (hasFieldName("lcn", rowValue)) {
            List<Row> locations = rowValue.getList(rowValue.fieldIndex("lcn"));
            locations.stream().map(this::convertLocation).forEach(builder::locationsAdd);
        }
        return builder.build();
    }

    private InterProGroup convertInterproGroup(Row rowValue) {
        InterProGroupBuilder builder = new InterProGroupBuilder();
        if (hasFieldName("_id", rowValue)) {
            builder.id(rowValue.getString(rowValue.fieldIndex("_id")));
        }
        if (hasFieldName("_name", rowValue)) {
            builder.name(rowValue.getString(rowValue.fieldIndex("_name")));
        }
        return builder.build();
    }

    private Location convertLocation(Row rowValue) {
        Long start = 0L;
        if (hasFieldName("_start", rowValue)) {
            start = rowValue.getLong(rowValue.fieldIndex("_start"));
        }
        Long end = 0L;
        if (hasFieldName("_end", rowValue)) {
            end = rowValue.getLong(rowValue.fieldIndex("_end"));
        }
        return new Location(start.intValue(), end.intValue());
    }

    private UniParcDBCrossReference convertDbReference(Row rowValue) {
        UniParcDBCrossReferenceBuilder builder = new UniParcDBCrossReferenceBuilder();

        if (hasFieldName("_id", rowValue)) {
            builder.id(rowValue.getString(rowValue.fieldIndex("_id")));
        }
        if (hasFieldName("_type", rowValue)) {
            String databaseType = rowValue.getString(rowValue.fieldIndex("_type"));
            builder.databaseType(UniParcDatabaseType.typeOf(databaseType));
        }
        if (hasFieldName("_version_i", rowValue)) {
            builder.versionI((int) rowValue.getLong(rowValue.fieldIndex("_version_i")));
        }
        if (hasFieldName("_active", rowValue)) {
            String active = rowValue.getString(rowValue.fieldIndex("_active"));
            builder.active(active.equalsIgnoreCase("Y"));
        }
        if (hasFieldName("_version", rowValue)) {
            builder.version((int) rowValue.getLong(rowValue.fieldIndex("_version")));
        }
        DateTimeFormatter formatter =
                new DateTimeFormatterBuilder()
                        .parseCaseInsensitive()
                        .append(DateTimeFormatter.ofPattern("yyyy-MM-dd"))
                        .toFormatter();
        if (hasFieldName("_created", rowValue)) {
            String created = rowValue.getString(rowValue.fieldIndex("_created"));
            builder.created(LocalDate.parse(created, formatter));
        }
        if (hasFieldName("_last", rowValue)) {
            String last = rowValue.getString(rowValue.fieldIndex("_last"));
            builder.lastUpdated(LocalDate.parse(last, formatter));
        }
        if (hasFieldName("property", rowValue)) {
            RowUtils.convertProperties(rowValue)
                    .forEach(
                            (key, value) -> {
                                value.forEach(valueItem -> builder.propertiesAdd(key, valueItem));
                            });
        }

        return builder.build();
    }

    static StructType getSignatureSchema() {
        StructType structType = new StructType();

        structType = structType.add("_id", DataTypes.StringType, true);
        structType = structType.add("_database", DataTypes.StringType, true);

        structType = structType.add("ipr", getSeqFeatureGroupSchema(), true);
        ArrayType location = DataTypes.createArrayType(getLocationSchema());
        structType = structType.add("lcn", location, true);
        return structType;
    }

    static StructType getLocationSchema() {
        StructType structType = new StructType();
        structType = structType.add("_start", DataTypes.LongType, true);
        structType = structType.add("_end", DataTypes.LongType, true);
        return structType;
    }

    static StructType getSeqFeatureGroupSchema() {
        StructType structType = new StructType();
        structType = structType.add("_id", DataTypes.StringType, true);
        structType = structType.add("_name", DataTypes.StringType, true);
        return structType;
    }

    static StructType getDbReferenceSchema() {
        StructType structType = new StructType();
        structType = structType.add("_id", DataTypes.StringType, true);
        structType = structType.add("_type", DataTypes.StringType, true);
        structType = structType.add("_version_i", DataTypes.LongType, true);
        structType = structType.add("_active", DataTypes.StringType, true);
        structType = structType.add("_version", DataTypes.LongType, true);
        structType = structType.add("_created", DataTypes.StringType, true);
        structType = structType.add("_last", DataTypes.StringType, true);

        ArrayType property = DataTypes.createArrayType(RowUtils.getPropertySchema());
        structType = structType.add("property", property, true);
        return structType;
    }
}
