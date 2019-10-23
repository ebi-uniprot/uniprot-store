package indexer.uniref;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.uniprot.core.impl.SequenceImpl;
import org.uniprot.core.uniparc.impl.UniParcIdImpl;
import org.uniprot.core.uniprot.impl.UniProtAccessionImpl;
import org.uniprot.core.uniref.*;
import org.uniprot.core.uniref.builder.RepresentativeMemberBuilder;
import org.uniprot.core.uniref.builder.UniRefEntryBuilder;
import org.uniprot.core.uniref.builder.UniRefMemberBuilder;
import org.uniprot.core.uniref.impl.OverlapRegionImpl;
import org.uniprot.core.uniref.impl.UniRefEntryIdImpl;

import java.io.Serializable;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static indexer.util.RowUtils.hasFieldName;

/**
 * @author lgonzales
 * @since 2019-10-01
 */
public class DatasetUnirefEntryConverter implements MapFunction<Row, UniRefEntry>, Serializable {

    private static final long serialVersionUID = -526130623950089875L;
    private final UniRefType uniRefType;

    public DatasetUnirefEntryConverter(UniRefType uniRefType) {
        this.uniRefType = uniRefType;
    }

    @Override
    public UniRefEntry call(Row rowValue) throws Exception {
        UniRefEntryBuilder builder = new UniRefEntryBuilder();
        builder.entryType(uniRefType);
        builder.id(rowValue.getString(rowValue.fieldIndex("_id")));
        DateTimeFormatter formatter = new DateTimeFormatterBuilder().parseCaseInsensitive()
                .append(DateTimeFormatter.ofPattern("yyyy-MM-dd")).toFormatter();
        String xmlUpdatedDate = rowValue.getString(rowValue.fieldIndex("_updated"));
        builder.updated(LocalDate.parse(xmlUpdatedDate, formatter));

        if (hasFieldName("name", rowValue)) {
            builder.name(rowValue.getString(rowValue.fieldIndex("name")));
        }
        if (hasFieldName("property", rowValue)) {
            Map<String, String> propertyMap = convertProperties(rowValue);
            builder.memberCount(Integer.valueOf(propertyMap.getOrDefault("member count", "0")));
            builder.commonTaxon(propertyMap.get("common taxon"));
            builder.commonTaxonId(Integer.valueOf(propertyMap.getOrDefault("common taxon ID", "0")));
        }

        if (hasFieldName("member", rowValue)) {
            List<Row> members = rowValue.getList(rowValue.fieldIndex("member"));
            if (members == null) {
                Row member = (Row) rowValue.get(rowValue.fieldIndex("member"));
                members = Collections.singletonList(member);
            }
            members.stream()
                    .map(this::convertMember)
                    .forEach(builder::addMember);

        }

        if (hasFieldName("representativeMember", rowValue)) {
            Row representativeMemberRow = (Row) rowValue.get(rowValue.fieldIndex("representativeMember"));
            builder.representativeMember(convertRepresentativeMember(representativeMemberRow));
        }

        return builder.build();
    }

    private RepresentativeMember convertRepresentativeMember(Row representativeMemberRow) {
        RepresentativeMemberBuilder builder = new RepresentativeMemberBuilder()
                .from(convertMember(representativeMemberRow));
        if (hasFieldName("sequence", representativeMemberRow)) {
            Row sequence = (Row) representativeMemberRow.get(representativeMemberRow.fieldIndex("sequence"));
            if (hasFieldName("_VALUE", sequence)) {
                String sequenceValue = sequence.getString(sequence.fieldIndex("_VALUE"));
                builder.sequence(new SequenceImpl(sequenceValue));
            }
        }
        return builder.build();
    }

    private UniRefMember convertMember(Row member) {
        UniRefMemberBuilder builder = new UniRefMemberBuilder();
        if (hasFieldName("dbReference", member)) {
            Row dbReference = (Row) member.get(member.fieldIndex("dbReference"));
            builder.memberId(dbReference.getString(dbReference.fieldIndex("_id")));
            String memberType = dbReference.getString(dbReference.fieldIndex("_type"));
            builder.memberIdType(UniRefMemberIdType.typeOf(memberType));

            if (hasFieldName("property", dbReference)) {
                Map<String, String> propertyMap = convertProperties(dbReference);
                if (propertyMap.containsKey("UniProtKB accession")) {
                    builder.accession(new UniProtAccessionImpl(propertyMap.get("UniProtKB accession")));
                }
                if (propertyMap.containsKey("UniParc ID")) {
                    builder.uniparcId(new UniParcIdImpl(propertyMap.get("UniParc ID")));
                }
                if (propertyMap.containsKey("UniRef50 ID")) {
                    builder.uniref50Id(new UniRefEntryIdImpl(propertyMap.get("UniRef50 ID")));
                }
                if (propertyMap.containsKey("UniRef90 ID")) {
                    builder.uniref90Id(new UniRefEntryIdImpl(propertyMap.get("UniRef90 ID")));
                }
                if (propertyMap.containsKey("UniRef100 ID")) {
                    builder.uniref100Id(new UniRefEntryIdImpl(propertyMap.get("UniRef100 ID")));
                }
                if (propertyMap.containsKey("overlap region")) {
                    String overlap = propertyMap.get("overlap region");
                    int start = new Integer(overlap.substring(0, overlap.indexOf("-")));
                    int end = new Integer(overlap.substring(overlap.indexOf("-") + 1));
                    builder.overlapRegion(new OverlapRegionImpl(start, end));
                }
                builder.proteinName(propertyMap.get("protein name"));
                builder.organismName(propertyMap.get("source organism"));
                builder.organismTaxId(Integer.valueOf(propertyMap.getOrDefault("NCBI taxonomy", "0")));
                builder.sequenceLength(Integer.valueOf(propertyMap.getOrDefault("length", "0")));
                builder.isSeed(Boolean.getBoolean(propertyMap.getOrDefault("isSeed", "false")));
            }
        }
        return builder.build();
    }

    private Map<String, String> convertProperties(Row rowValue) {
        List<Row> properties = rowValue.getList(rowValue.fieldIndex("property"));
        if (properties == null) {
            Row member = (Row) rowValue.get(rowValue.fieldIndex("property"));
            properties = Collections.singletonList(member);
        }
        Map<String, String> propertyMap = new HashMap<>();
        properties.forEach(property -> {
            if (hasFieldName("_type", property) && hasFieldName("_value", property)) {
                String type = property.getString(property.fieldIndex("_type"));
                String value = property.getString(property.fieldIndex("_value"));
                propertyMap.put(type, value);
            }
        });
        return propertyMap;
    }

    public static StructType getUniRefXMLSchema() {
        StructType structType = new StructType();
        structType = structType.add("_id", DataTypes.StringType, true);
        structType = structType.add("_updated", DataTypes.StringType, true);
        structType = structType.add("member", DataTypes.createArrayType(getMemberSchema()), true);
        structType = structType.add("name", DataTypes.StringType, true);
        structType = structType.add("property", DataTypes.createArrayType(getPropertySchema()), true);

        StructType representativeMember = getMemberSchema();
        representativeMember = representativeMember.add("sequence", getSequenceSchema(), true);

        structType = structType.add("representativeMember", representativeMember, true);
        return structType;

    }

    private static StructType getMemberSchema() {
        StructType dbReference = new StructType();
        dbReference = dbReference.add("_id", DataTypes.StringType, true);
        dbReference = dbReference.add("_type", DataTypes.StringType, true);
        dbReference = dbReference.add("property", DataTypes.createArrayType(getPropertySchema()), true);

        StructType member = new StructType();
        member = member.add("dbReference", dbReference, true);
        return member;
    }

    private static StructType getPropertySchema() {
        StructType structType = new StructType();
        structType = structType.add("_VALUE", DataTypes.StringType, true);
        structType = structType.add("_type", DataTypes.StringType, true);
        structType = structType.add("_value", DataTypes.StringType, true);
        return structType;
    }

    private static StructType getSequenceSchema() {
        StructType structType = new StructType();
        structType = structType.add("_VALUE", DataTypes.StringType, true);
        structType = structType.add("_checksum", DataTypes.StringType, true);
        structType = structType.add("_length", DataTypes.LongType, true);
        return structType;
    }

}
