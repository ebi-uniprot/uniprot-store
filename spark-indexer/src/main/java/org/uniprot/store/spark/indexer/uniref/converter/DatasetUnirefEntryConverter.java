package org.uniprot.store.spark.indexer.uniref.converter;

import static org.uniprot.store.spark.indexer.util.RowUtils.hasFieldName;

import java.io.Serializable;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.uniprot.core.uniparc.impl.UniParcIdImpl;
import org.uniprot.core.uniprot.impl.UniProtAccessionImpl;
import org.uniprot.core.uniref.*;
import org.uniprot.core.uniref.builder.GoTermBuilder;
import org.uniprot.core.uniref.builder.RepresentativeMemberBuilder;
import org.uniprot.core.uniref.builder.UniRefEntryBuilder;
import org.uniprot.core.uniref.builder.UniRefMemberBuilder;
import org.uniprot.core.uniref.impl.OverlapRegionImpl;
import org.uniprot.core.uniref.impl.UniRefEntryIdImpl;
import org.uniprot.store.spark.indexer.util.RowUtils;

/**
 * This class convert XML Row result to a UniRefEntry
 *
 * @author lgonzales
 * @since 2019-10-01
 */
public class DatasetUnirefEntryConverter implements MapFunction<Row, UniRefEntry>, Serializable {

    private static final String PROPERTY_MEMBER_COUNT = "member count";
    private static final String PROPERTY_COMMON_TAXON = "common taxon";
    private static final String PROPERTY_COMMON_TAXON_ID = "common taxon ID";
    private static final String PROPERTY_GO_FUNCTION = "GO Molecular Function";
    private static final String PROPERTY_GO_COMPONENT = "GO Cellular Component";
    private static final String PROPERTY_GO_PROCESS = "GO Biological Process";
    private static final long serialVersionUID = -526130623950089875L;
    private final UniRefType uniRefType;

    public DatasetUnirefEntryConverter(UniRefType uniRefType) {
        this.uniRefType = uniRefType;
    }

    /**
     * @param rowValue XML Row
     * @return mapped UniRefEntry
     */
    @Override
    public UniRefEntry call(Row rowValue) throws Exception {
        UniRefEntryBuilder builder = new UniRefEntryBuilder();
        builder.entryType(uniRefType);
        builder.id(rowValue.getString(rowValue.fieldIndex("_id")));
        DateTimeFormatter formatter =
                new DateTimeFormatterBuilder()
                        .parseCaseInsensitive()
                        .append(DateTimeFormatter.ofPattern("yyyy-MM-dd"))
                        .toFormatter();
        String xmlUpdatedDate = rowValue.getString(rowValue.fieldIndex("_updated"));
        builder.updated(LocalDate.parse(xmlUpdatedDate, formatter));

        if (hasFieldName("name", rowValue)) {
            builder.name(rowValue.getString(rowValue.fieldIndex("name")));
        }
        if (hasFieldName("property", rowValue)) {
            Map<String, List<String>> propertyMap = RowUtils.convertProperties(rowValue);
            if (propertyMap.containsKey(PROPERTY_MEMBER_COUNT)) {
                String memberCount = propertyMap.get(PROPERTY_MEMBER_COUNT).get(0);
                builder.memberCount(Integer.valueOf(memberCount));
            }
            if (propertyMap.containsKey(PROPERTY_COMMON_TAXON)) {
                String commonTaxon = propertyMap.get(PROPERTY_COMMON_TAXON).get(0);
                builder.commonTaxon(commonTaxon);
            }
            if (propertyMap.containsKey(PROPERTY_COMMON_TAXON_ID)) {
                String commonTaxonId = propertyMap.get(PROPERTY_COMMON_TAXON_ID).get(0);
                builder.commonTaxonId(Integer.valueOf(commonTaxonId));
            }
            if (propertyMap.containsKey(PROPERTY_GO_FUNCTION)) {
                propertyMap.get(PROPERTY_GO_FUNCTION).stream()
                        .map(goTerm -> createGoTerm(GoTermType.FUNCTION, goTerm))
                        .forEach(builder::goTermsAdd);
            }
            if (propertyMap.containsKey(PROPERTY_GO_COMPONENT)) {
                propertyMap.get(PROPERTY_GO_COMPONENT).stream()
                        .map(goTerm -> createGoTerm(GoTermType.FUNCTION, goTerm))
                        .forEach(builder::goTermsAdd);
            }
            if (propertyMap.containsKey(PROPERTY_GO_PROCESS)) {
                propertyMap.get(PROPERTY_GO_PROCESS).stream()
                        .map(goTerm -> createGoTerm(GoTermType.FUNCTION, goTerm))
                        .forEach(builder::goTermsAdd);
            }
        }

        if (hasFieldName("member", rowValue)) {
            List<Row> members = rowValue.getList(rowValue.fieldIndex("member"));
            if (members == null) {
                Row member = (Row) rowValue.get(rowValue.fieldIndex("member"));
                members = Collections.singletonList(member);
            }
            members.stream().map(this::convertMember).forEach(builder::membersAdd);
        }

        if (hasFieldName("representativeMember", rowValue)) {
            Row representativeMemberRow =
                    (Row) rowValue.get(rowValue.fieldIndex("representativeMember"));
            builder.representativeMember(convertRepresentativeMember(representativeMemberRow));
        }

        return builder.build();
    }

    private GoTerm createGoTerm(GoTermType type, String id) {
        return new GoTermBuilder().type(type).id(id).build();
    }

    private RepresentativeMember convertRepresentativeMember(Row representativeMemberRow) {
        RepresentativeMemberBuilder builder =
                RepresentativeMemberBuilder.from(convertMember(representativeMemberRow));
        if (hasFieldName("sequence", representativeMemberRow)) {
            Row sequence =
                    (Row)
                            representativeMemberRow.get(
                                    representativeMemberRow.fieldIndex("sequence"));
            builder.sequence(RowUtils.convertSequence(sequence));
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
                Map<String, List<String>> propertyMap = RowUtils.convertProperties(dbReference);
                if (propertyMap.containsKey("UniProtKB accession")) {
                    propertyMap.get("UniProtKB accession").stream()
                            .map(UniProtAccessionImpl::new)
                            .forEach(builder::accessionsAdd);
                }
                if (propertyMap.containsKey("UniParc ID")) {
                    String uniparcId = propertyMap.get("UniParc ID").get(0);
                    builder.uniparcId(new UniParcIdImpl(uniparcId));
                }
                if (propertyMap.containsKey("UniRef50 ID")) {
                    String uniref50 = propertyMap.get("UniRef50 ID").get(0);
                    builder.uniref50Id(new UniRefEntryIdImpl(uniref50));
                }
                if (propertyMap.containsKey("UniRef90 ID")) {
                    String uniref90 = propertyMap.get("UniRef90 ID").get(0);
                    builder.uniref90Id(new UniRefEntryIdImpl(uniref90));
                }
                if (propertyMap.containsKey("UniRef100 ID")) {
                    String uniref100 = propertyMap.get("UniRef100 ID").get(0);
                    builder.uniref100Id(new UniRefEntryIdImpl(uniref100));
                }
                if (propertyMap.containsKey("overlap region")) {
                    String overlap = propertyMap.get("overlap region").get(0);
                    int start = new Integer(overlap.substring(0, overlap.indexOf("-")));
                    int end = new Integer(overlap.substring(overlap.indexOf("-") + 1));
                    builder.overlapRegion(new OverlapRegionImpl(start, end));
                }
                if (propertyMap.containsKey("protein name")) {
                    builder.proteinName(propertyMap.get("protein name").get(0));
                }
                if (propertyMap.containsKey("source organism")) {
                    builder.organismName(propertyMap.get("source organism").get(0));
                }
                if (propertyMap.containsKey("NCBI taxonomy")) {
                    builder.organismTaxId(Integer.valueOf(propertyMap.get("NCBI taxonomy").get(0)));
                }
                if (propertyMap.containsKey("length")) {
                    builder.sequenceLength(Integer.valueOf(propertyMap.get("length").get(0)));
                }
                if (propertyMap.containsKey("isSeed")) {
                    builder.isSeed(Boolean.valueOf(propertyMap.get("isSeed").get(0)));
                }
            }
        }
        return builder.build();
    }

    public static StructType getUniRefXMLSchema() {
        StructType structType = new StructType();
        structType = structType.add("_id", DataTypes.StringType, true);
        structType = structType.add("_updated", DataTypes.StringType, true);
        structType = structType.add("member", DataTypes.createArrayType(getMemberSchema()), true);
        structType = structType.add("name", DataTypes.StringType, true);
        structType =
                structType.add(
                        "property", DataTypes.createArrayType(RowUtils.getPropertySchema()), true);
        structType = structType.add("representativeMember", getRepresentativeMemberSchema(), true);
        return structType;
    }

    static StructType getRepresentativeMemberSchema() {
        StructType representativeMember = getMemberSchema();
        representativeMember =
                representativeMember.add("sequence", RowUtils.getSequenceSchema(), true);
        return representativeMember;
    }

    static StructType getMemberSchema() {
        StructType member = new StructType();
        member = member.add("dbReference", RowUtils.getDBReferenceSchema(), true);
        return member;
    }
}
