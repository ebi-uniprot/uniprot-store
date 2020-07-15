package org.uniprot.store.spark.indexer.uniref.converter;

import static org.uniprot.core.uniref.UniRefUtils.*;
import static org.uniprot.store.spark.indexer.common.util.RowUtils.hasFieldName;
import static org.uniprot.store.spark.indexer.uniref.UniRefXmlUtils.*;

import java.io.Serializable;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.uniprot.core.Value;
import org.uniprot.core.uniprotkb.impl.UniProtKBAccessionBuilder;
import org.uniprot.core.uniref.*;
import org.uniprot.core.uniref.impl.RepresentativeMemberBuilder;
import org.uniprot.core.uniref.impl.UniRefEntryLightBuilder;
import org.uniprot.core.uniref.impl.UniRefMemberBuilder;
import org.uniprot.core.util.Utils;
import org.uniprot.store.spark.indexer.common.util.RowUtils;

/**
 * Converts XML {@link Row} instances to {@link UniRefEntryLight} instances.
 *
 * <p>Created 30/06/2020
 *
 * @author Edd
 */
public class DatasetUniRefEntryLightConverter
        implements Function<Row, UniRefEntryLight>, Serializable {

    private static final long serialVersionUID = -5612011317846388428L;
    private static final int ORGANISMS_COUNT_MAX = 10;
    private final UniRefType uniRefType;
    private final AtomicInteger organismsCount;

    public DatasetUniRefEntryLightConverter(UniRefType uniRefType) {
        this.uniRefType = uniRefType;
        this.organismsCount = new AtomicInteger();
    }

    /**
     * @param rowValue XML Row
     * @return mapped UniRefEntry
     */
    @Override
    public UniRefEntryLight call(Row rowValue) throws Exception {
        UniRefEntryLightBuilder builder = new UniRefEntryLightBuilder();
        builder.entryType(uniRefType);
        builder.id(rowValue.getString(rowValue.fieldIndex(ID)));
        DateTimeFormatter formatter =
                new DateTimeFormatterBuilder()
                        .parseCaseInsensitive()
                        .append(DateTimeFormatter.ofPattern("yyyy-MM-dd"))
                        .toFormatter();
        String xmlUpdatedDate = rowValue.getString(rowValue.fieldIndex(UPDATED));
        builder.updated(LocalDate.parse(xmlUpdatedDate, formatter));

        if (hasFieldName(NAME, rowValue)) {
            builder.name(rowValue.getString(rowValue.fieldIndex(NAME)));
        }
        if (hasFieldName(PROPERTY, rowValue)) {
            Map<String, List<String>> propertyMap = RowUtils.convertProperties(rowValue);
            if (propertyMap.containsKey(PROPERTY_MEMBER_COUNT)) {
                String memberCount = propertyMap.get(PROPERTY_MEMBER_COUNT).get(0);
                builder.memberCount(Integer.parseInt(memberCount));
            }
            if (propertyMap.containsKey(PROPERTY_COMMON_TAXON_ID)) {
                String commonTaxonId = propertyMap.get(PROPERTY_COMMON_TAXON_ID).get(0);
                builder.commonTaxonId(Integer.parseInt(commonTaxonId));
            }
            if (propertyMap.containsKey(PROPERTY_COMMON_TAXON)) {
                String commonTaxon = propertyMap.get(PROPERTY_COMMON_TAXON).get(0);
                builder.commonTaxon(commonTaxon);
            }
        }

        if (hasFieldName(MEMBER, rowValue)) {
            List<Row> members = rowValue.getList(rowValue.fieldIndex(MEMBER));
            if (members == null) {
                Row member = (Row) rowValue.get(rowValue.fieldIndex(MEMBER));
                members = Collections.singletonList(member);
            }
            members.stream()
                    .map(this::convertMember)
                    .forEach(member -> addMemberInfo(builder, member));
        }

        if (hasFieldName(REPRESENTATIVE_MEMBER, rowValue)) {
            Row representativeMemberRow =
                    (Row) rowValue.get(rowValue.fieldIndex(REPRESENTATIVE_MEMBER));
            RepresentativeMember representativeMember =
                    convertRepresentativeMember(representativeMemberRow);
            // member accessions
            builder.sequence(representativeMember.getSequence().getValue());

            builder.representativeId(representativeMember.getMemberId());

            addMemberInfo(builder, representativeMember);
        }

        return builder.build();
    }

    private void addMemberInfo(UniRefEntryLightBuilder builder, UniRefMember member) {
        // member accessions
        member.getUniProtAccessions().stream()
                .map(Value::getValue)
                .findFirst()
                .ifPresent(builder::membersAdd);

        // organism name and id
        if (member.getOrganismTaxId() > 0) {
            builder.organismIdsAdd(member.getOrganismTaxId());
        }
        if (organismsCount.get() < ORGANISMS_COUNT_MAX
                && Utils.notNullNotEmpty(member.getOrganismName())) {
            builder.organismsAdd(member.getOrganismName());
            organismsCount.getAndIncrement();
        }

        // uniparc id presence
        String uniparcId = member.getUniParcId() == null ? null : member.getUniParcId().getValue();
        if (member.getMemberIdType() == UniRefMemberIdType.UNIPARC) {
            uniparcId = member.getMemberId();
        }
        if (Utils.notNullNotEmpty(uniparcId)) {
            builder.membersAdd(uniparcId);
        }

        builder.memberIdTypesAdd(member.getMemberIdType());
    }

    private RepresentativeMember convertRepresentativeMember(Row representativeMemberRow) {
        RepresentativeMemberBuilder builder =
                RepresentativeMemberBuilder.from(convertMember(representativeMemberRow));
        if (hasFieldName(SEQUENCE, representativeMemberRow)) {
            Row sequence =
                    (Row) representativeMemberRow.get(representativeMemberRow.fieldIndex(SEQUENCE));
            builder.sequence(RowUtils.convertSequence(sequence));
        }
        return builder.build();
    }

    private UniRefMember convertMember(Row member) {
        UniRefMemberBuilder builder = new UniRefMemberBuilder();
        if (hasFieldName(DB_REFERENCE, member)) {
            Row dbReference = (Row) member.get(member.fieldIndex(DB_REFERENCE));
            String memberId = dbReference.getString(dbReference.fieldIndex(ID));
            builder.memberId(memberId);

            String memberType = dbReference.getString(dbReference.fieldIndex("_type"));
            if (UniRefMemberIdType.typeOf(memberType) == UniRefMemberIdType.UNIPARC) {
                builder.memberIdType(UniRefMemberIdType.UNIPARC);
            }

            if (hasFieldName(PROPERTY, dbReference)) {
                Map<String, List<String>> propertyMap = RowUtils.convertProperties(dbReference);
                if (propertyMap.containsKey(PROPERTY_ACCESSION)) {
                    propertyMap.get(PROPERTY_ACCESSION).stream()
                            .map(val -> new UniProtKBAccessionBuilder(val).build())
                            .forEach(
                                    acc -> {
                                        builder.accessionsAdd(acc);
                                        builder.memberIdType(
                                                getUniProtKBIdType(memberId, acc.getValue()));
                                    });
                }
                if (propertyMap.containsKey(PROPERTY_TAXONOMY)) {
                    builder.organismTaxId(
                            Long.parseLong(propertyMap.get(PROPERTY_TAXONOMY).get(0)));
                }
                if (propertyMap.containsKey(PROPERTY_ORGANISM)) {
                    builder.organismName(propertyMap.get(PROPERTY_ORGANISM).get(0));
                }
                if (propertyMap.containsKey(PROPERTY_PROTEIN_NAME)) {
                    builder.proteinName(propertyMap.get(PROPERTY_PROTEIN_NAME).get(0));
                }
            }
        }
        return builder.build();
    }

    public static StructType getUniRefXMLSchema() {
        StructType structType = new StructType();
        structType = structType.add(ID, DataTypes.StringType, true);
        structType = structType.add(UPDATED, DataTypes.StringType, true);
        structType = structType.add(MEMBER, DataTypes.createArrayType(getMemberSchema()), true);
        structType = structType.add(NAME, DataTypes.StringType, true);
        structType =
                structType.add(
                        PROPERTY, DataTypes.createArrayType(RowUtils.getPropertySchema()), true);
        structType = structType.add(REPRESENTATIVE_MEMBER, getRepresentativeMemberSchema(), true);
        return structType;
    }

    static StructType getRepresentativeMemberSchema() {
        StructType representativeMember = getMemberSchema();
        representativeMember =
                representativeMember.add(SEQUENCE, RowUtils.getSequenceSchema(), true);
        return representativeMember;
    }

    static StructType getMemberSchema() {
        StructType member = new StructType();
        member = member.add(DB_REFERENCE, RowUtils.getDBReferenceSchema(), true);
        return member;
    }
}
