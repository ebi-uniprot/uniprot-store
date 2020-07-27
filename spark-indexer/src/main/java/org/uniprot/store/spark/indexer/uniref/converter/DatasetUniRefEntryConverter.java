package org.uniprot.store.spark.indexer.uniref.converter;

import static org.uniprot.store.spark.indexer.common.util.RowUtils.hasFieldName;
import static org.uniprot.store.spark.indexer.uniref.UniRefXmlUtils.*;

import java.io.Serializable;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Row;
import org.uniprot.core.uniparc.impl.UniParcIdBuilder;
import org.uniprot.core.uniprotkb.impl.UniProtKBAccessionBuilder;
import org.uniprot.core.uniref.*;
import org.uniprot.core.uniref.impl.*;
import org.uniprot.store.spark.indexer.common.util.RowUtils;

/**
 * This class convert XML Row result to a UniRefEntry
 *
 * @author lgonzales
 * @since 2019-10-01
 */
public class DatasetUniRefEntryConverter implements Function<Row, UniRefEntry>, Serializable {

    private static final long serialVersionUID = -526130623950089875L;
    private final UniRefType uniRefType;

    public DatasetUniRefEntryConverter(UniRefType uniRefType) {
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
            if (propertyMap.containsKey(PROPERTY_COMMON_TAXON)) {
                String commonTaxon = propertyMap.get(PROPERTY_COMMON_TAXON).get(0);
                builder.commonTaxon(commonTaxon);
            }
            if (propertyMap.containsKey(PROPERTY_COMMON_TAXON_ID)) {
                String commonTaxonId = propertyMap.get(PROPERTY_COMMON_TAXON_ID).get(0);
                builder.commonTaxonId(Long.valueOf(commonTaxonId));
            }
            builder.goTermsSet(convertUniRefGoTermsProperties(propertyMap));
        }

        if (hasFieldName(MEMBER, rowValue)) {
            List<Row> members = rowValue.getList(rowValue.fieldIndex(MEMBER));
            if (members == null) {
                Row member = (Row) rowValue.get(rowValue.fieldIndex(MEMBER));
                members = Collections.singletonList(member);
            }
            members.stream().map(this::convertMember).forEach(builder::membersAdd);
        }

        if (hasFieldName(REPRESENTATIVE_MEMBER, rowValue)) {
            Row representativeMemberRow =
                    (Row) rowValue.get(rowValue.fieldIndex(REPRESENTATIVE_MEMBER));
            builder.representativeMember(convertRepresentativeMember(representativeMemberRow));
        }

        return builder.build();
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
            builder.memberId(dbReference.getString(dbReference.fieldIndex(ID)));
            String memberType = dbReference.getString(dbReference.fieldIndex("_type"));
            builder.memberIdType(UniRefMemberIdType.typeOf(memberType));

            if (hasFieldName(PROPERTY, dbReference)) {
                Map<String, List<String>> propertyMap = RowUtils.convertProperties(dbReference);
                if (propertyMap.containsKey(PROPERTY_ACCESSION)) {
                    propertyMap.get(PROPERTY_ACCESSION).stream()
                            .map(val -> new UniProtKBAccessionBuilder(val).build())
                            .forEach(builder::accessionsAdd);
                }
                if (propertyMap.containsKey(PROPERTY_UNIPARC_ID)) {
                    String uniparcId = propertyMap.get(PROPERTY_UNIPARC_ID).get(0);
                    builder.uniparcId(new UniParcIdBuilder(uniparcId).build());
                }
                if (propertyMap.containsKey(PROPERTY_UNIREF_50_ID)) {
                    String uniref50 = propertyMap.get(PROPERTY_UNIREF_50_ID).get(0);
                    builder.uniref50Id(new UniRefEntryIdBuilder(uniref50).build());
                }
                if (propertyMap.containsKey(PROPERTY_UNIREF_90_ID)) {
                    String uniref90 = propertyMap.get(PROPERTY_UNIREF_90_ID).get(0);
                    builder.uniref90Id(new UniRefEntryIdBuilder(uniref90).build());
                }
                if (propertyMap.containsKey(PROPERTY_UNIREF_100_ID)) {
                    String uniref100 = propertyMap.get(PROPERTY_UNIREF_100_ID).get(0);
                    builder.uniref100Id(new UniRefEntryIdBuilder(uniref100).build());
                }
                if (propertyMap.containsKey(PROPERTY_OVERLAP_REGION)) {
                    String overlap = propertyMap.get(PROPERTY_OVERLAP_REGION).get(0);
                    int start = Integer.parseInt(overlap.substring(0, overlap.indexOf('-')));
                    int end = Integer.parseInt(overlap.substring(overlap.indexOf('-') + 1));
                    builder.overlapRegion(new OverlapRegionBuilder().start(start).end(end).build());
                }
                if (propertyMap.containsKey(PROPERTY_PROTEIN_NAME)) {
                    builder.proteinName(propertyMap.get(PROPERTY_PROTEIN_NAME).get(0));
                }
                if (propertyMap.containsKey(PROPERTY_ORGANISM)) {
                    builder.organismName(propertyMap.get(PROPERTY_ORGANISM).get(0));
                }
                if (propertyMap.containsKey(PROPERTY_TAXONOMY)) {
                    builder.organismTaxId(
                            Long.parseLong(propertyMap.get(PROPERTY_TAXONOMY).get(0)));
                }
                if (propertyMap.containsKey(PROPERTY_LENGTH)) {
                    builder.sequenceLength(
                            Integer.parseInt(propertyMap.get(PROPERTY_LENGTH).get(0)));
                }
                if (propertyMap.containsKey(PROPERTY_IS_SEED)) {
                    builder.isSeed(Boolean.valueOf(propertyMap.get(PROPERTY_IS_SEED).get(0)));
                }
            }
        }
        return builder.build();
    }
}
