package org.uniprot.store.spark.indexer.uniref.converter;

import static org.uniprot.store.spark.indexer.common.util.RowUtils.hasFieldName;
import static org.uniprot.store.spark.indexer.uniref.UniRefXmlUtils.*;
import static org.uniprot.store.spark.indexer.uniref.UniRefXmlUtils.PROPERTY_IS_SEED;

import java.util.List;
import java.util.Map;

import org.apache.spark.sql.Row;
import org.uniprot.core.uniparc.impl.UniParcIdBuilder;
import org.uniprot.core.uniprotkb.impl.UniProtKBAccessionBuilder;
import org.uniprot.core.uniref.RepresentativeMember;
import org.uniprot.core.uniref.UniRefMember;
import org.uniprot.core.uniref.UniRefMemberIdType;
import org.uniprot.core.uniref.impl.OverlapRegionBuilder;
import org.uniprot.core.uniref.impl.RepresentativeMemberBuilder;
import org.uniprot.core.uniref.impl.UniRefEntryIdBuilder;
import org.uniprot.core.uniref.impl.UniRefMemberBuilder;
import org.uniprot.store.spark.indexer.common.util.RowUtils;

/**
 * @author lgonzales
 * @since 10/12/2020
 */
class DatasetUniRefConverterUtil {

    private DatasetUniRefConverterUtil() {}

    static RepresentativeMember convertRepresentativeMember(Row representativeMemberRow) {
        RepresentativeMemberBuilder builder =
                RepresentativeMemberBuilder.from(convertMember(representativeMemberRow));
        if (hasFieldName(SEQUENCE, representativeMemberRow)) {
            Row sequence =
                    (Row) representativeMemberRow.get(representativeMemberRow.fieldIndex(SEQUENCE));
            builder.sequence(RowUtils.convertSequence(sequence));
        }
        return builder.build();
    }

    static UniRefMember convertMember(Row member) {
        UniRefMemberBuilder builder = new UniRefMemberBuilder();
        if (hasFieldName(DB_REFERENCE, member)) {
            Row dbReference = (Row) member.get(member.fieldIndex(DB_REFERENCE));
            builder.memberId(dbReference.getString(dbReference.fieldIndex(ID)));
            String memberType = dbReference.getString(dbReference.fieldIndex("_type"));
            builder.memberIdType(UniRefMemberIdType.typeOf(memberType));

            if (hasFieldName(PROPERTY, dbReference)) {
                convertMemberProperties(builder, dbReference);
            }
        }
        return builder.build();
    }

    static void convertMemberProperties(UniRefMemberBuilder builder, Row dbReference) {
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
            builder.organismTaxId(Long.parseLong(propertyMap.get(PROPERTY_TAXONOMY).get(0)));
        }
        if (propertyMap.containsKey(PROPERTY_LENGTH)) {
            builder.sequenceLength(Integer.parseInt(propertyMap.get(PROPERTY_LENGTH).get(0)));
        }
        if (propertyMap.containsKey(PROPERTY_IS_SEED)) {
            builder.isSeed(Boolean.valueOf(propertyMap.get(PROPERTY_IS_SEED).get(0)));
        }
    }
}
