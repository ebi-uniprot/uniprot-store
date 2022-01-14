package org.uniprot.store.spark.indexer.uniref.converter;

import static org.uniprot.core.uniref.UniRefUtils.getOrganismCommonName;
import static org.uniprot.core.uniref.UniRefUtils.getOrganismScientificName;
import static org.uniprot.store.spark.indexer.common.util.RowUtils.hasFieldName;
import static org.uniprot.store.spark.indexer.uniref.UniRefXmlUtils.*;
import static org.uniprot.store.spark.indexer.uniref.converter.DatasetUniRefConverterUtil.*;

import java.io.Serializable;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Row;
import org.uniprot.core.uniprotkb.taxonomy.impl.OrganismBuilder;
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
                String memberCountStr = propertyMap.get(PROPERTY_MEMBER_COUNT).get(0);
                int memberCount = Integer.parseInt(memberCountStr);
                builder.memberCount(memberCount);
            }
            if (propertyMap.containsKey(PROPERTY_COMMON_TAXON_ID)) {
                OrganismBuilder organismBuilder = new OrganismBuilder();
                if (propertyMap.containsKey(PROPERTY_COMMON_TAXON)) {
                    String commonTaxon = propertyMap.get(PROPERTY_COMMON_TAXON).get(0);
                    organismBuilder.commonName(getOrganismCommonName(commonTaxon));
                    organismBuilder.scientificName(getOrganismScientificName(commonTaxon));
                }
                String commonTaxonId = propertyMap.get(PROPERTY_COMMON_TAXON_ID).get(0);
                organismBuilder.taxonId(Long.parseLong(commonTaxonId));
                builder.commonTaxon(organismBuilder.build());
            }
            builder.goTermsSet(convertUniRefGoTermsProperties(propertyMap));
        }

        if (hasFieldName(MEMBER, rowValue)) {
            List<Row> members = rowValue.getList(rowValue.fieldIndex(MEMBER));
            if (members == null) {
                Row member = (Row) rowValue.get(rowValue.fieldIndex(MEMBER));
                members = Collections.singletonList(member);
            }
            members.stream()
                    .map(DatasetUniRefConverterUtil::convertMember)
                    .forEach(builder::membersAdd);
        }

        if (hasFieldName(REPRESENTATIVE_MEMBER, rowValue)) {
            Row representativeMemberRow =
                    (Row) rowValue.get(rowValue.fieldIndex(REPRESENTATIVE_MEMBER));
            builder.representativeMember(convertRepresentativeMember(representativeMemberRow));
        }

        return builder.build();
    }
}
