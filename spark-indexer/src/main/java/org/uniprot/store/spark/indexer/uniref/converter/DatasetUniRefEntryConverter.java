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
import org.uniprot.core.cv.go.GeneOntologyEntry;
import org.uniprot.core.cv.go.GoAspect;
import org.uniprot.core.cv.go.builder.GeneOntologyEntryBuilder;
import org.uniprot.core.uniparc.impl.UniParcIdImpl;
import org.uniprot.core.uniprot.impl.UniProtAccessionImpl;
import org.uniprot.core.uniref.*;
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
public class DatasetUniRefEntryConverter implements MapFunction<Row, UniRefEntry>, Serializable {

    private static final String PROPERTY_MEMBER_COUNT = "member count";
    private static final String PROPERTY_COMMON_TAXON = "common taxon";
    private static final String PROPERTY_COMMON_TAXON_ID = "common taxon ID";
    private static final String PROPERTY_GO_FUNCTION = "GO Molecular Function";
    private static final String PROPERTY_GO_COMPONENT = "GO Cellular Component";
    private static final String PROPERTY_GO_PROCESS = "GO Biological Process";

    private static final String PROPERTY_ACCESSION = "UniProtKB accession";
    private static final String PROPERTY_UNIPARC_ID = "UniParc ID";
    private static final String PROPERTY_UNIREF_50_ID = "UniRef50 ID";
    private static final String PROPERTY_UNIREF_90_ID = "UniRef90 ID";
    private static final String PROPERTY_UNIREF_100_ID = "UniRef100 ID";
    private static final String PROPERTY_OVERLAP_REGION = "overlap region";
    private static final String PROPERTY_PROTEIN_NAME = "protein name";
    private static final String PROPERTY_ORGANISM = "source organism";
    private static final String PROPERTY_TAXONOMY = "NCBI taxonomy";
    private static final String PROPERTY_LENGTH = "length";
    private static final String PROPERTY_IS_SEED = "isSeed";

    private static final String DB_REFERENCE = "dbReference";
    private static final String SEQUENCE = "sequence";
    private static final String ID = "_id";
    private static final String NAME = "name";
    private static final String UPDATED = "_updated";
    private static final String MEMBER = "member";
    private static final String PROPERTY = "property";
    private static final String REPRESENTATIVE_MEMBER = "representativeMember";

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
                        .map(goTerm -> createGoTerm(GoAspect.FUNCTION, goTerm))
                        .forEach(builder::goTermsAdd);
            }
            if (propertyMap.containsKey(PROPERTY_GO_COMPONENT)) {
                propertyMap.get(PROPERTY_GO_COMPONENT).stream()
                        .map(goTerm -> createGoTerm(GoAspect.FUNCTION, goTerm))
                        .forEach(builder::goTermsAdd);
            }
            if (propertyMap.containsKey(PROPERTY_GO_PROCESS)) {
                propertyMap.get(PROPERTY_GO_PROCESS).stream()
                        .map(goTerm -> createGoTerm(GoAspect.FUNCTION, goTerm))
                        .forEach(builder::goTermsAdd);
            }
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

    private GeneOntologyEntry createGoTerm(GoAspect type, String id) {
        return new GeneOntologyEntryBuilder().aspect(type).id(id).build();
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
                            .map(UniProtAccessionImpl::new)
                            .forEach(builder::accessionsAdd);
                }
                if (propertyMap.containsKey(PROPERTY_UNIPARC_ID)) {
                    String uniparcId = propertyMap.get(PROPERTY_UNIPARC_ID).get(0);
                    builder.uniparcId(new UniParcIdImpl(uniparcId));
                }
                if (propertyMap.containsKey(PROPERTY_UNIREF_50_ID)) {
                    String uniref50 = propertyMap.get(PROPERTY_UNIREF_50_ID).get(0);
                    builder.uniref50Id(new UniRefEntryIdImpl(uniref50));
                }
                if (propertyMap.containsKey(PROPERTY_UNIREF_90_ID)) {
                    String uniref90 = propertyMap.get(PROPERTY_UNIREF_90_ID).get(0);
                    builder.uniref90Id(new UniRefEntryIdImpl(uniref90));
                }
                if (propertyMap.containsKey(PROPERTY_UNIREF_100_ID)) {
                    String uniref100 = propertyMap.get(PROPERTY_UNIREF_100_ID).get(0);
                    builder.uniref100Id(new UniRefEntryIdImpl(uniref100));
                }
                if (propertyMap.containsKey(PROPERTY_OVERLAP_REGION)) {
                    String overlap = propertyMap.get(PROPERTY_OVERLAP_REGION).get(0);
                    int start = Integer.valueOf(overlap.substring(0, overlap.indexOf("-")));
                    int end = Integer.valueOf(overlap.substring(overlap.indexOf("-") + 1));
                    builder.overlapRegion(new OverlapRegionImpl(start, end));
                }
                if (propertyMap.containsKey(PROPERTY_PROTEIN_NAME)) {
                    builder.proteinName(propertyMap.get(PROPERTY_PROTEIN_NAME).get(0));
                }
                if (propertyMap.containsKey(PROPERTY_ORGANISM)) {
                    builder.organismName(propertyMap.get(PROPERTY_ORGANISM).get(0));
                }
                if (propertyMap.containsKey(PROPERTY_TAXONOMY)) {
                    builder.organismTaxId(Long.valueOf(propertyMap.get(PROPERTY_TAXONOMY).get(0)));
                }
                if (propertyMap.containsKey(PROPERTY_LENGTH)) {
                    builder.sequenceLength(
                            Integer.valueOf(propertyMap.get(PROPERTY_LENGTH).get(0)));
                }
                if (propertyMap.containsKey(PROPERTY_IS_SEED)) {
                    builder.isSeed(Boolean.valueOf(propertyMap.get(PROPERTY_IS_SEED).get(0)));
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
