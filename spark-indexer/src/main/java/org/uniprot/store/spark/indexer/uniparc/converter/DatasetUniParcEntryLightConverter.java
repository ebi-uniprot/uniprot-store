package org.uniprot.store.spark.indexer.uniparc.converter;

import static org.uniprot.store.spark.indexer.common.util.RowUtils.hasFieldName;
import static org.uniprot.store.spark.indexer.uniparc.converter.DatasetUniParcEntryConverter.*;

import java.io.Serial;
import java.io.Serializable;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.util.*;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Row;
import org.uniprot.core.Location;
import org.uniprot.core.uniparc.*;
import org.uniprot.core.uniparc.impl.InterProGroupBuilder;
import org.uniprot.core.uniparc.impl.SequenceFeatureBuilder;
import org.uniprot.core.uniparc.impl.UniParcEntryLightBuilder;
import org.uniprot.core.util.PairImpl;
import org.uniprot.store.spark.indexer.common.util.RowUtils;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DatasetUniParcEntryLightConverter
        implements MapFunction<Row, UniParcEntryLight>, Serializable {
    @Serial private static final long serialVersionUID = 2705637837095269737L;

    @Override
    public UniParcEntryLight call(Row rowValue) throws Exception {
        // keep xrefId and current repetition count
        Map<String, Integer> xrefIdCount = new HashMap<>();
        Set<String> taxonIds = new HashSet<>();
        UniParcEntryLightBuilder builder = new UniParcEntryLightBuilder();
        String uniParcId = rowValue.getString(rowValue.fieldIndex(ACCESSION));
        builder.uniParcId(uniParcId);
        LocalDate mostRecentUpdated = LocalDate.MIN;
        LocalDate oldestCreated = LocalDate.MAX;
        if (hasFieldName(DB_REFERENCE, rowValue)) {
            List<Row> dbReferences = rowValue.getList(rowValue.fieldIndex(DB_REFERENCE));
            for (Row dbReference : dbReferences) {
                String uniProtKBAccession = getUniProtKBAccession(dbReference);
                builder.uniProtKBAccessionsAdd(uniProtKBAccession);
                // get uniparc xref composite key
                String uniParcXrefId = getUniParcXRefId(dbReference);
                if (xrefIdCount.containsKey(
                        uniParcXrefId)) { // add the next suffix from map in case of collision
                    String suffixedUniParcXrefId =
                            uniParcXrefId + "-" + xrefIdCount.get(uniParcXrefId);
                    xrefIdCount.put(uniParcXrefId, xrefIdCount.get(uniParcXrefId) + 1);
                    uniParcXrefId = suffixedUniParcXrefId;
                } else {
                    xrefIdCount.put(uniParcXrefId, 1);
                }

                // get most recent updated xref
                LocalDate createdDate = getMostRecentCrossRefUpdated(dbReference);
                mostRecentUpdated =
                        Objects.isNull(createdDate) || mostRecentUpdated.isAfter(createdDate)
                                ? mostRecentUpdated
                                : createdDate;

                // get oldest xref created
                LocalDate lastDate = getOldestCrossRefCreated(dbReference);
                oldestCreated =
                        Objects.isNull(lastDate) || oldestCreated.isBefore(lastDate)
                                ? oldestCreated
                                : lastDate;

                builder.uniParcCrossReferencesAdd(uniParcXrefId);
                String taxonId = getTaxonomyId(dbReference);
                if (Objects.nonNull(taxonId) && !taxonIds.contains(taxonId)) {
                    builder.commonTaxonsAdd(new PairImpl<>(taxonId, ""));
                }
                taxonIds.add(taxonId);
            }
        }
        // do not update if they have initial value
        if (!LocalDate.MIN.equals(mostRecentUpdated)) {
            builder.mostRecentCrossRefUpdated(mostRecentUpdated);
        }
        if (!LocalDate.MAX.equals(oldestCreated)) {
            builder.oldestCrossRefCreated(oldestCreated);
        }

        if (hasFieldName(SIGNATURE_SEQUENCE_MATCH, rowValue)) {
            List<Row> sequenceFeatures =
                    rowValue.getList(rowValue.fieldIndex(SIGNATURE_SEQUENCE_MATCH));
            sequenceFeatures.stream()
                    .map(this::convertSequenceFeatures)
                    .forEach(builder::sequenceFeaturesAdd);
        }
        if (hasFieldName(SEQUENCE, rowValue)) {
            Row sequence = (Row) rowValue.get(rowValue.fieldIndex(SEQUENCE));
            builder.sequence(RowUtils.convertSequence(sequence));
        }

        return builder.build();
    }

    private SequenceFeature convertSequenceFeatures(Row rowValue) {
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

    private InterProGroup convertInterproGroup(Row rowValue) {
        InterProGroupBuilder builder = new InterProGroupBuilder();
        if (hasFieldName(ID, rowValue)) {
            builder.id(rowValue.getString(rowValue.fieldIndex(ID)));
        }
        if (hasFieldName(NAME, rowValue)) {
            builder.name(rowValue.getString(rowValue.fieldIndex(NAME)));
        }
        return builder.build();
    }

    private Location convertLocation(Row rowValue) {
        Long start = 0L;
        if (hasFieldName(START, rowValue)) {
            start = rowValue.getLong(rowValue.fieldIndex(START));
        }
        Long end = 0L;
        if (hasFieldName(END, rowValue)) {
            end = rowValue.getLong(rowValue.fieldIndex(END));
        }
        return new Location(start.intValue(), end.intValue());
    }

    private String getUniProtKBAccession(Row rowValue) {

        String uniProtKBAccession = null;
        String id = rowValue.getString(rowValue.fieldIndex(ID));

        String databaseType = rowValue.getString(rowValue.fieldIndex(TYPE));
        if (UniParcDatabase.TREMBL == UniParcDatabase.typeOf(databaseType)
                || UniParcDatabase.SWISSPROT == UniParcDatabase.typeOf(databaseType)) {

            uniProtKBAccession = id;
        }

        return uniProtKBAccession;
    }

    private String getUniParcXRefId(Row rowValue) {
        String id = rowValue.getString(rowValue.fieldIndex(ID));
        String databaseType = rowValue.getString(rowValue.fieldIndex(TYPE));
        StringBuilder xrefIdBuilder =
                new StringBuilder(UniParcDatabase.typeOf(databaseType).name());
        xrefIdBuilder.append("-");
        xrefIdBuilder.append(id);
        return xrefIdBuilder.toString();
    }

    private String getTaxonomyId(Row rowValue) {
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

    private LocalDate getMostRecentCrossRefUpdated(Row rowValue) {
        DateTimeFormatter formatter = getDateTimeFormatter();
        if (hasFieldName(CREATED, rowValue)) {
            String created = rowValue.getString(rowValue.fieldIndex(CREATED));
            return LocalDate.parse(created, formatter);
        }
        return null;
    }

    private LocalDate getOldestCrossRefCreated(Row rowValue) {
        DateTimeFormatter formatter = getDateTimeFormatter();
        if (hasFieldName(LAST, rowValue)) {
            String last = rowValue.getString(rowValue.fieldIndex(LAST));
            return LocalDate.parse(last, formatter);
        }
        return null;
    }

    private DateTimeFormatter getDateTimeFormatter() {
        DateTimeFormatter formatter =
                new DateTimeFormatterBuilder()
                        .parseCaseInsensitive()
                        .append(DateTimeFormatter.ofPattern("yyyy-MM-dd"))
                        .toFormatter();
        return formatter;
    }
}
