package org.uniprot.store.spark.indexer.uniparc.converter;

import java.io.Serial;
import java.time.LocalDate;
import java.util.*;

import org.apache.spark.sql.Row;
import org.uniprot.core.uniparc.UniParcDatabase;
import org.uniprot.core.uniparc.UniParcEntryLight;
import org.uniprot.core.uniparc.impl.UniParcEntryLightBuilder;
import org.uniprot.core.util.PairImpl;
import org.uniprot.store.spark.indexer.common.util.RowUtils;

public class DatasetUniParcEntryLightConverter
        extends BaseUniParcEntryConverter<UniParcEntryLight> {

    @Serial private static final long serialVersionUID = -2217763341816079882L;

    @Override
    public UniParcEntryLight call(Row rowValue) throws Exception {
        Map<String, Integer> xrefIdCount = new HashMap<>();
        Set<String> taxonIds = new HashSet<>();
        UniParcEntryLightBuilder builder = new UniParcEntryLightBuilder();
        String uniParcId = rowValue.getString(rowValue.fieldIndex(ACCESSION));
        builder.uniParcId(uniParcId);
        LocalDate mostRecentUpdated = LocalDate.MIN;
        LocalDate oldestCreated = LocalDate.MAX;

        if (RowUtils.hasFieldName(DB_REFERENCE, rowValue)) {
            List<Row> dbReferences = rowValue.getList(rowValue.fieldIndex(DB_REFERENCE));
            for (Row dbReference : dbReferences) {
                String uniProtKBAccession = getUniProtKBAccession(dbReference);
                builder.uniProtKBAccessionsAdd(uniProtKBAccession);

                String uniParcXrefId = getUniParcXRefId(uniParcId, dbReference);
                if (xrefIdCount.containsKey(uniParcXrefId)) {
                    String suffixedUniParcXrefId =
                            uniParcXrefId + "-" + xrefIdCount.get(uniParcXrefId);
                    xrefIdCount.put(uniParcXrefId, xrefIdCount.get(uniParcXrefId) + 1);
                    uniParcXrefId = suffixedUniParcXrefId;
                } else {
                    xrefIdCount.put(uniParcXrefId, 1);
                }

                LocalDate createdDate = parseDate(dbReference, CREATED);
                mostRecentUpdated =
                        Objects.isNull(createdDate) || mostRecentUpdated.isAfter(createdDate)
                                ? mostRecentUpdated
                                : createdDate;

                LocalDate lastDate = parseDate(dbReference, LAST);
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

        if (!LocalDate.MIN.equals(mostRecentUpdated)) {
            builder.mostRecentCrossRefUpdated(mostRecentUpdated);
        }
        if (!LocalDate.MAX.equals(oldestCreated)) {
            builder.oldestCrossRefCreated(oldestCreated);
        }

        if (RowUtils.hasFieldName(SIGNATURE_SEQUENCE_MATCH, rowValue)) {
            List<Row> sequenceFeatures =
                    rowValue.getList(rowValue.fieldIndex(SIGNATURE_SEQUENCE_MATCH));
            sequenceFeatures.stream()
                    .map(this::convertSequenceFeatures)
                    .forEach(builder::sequenceFeaturesAdd);
        }
        if (RowUtils.hasFieldName(SEQUENCE, rowValue)) {
            Row sequence = (Row) rowValue.get(rowValue.fieldIndex(SEQUENCE));
            builder.sequence(RowUtils.convertSequence(sequence));
        }

        return builder.build();
    }

    private String getUniProtKBAccession(Row rowValue) {
        String id = rowValue.getString(rowValue.fieldIndex(ID));
        String databaseType = rowValue.getString(rowValue.fieldIndex(TYPE));
        UniParcDatabase database = UniParcDatabase.typeOf(databaseType);

        if (UniParcDatabase.TREMBL == database || UniParcDatabase.SWISSPROT == database) {
            return id;
        }
        return null;
    }

    private String getUniParcXRefId(String uniParcId, Row rowValue) {
        String id = rowValue.getString(rowValue.fieldIndex(ID));
        String databaseType = rowValue.getString(rowValue.fieldIndex(TYPE));
        return uniParcId + "-" + UniParcDatabase.typeOf(databaseType).name() + "-" + id;
    }
}