package org.uniprot.store.spark.indexer.uniparc.converter;

import static org.uniprot.core.uniparc.impl.UniParcEntryLightBuilder.HAS_ACTIVE_CROSS_REF;

import java.io.Serial;
import java.time.LocalDate;
import java.util.*;

import org.apache.spark.sql.Row;
import org.uniprot.core.uniparc.CommonOrganism;
import org.uniprot.core.uniparc.UniParcDatabase;
import org.uniprot.core.uniparc.UniParcEntryLight;
import org.uniprot.core.uniparc.impl.CommonOrganismBuilder;
import org.uniprot.core.uniparc.impl.UniParcEntryLightBuilder;
import org.uniprot.store.spark.indexer.common.util.RowUtils;

public class DatasetUniParcEntryLightConverter
        extends BaseUniParcEntryConverter<UniParcEntryLight> {

    @Serial private static final long serialVersionUID = -2217763341816079882L;

    @Override
    public UniParcEntryLight call(Row rowValue) throws Exception {
        Set<String> taxonIds = new HashSet<>();
        UniParcEntryLightBuilder builder = new UniParcEntryLightBuilder();
        String uniParcId = rowValue.getString(rowValue.fieldIndex(ACCESSION));
        builder.uniParcId(uniParcId);
        LocalDate mostRecentUpdated = LocalDate.MIN;
        LocalDate oldestCreated = LocalDate.MAX;
        boolean hasActiveCrossRef = false;
        if (RowUtils.hasFieldName(DB_REFERENCE, rowValue)) {
            List<Row> dbReferences = rowValue.getList(rowValue.fieldIndex(DB_REFERENCE));
            for (Row dbReference : dbReferences) {
                // set hasActiveCrossRef to true if any cross ref is active
                if (RowUtils.hasFieldName(ACTIVE, dbReference)) {
                    String active = dbReference.getString(dbReference.fieldIndex(ACTIVE));
                    hasActiveCrossRef = hasActiveCrossRef || active.equalsIgnoreCase("Y");
                }
                String uniProtKBAccession = getUniProtKBAccession(dbReference);
                builder.uniProtKBAccessionsAdd(uniProtKBAccession);

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

                String taxonId = getTaxonomyId(dbReference);
                if (Objects.nonNull(taxonId) && !taxonIds.contains(taxonId)) {
                    CommonOrganism commonTaxon =
                            new CommonOrganismBuilder().topLevel(taxonId).commonTaxon("").build();
                    builder.commonTaxonsAdd(commonTaxon);
                }
                taxonIds.add(taxonId);
            }
            builder.crossReferenceCount(dbReferences.size());
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

        if (!hasActiveCrossRef) {
            builder.extraAttributesAdd(HAS_ACTIVE_CROSS_REF, hasActiveCrossRef);
        }

        return builder.build();
    }

    private String getUniProtKBAccession(Row rowValue) {
        String id = rowValue.getString(rowValue.fieldIndex(ID));
        String databaseType = rowValue.getString(rowValue.fieldIndex(TYPE));
        UniParcDatabase database = UniParcDatabase.typeOf(databaseType);

        if (UniParcDatabase.TREMBL == database
                || UniParcDatabase.SWISSPROT == database
                || UniParcDatabase.SWISSPROT_VARSPLIC == database) {

            String active = rowValue.getString(rowValue.fieldIndex(ACTIVE));

            if ("Y".equals(active) && RowUtils.hasFieldName(VERSION, rowValue)) {
                long version = rowValue.getLong(rowValue.fieldIndex(VERSION));
                id = id + "." + version;
            }

            return id;
        }
        return null;
    }
}
