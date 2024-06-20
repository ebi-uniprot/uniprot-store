package org.uniprot.store.spark.indexer.uniparc.converter;

import static org.uniprot.store.spark.indexer.common.util.RowUtils.hasFieldName;
import static org.uniprot.store.spark.indexer.uniparc.converter.DatasetUniParcEntryConverter.*;

import java.io.Serial;
import java.io.Serializable;
import java.util.List;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Row;
import org.uniprot.core.Location;
import org.uniprot.core.uniparc.*;
import org.uniprot.core.uniparc.impl.InterProGroupBuilder;
import org.uniprot.core.uniparc.impl.SequenceFeatureBuilder;
import org.uniprot.core.uniparc.impl.UniParcEntryLightBuilder;
import org.uniprot.store.spark.indexer.common.util.RowUtils;

public class DatasetUniParcEntryLightConverter
        implements MapFunction<Row, UniParcEntryLight>, Serializable {
    @Serial private static final long serialVersionUID = 2705637837095269737L;

    @Override
    public UniParcEntryLight call(Row rowValue) throws Exception {
        UniParcEntryLightBuilder builder = new UniParcEntryLightBuilder();
        if (hasFieldName(UNIPROTKB_EXCLUSION, rowValue)) {
            builder.uniProtExclusionReason(
                    rowValue.getString(rowValue.fieldIndex(UNIPROTKB_EXCLUSION)));
        }
        String uniParcId = rowValue.getString(rowValue.fieldIndex(ACCESSION));
        builder.uniParcId(uniParcId);
        if (hasFieldName(DB_REFERENCE, rowValue)) {
            List<Row> dbReferences = rowValue.getList(rowValue.fieldIndex(DB_REFERENCE));
            for (Row dbReference : dbReferences) {
                String uniProtKBAccession = getUniProtKBAccession(dbReference);
                builder.uniProtKBAccessionsAdd(uniProtKBAccession);
                String uniParcXrefId = getUniParcXRefId(uniParcId, dbReference);
                builder.uniParcCrossReferencesAdd(uniParcXrefId);
            }
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

        // TODO add code to fill commonTaxon
        // builder.commonTaxonsSet();
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

    private String getUniParcXRefId(String uniParcId, Row rowValue) {
        String id = rowValue.getString(rowValue.fieldIndex(ID));
        String databaseType = rowValue.getString(rowValue.fieldIndex(TYPE));
        String versionI = String.valueOf(rowValue.getLong(rowValue.fieldIndex((VERSION_I))));
        StringBuilder xrefIdBuilder = new StringBuilder(uniParcId);
        xrefIdBuilder.append("-");
        xrefIdBuilder.append(UniParcDatabase.typeOf(databaseType).name());
        xrefIdBuilder.append("-");
        xrefIdBuilder.append(id);
        xrefIdBuilder.append("-");
        xrefIdBuilder.append(versionI);
        return xrefIdBuilder.toString();
    }
}
