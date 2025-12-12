package org.uniprot.store.spark.indexer.uniparc.converter;

import java.io.Serial;
import java.util.List;
import java.util.Map;

import org.apache.spark.sql.Row;
import org.uniprot.core.uniparc.UniParcCrossReference;
import org.uniprot.core.uniparc.UniParcDatabase;
import org.uniprot.core.uniparc.UniParcEntry;
import org.uniprot.core.uniparc.impl.ProteomeIdComponentBuilder;
import org.uniprot.core.uniparc.impl.UniParcCrossReferenceBuilder;
import org.uniprot.core.uniparc.impl.UniParcEntryBuilder;
import org.uniprot.core.uniprotkb.taxonomy.Organism;
import org.uniprot.core.uniprotkb.taxonomy.impl.OrganismBuilder;
import org.uniprot.store.spark.indexer.common.util.RowUtils;

/**
 * This class convert XML Row result to a UniParcEntry
 *
 * @author lgonzales
 * @since 2020-02-13
 */
public class DatasetUniParcEntryConverter extends BaseUniParcEntryConverter<UniParcEntry> {

    @Serial private static final long serialVersionUID = 1817073609115796687L;
    public static final String PROTEOME_COMPONENT_SEPARATOR = ":";

    @Override
    public UniParcEntry call(Row rowValue) throws Exception {
        UniParcEntryBuilder builder = new UniParcEntryBuilder();
        if (RowUtils.hasFieldName(UNIPROTKB_EXCLUSION, rowValue)) {
            builder.uniprotExclusionReason(
                    rowValue.getString(rowValue.fieldIndex(UNIPROTKB_EXCLUSION)));
        }
        if (RowUtils.hasFieldName(ACCESSION, rowValue)) {
            builder.uniParcId(rowValue.getString(rowValue.fieldIndex(ACCESSION)));
        }
        if (RowUtils.hasFieldName(DB_REFERENCE, rowValue)) {
            List<Row> dbReferences = rowValue.getList(rowValue.fieldIndex(DB_REFERENCE));
            for (Row dbReference : dbReferences) {
                UniParcCrossReference xref = convertDbReference(dbReference);
                builder.uniParcCrossReferencesAdd(xref);
            }
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

    private UniParcCrossReference convertDbReference(Row rowValue) {
        UniParcCrossReferenceBuilder builder = new UniParcCrossReferenceBuilder();

        if (RowUtils.hasFieldName(ID, rowValue)) {
            builder.id(rowValue.getString(rowValue.fieldIndex(ID)));
        }
        if (RowUtils.hasFieldName(TYPE, rowValue)) {
            String databaseType = rowValue.getString(rowValue.fieldIndex(TYPE));
            builder.database(UniParcDatabase.typeOf(databaseType));
        }
        if (RowUtils.hasFieldName(VERSION_I, rowValue)) {
            builder.versionI((int) rowValue.getLong(rowValue.fieldIndex(VERSION_I)));
        }
        if (RowUtils.hasFieldName(ACTIVE, rowValue)) {
            String active = rowValue.getString(rowValue.fieldIndex(ACTIVE));
            builder.active(active.equalsIgnoreCase("Y"));
        }
        if (RowUtils.hasFieldName(VERSION, rowValue)) {
            builder.version((int) rowValue.getLong(rowValue.fieldIndex(VERSION)));
        }

        builder.created(parseDate(rowValue, CREATED));
        builder.lastUpdated(parseDate(rowValue, LAST));

        if (RowUtils.hasFieldName(PROPERTY, rowValue)) {
            convertProperties(rowValue, builder);
        }

        return builder.build();
    }

    private void convertProperties(Row rowValue, UniParcCrossReferenceBuilder builder) {
        Map<String, List<String>> propertyMap = RowUtils.convertProperties(rowValue);
        if (propertyMap.containsKey(PROPERTY_GENE_NAME)) {
            builder.geneName(propertyMap.get(PROPERTY_GENE_NAME).get(0));
            propertyMap.remove(PROPERTY_GENE_NAME);
        }
        if (propertyMap.containsKey(PROPERTY_PROTEIN_NAME)) {
            builder.proteinName(propertyMap.get(PROPERTY_PROTEIN_NAME).get(0));
            propertyMap.remove(PROPERTY_PROTEIN_NAME);
        }
        if (propertyMap.containsKey(PROPERTY_CHAIN)) {
            builder.chain(propertyMap.get(PROPERTY_CHAIN).get(0));
            propertyMap.remove(PROPERTY_CHAIN);
        }
        if (propertyMap.containsKey(PROPERTY_NCBI_GI)) {
            builder.ncbiGi(propertyMap.get(PROPERTY_NCBI_GI).get(0));
            propertyMap.remove(PROPERTY_NCBI_GI);
        }
        if (propertyMap.containsKey(PROPERTY_PROTEOMEID_COMPONENT)) {
            List<String> proteomeComponents = propertyMap.get(PROPERTY_PROTEOMEID_COMPONENT);
            for (String proteomeComponent : proteomeComponents) {
                String[] split = proteomeComponent.split(PROTEOME_COMPONENT_SEPARATOR);
                builder.proteomeIdComponentsAdd(
                        new ProteomeIdComponentBuilder()
                                .proteomeId(split[0])
                                .component(split[1])
                                .build());
            }
            propertyMap.remove(PROPERTY_PROTEOMEID_COMPONENT);
        }
        if (propertyMap.containsKey(PROPERTY_NCBI_TAXONOMY_ID)) {
            String taxonId = propertyMap.get(PROPERTY_NCBI_TAXONOMY_ID).get(0);
            Organism organism = new OrganismBuilder().taxonId(Long.parseLong(taxonId)).build();
            builder.organism(organism);
            propertyMap.remove(PROPERTY_NCBI_TAXONOMY_ID);
        }
        if (propertyMap.containsKey(PROPERTY_UNIPROTKB_ACCESSION)) {
            String accession = propertyMap.get(PROPERTY_UNIPROTKB_ACCESSION).get(0);
            builder.propertiesAdd(PROPERTY_UNIPROTKB_ACCESSION, accession);
            propertyMap.remove(PROPERTY_UNIPROTKB_ACCESSION);
        }

        if (!propertyMap.isEmpty()) {
            StringBuilder message = new StringBuilder();
            message.append("Unable to parse UniParc property:");
            propertyMap.forEach(
                    (key, value) -> message.append(key).append(":").append(value.get(0)));
            throw new IllegalArgumentException(message.toString());
        }
    }
}
