package org.uniprot.store.spark.indexer.uniparc.converter;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.uniprot.core.Property;
import org.uniprot.core.uniparc.SequenceFeature;
import org.uniprot.core.uniparc.UniParcCrossReference;
import org.uniprot.core.uniparc.UniParcDatabase;
import org.uniprot.core.uniparc.UniParcEntry;
import org.uniprot.core.uniprotkb.taxonomy.Taxonomy;
import org.uniprot.core.util.Utils;
import org.uniprot.store.config.uniparc.UniParcConfigUtil;
import org.uniprot.store.search.document.DocumentConverter;
import org.uniprot.store.search.document.uniparc.UniParcDocument;

/**
 * This class convert an UniParcEntry to UniParcDocument
 *
 * @author lgonzales
 * @since 2020-02-15
 */
public class UniParcDocumentConverter implements DocumentConverter<UniParcEntry, UniParcDocument> {

    @Override
    public UniParcDocument convert(UniParcEntry uniparcEntry) {
        UniParcDocument.UniParcDocumentBuilder builder = UniParcDocument.builder();
        builder.upi(uniparcEntry.getUniParcId().getValue())
                .seqLength(uniparcEntry.getSequence().getLength())
                .sequenceChecksum(uniparcEntry.getSequence().getCrc64())
                .sequenceMd5(uniparcEntry.getSequence().getMd5())
                .taxLineageIds(getTaxonomies(uniparcEntry));
        uniparcEntry.getUniParcCrossReferences().forEach(val -> processDbReference(val, builder));
        uniparcEntry.getSequenceFeatures().forEach(val -> processSequenceFeature(val, builder));
        return builder.build();
    }

    private List<Integer> getTaxonomies(UniParcEntry uniparcEntry) {
        return uniparcEntry.getTaxonomies().stream()
                .map(Taxonomy::getTaxonId)
                .map(Long::intValue)
                .collect(Collectors.toList());
    }

    private void processDbReference(
            UniParcCrossReference xref, UniParcDocument.UniParcDocumentBuilder builder) {
        UniParcDatabase type = xref.getDatabase();

        builder.dbId(xref.getId());

        Map.Entry<String, String> dbTypeData = UniParcConfigUtil.getDBNameValue(type);
        if (xref.isActive()) {
            builder.active(dbTypeData.getValue());
        }
        builder.database(dbTypeData.getValue());
        if (xref.isActive()
                && (type == UniParcDatabase.SWISSPROT || type == UniParcDatabase.TREMBL)) {
            builder.uniprotAccession(xref.getId());
            builder.uniprotIsoform(xref.getId());
        }

        if (xref.isActive() && type == UniParcDatabase.SWISSPROT_VARSPLIC) {
            builder.uniprotIsoform(xref.getId());
        }

        xref.getProperties().stream()
                .filter(val -> val.getKey().equals(UniParcCrossReference.PROPERTY_PROTEOME_ID))
                .map(Property::getValue)
                .forEach(builder::upid);

        xref.getProperties().stream()
                .filter(val -> val.getKey().equals(UniParcCrossReference.PROPERTY_PROTEIN_NAME))
                .map(Property::getValue)
                .forEach(builder::proteinName);

        xref.getProperties().stream()
                .filter(val -> val.getKey().equals(UniParcCrossReference.PROPERTY_GENE_NAME))
                .map(Property::getValue)
                .forEach(builder::geneName);
    }

    private void processSequenceFeature(
            SequenceFeature sequenceFeature, UniParcDocument.UniParcDocumentBuilder builder) {
        if (Utils.notNull(sequenceFeature.getInterProDomain())) {
            builder.featureId(sequenceFeature.getInterProDomain().getId());
        }
        if (Utils.notNull(sequenceFeature.getSignatureDbId())) {
            builder.featureId(sequenceFeature.getSignatureDbId());
        }
    }
}
