package org.uniprot.store.search.document.uniparc;

import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.uniprot.core.uniparc.SequenceFeature;
import org.uniprot.core.uniparc.UniParcCrossReference;
import org.uniprot.core.uniparc.UniParcDatabase;
import org.uniprot.core.uniparc.UniParcEntry;
import org.uniprot.core.util.Pair;
import org.uniprot.core.util.Utils;
import org.uniprot.store.config.uniparc.UniParcConfigUtil;
import org.uniprot.store.search.document.DocumentConverter;

/**
 * This class convert an UniParcEntry to UniParcDocument
 *
 * @author lgonzales
 * @since 2020-02-15
 */
public class UniParcDocumentConverter implements DocumentConverter<UniParcEntry, UniParcDocument> {

    @Override
    public UniParcDocument convert(UniParcEntry uniParcEntry) {
        UniParcDocument.UniParcDocumentBuilder builder = UniParcDocument.builder();
        builder.upi(uniParcEntry.getUniParcId().getValue())
                .seqLength(uniParcEntry.getSequence().getLength())
                .sequenceChecksum(uniParcEntry.getSequence().getCrc64())
                .sequenceChecksum(uniParcEntry.getSequence().getMd5());
        uniParcEntry.getUniParcCrossReferences().forEach(val -> processDbReference(val, builder));
        uniParcEntry.getSequenceFeatures().forEach(val -> processSequenceFeature(val, builder));
        return builder.build();
    }

    private void processDbReference(
            UniParcCrossReference xref, UniParcDocument.UniParcDocumentBuilder builder) {
        UniParcDatabase type = xref.getDatabase();

        String dbId = xref.getId();
        if (Objects.nonNull(xref.getVersion()) && xref.getVersion() > 0) {
            dbId += "." + xref.getVersion();
        }
        builder.dbId(dbId);

        Map.Entry<String, String> dbTypeData = UniParcConfigUtil.getDBNameValue(type);
        if (xref.isActive()) {
            builder.active(dbTypeData.getValue());
        }
        builder.database(dbTypeData.getValue());
        builder.notDuplicatedDatabasesFacet(type.getIndex());
        if (xref.isActive()
                && (type == UniParcDatabase.SWISSPROT || type == UniParcDatabase.TREMBL)) {
            builder.uniprotAccession(xref.getId());
            builder.uniprotIsoform(xref.getId());
        }

        if (xref.isActive() && type == UniParcDatabase.SWISSPROT_VARSPLIC) {
            builder.uniprotIsoform(xref.getId());
        }

        if (Utils.notNullNotEmpty(xref.getProteomeIdComponentPairs())) {
            List<Pair<String, String>> proteomeIdComponentPairs =
                    xref.getProteomeIdComponentPairs();
            for (Pair<String, String> proteomeIdComponentPair : proteomeIdComponentPairs) {
                builder.proteome(proteomeIdComponentPair.getKey());
                builder.proteomeComponent(
                        proteomeIdComponentPair.getKey()
                                + ":"
                                + proteomeIdComponentPair.getValue());
            }
        }

        if (Utils.notNullNotEmpty(xref.getProteinName())) {
            builder.proteinName(xref.getProteinName());
        }

        if (Utils.notNullNotEmpty(xref.getGeneName())) {
            builder.geneName(xref.getGeneName());
        }

        if (Utils.notNull(xref.getOrganism())) {
            builder.taxLineageId((int) xref.getOrganism().getTaxonId());
        }
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
