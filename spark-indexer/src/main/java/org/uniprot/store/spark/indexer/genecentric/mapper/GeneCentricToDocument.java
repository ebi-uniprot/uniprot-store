package org.uniprot.store.spark.indexer.genecentric.mapper;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.api.java.function.Function;
import org.uniprot.core.genecentric.GeneCentricEntry;
import org.uniprot.core.genecentric.Protein;
import org.uniprot.core.json.parser.genecentric.GeneCentricJsonConfig;
import org.uniprot.core.uniprotkb.UniProtKBEntryType;
import org.uniprot.core.util.Utils;
import org.uniprot.store.search.document.proteome.GeneCentricDocument;
import org.uniprot.store.spark.indexer.common.exception.IndexHDFSDocumentsException;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * @author lgonzales
 * @since 22/10/2020
 */
public class GeneCentricToDocument implements Function<GeneCentricEntry, GeneCentricDocument> {
    private static final long serialVersionUID = -3437292527678058919L;

    private final ObjectMapper objectMapper;

    public GeneCentricToDocument() {
        objectMapper = GeneCentricJsonConfig.getInstance().getFullObjectMapper();
    }

    @Override
    public GeneCentricDocument call(GeneCentricEntry geneCentricEntry) throws Exception {
        Protein canonicalProtein = geneCentricEntry.getCanonicalProtein();
        List<String> accessions = new ArrayList<>();
        accessions.add(canonicalProtein.getId());
        List<String> geneNames = new ArrayList<>();
        geneNames.add(canonicalProtein.getGeneName());

        if (Utils.notNullNotEmpty(geneCentricEntry.getRelatedProteins())) {
            geneCentricEntry
                    .getRelatedProteins()
                    .forEach(
                            relatedProtein -> {
                                accessions.add(relatedProtein.getId());
                                geneNames.add(relatedProtein.getGeneName());
                            });
        }

        long taxonomyId = 0L;
        if (Utils.notNull(canonicalProtein.getOrganism())) {
            taxonomyId = canonicalProtein.getOrganism().getTaxonId();
        }

        return GeneCentricDocument.builder()
                .accession(canonicalProtein.getId())
                .reviewed(canonicalProtein.getEntryType() == UniProtKBEntryType.SWISSPROT)
                .organismTaxId((int) taxonomyId)
                .accessions(accessions)
                .upid(geneCentricEntry.getProteomeId())
                .geneNames(geneNames)
                .geneCentricStored(getStoredGeneCentricEntry(geneCentricEntry))
                .build();
    }

    private byte[] getStoredGeneCentricEntry(GeneCentricEntry geneCentricEntry) {
        byte[] binaryEntry;
        try {
            binaryEntry = objectMapper.writeValueAsBytes(geneCentricEntry);
        } catch (JsonProcessingException e) {
            throw new IndexHDFSDocumentsException(
                    "Unable to parse GeneCentricEntry to binary json: ", e);
        }
        return binaryEntry;
    }
}
