package org.uniprot.store.search.document.genecentric;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.uniprot.core.genecentric.GeneCentricEntry;
import org.uniprot.core.genecentric.Protein;
import org.uniprot.core.uniprotkb.UniProtKBEntryType;
import org.uniprot.core.util.Utils;
import org.uniprot.store.search.document.DocumentConversionException;
import org.uniprot.store.search.document.DocumentConverter;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * @author lgonzales
 * @since 10/11/2020
 */
public class GeneCentricDocumentConverter
        implements DocumentConverter<GeneCentricEntry, GeneCentricDocument> {

    private final ObjectMapper objectMapper;

    public GeneCentricDocumentConverter(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    @Override
    public GeneCentricDocument convert(GeneCentricEntry geneCentricEntry) {
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

    public byte[] getStoredGeneCentricEntry(GeneCentricEntry geneCentricEntry) {
        byte[] binaryEntry;
        try {
            binaryEntry = objectMapper.writeValueAsBytes(geneCentricEntry);
        } catch (JsonProcessingException e) {
            throw new DocumentConversionException(
                    "Unable to parse GeneCentricEntry to binary json: ", e);
        }
        return binaryEntry;
    }

    public GeneCentricEntry getCanonicalEntryFromDocument(GeneCentricDocument geneCentricDocument) {
        try {
            return objectMapper.readValue(
                    geneCentricDocument.getGeneCentricStored(), GeneCentricEntry.class);
        } catch (IOException e) {
            throw new DocumentConversionException(
                    "Unable to parse gene centric entry from binary json: ", e);
        }
    }
}
