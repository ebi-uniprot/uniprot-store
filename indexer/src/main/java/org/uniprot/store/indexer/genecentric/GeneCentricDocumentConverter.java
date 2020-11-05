package org.uniprot.store.indexer.genecentric;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.uniprot.core.genecentric.GeneCentricEntry;
import org.uniprot.core.genecentric.Protein;
import org.uniprot.core.uniprotkb.UniProtKBEntryType;
import org.uniprot.store.job.common.DocumentConversionException;
import org.uniprot.store.job.common.converter.DocumentConverter;
import org.uniprot.store.search.document.proteome.GeneCentricDocument;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * @author lgonzales
 * @since 04/11/2020
 */
public class GeneCentricDocumentConverter
        implements DocumentConverter<GeneCentricEntry, GeneCentricDocument> {

    private final ObjectMapper objectMapper;

    public GeneCentricDocumentConverter(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    @Override
    public GeneCentricDocument convert(GeneCentricEntry geneCentricEntry) {
        GeneCentricDocument.GeneCentricDocumentBuilder builder = GeneCentricDocument.builder();
        Protein canonical = geneCentricEntry.getCanonicalProtein();

        List<String> accessions = new ArrayList<>();
        accessions.add(canonical.getId());
        geneCentricEntry.getRelatedProteins().stream().map(Protein::getId).forEach(accessions::add);

        List<String> genes = new ArrayList<>();
        genes.add(canonical.getGeneName());
        geneCentricEntry.getRelatedProteins().stream()
                .map(Protein::getGeneName)
                .forEach(genes::add);

        long taxonId = canonical.getOrganism().getTaxonId();

        builder.accession(canonical.getId())
                .accessions(accessions)
                .geneNames(genes)
                .reviewed(canonical.getEntryType() == UniProtKBEntryType.SWISSPROT)
                .upid(geneCentricEntry.getProteomeId())
                .organismTaxId((int) taxonId);

        byte[] binaryEntry = getGeneCentricStored(geneCentricEntry);
        builder.geneCentricStored(binaryEntry);

        return builder.build();
    }

    public GeneCentricEntry getCanonicalEntryFromDocument(
            GeneCentricDocument geneCentricDocument) {
        try {
            return objectMapper.readValue(
                    geneCentricDocument.getGeneCentricStored(), GeneCentricEntry.class);
        } catch (IOException e) {
            throw new DocumentConversionException(
                    "Unable to parse gene centric entry from binary json: ", e);
        }
    }

    private byte[] getGeneCentricStored(GeneCentricEntry geneCentricEntry) {
        byte[] binaryEntry;
        try {
            binaryEntry = objectMapper.writeValueAsBytes(geneCentricEntry);
        } catch (JsonProcessingException e) {
            throw new DocumentConversionException(
                    "Unable to parse gene centric entry to binary json: ", e);
        }
        return binaryEntry;
    }

}
