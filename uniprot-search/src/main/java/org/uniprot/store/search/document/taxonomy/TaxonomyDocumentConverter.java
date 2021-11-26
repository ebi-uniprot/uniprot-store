package org.uniprot.store.search.document.taxonomy;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.uniprot.core.taxonomy.*;
import org.uniprot.core.taxonomy.impl.TaxonomyEntryBuilder;
import org.uniprot.core.uniprotkb.taxonomy.Taxonomy;
import org.uniprot.store.search.document.DocumentConversionException;
import org.uniprot.store.search.document.DocumentConverter;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class TaxonomyDocumentConverter
        implements DocumentConverter<TaxonomyEntry, TaxonomyDocument> {

    private final ObjectMapper jsonMapper;

    public TaxonomyDocumentConverter(ObjectMapper objectMapper) {
        jsonMapper = objectMapper;
    }

    @Override
    public TaxonomyDocument convert(TaxonomyEntry entry) {
        if (entry.getTaxonId() == 1L) {
            return null;
        }
        TaxonomyDocument.TaxonomyDocumentBuilder documentBuilder = TaxonomyDocument.builder();
        documentBuilder.id(String.valueOf(entry.getTaxonId()));
        documentBuilder.taxId(entry.getTaxonId());
        if (entry.getParent().getTaxonId() == 1L) {
            entry = TaxonomyEntryBuilder.from(entry).parent(null).build();
        } else {
            documentBuilder.parent(entry.getParent().getTaxonId());
        }

        documentBuilder.scientific(entry.getScientificName());
        documentBuilder.common(entry.getCommonName());
        documentBuilder.mnemonic(entry.getMnemonic());
        documentBuilder.synonym(String.join(", ", entry.getSynonyms()));
        documentBuilder.rank(entry.getRank().getDisplayName());

        documentBuilder.active(entry.isActive());
        documentBuilder.hidden(entry.isHidden());
        documentBuilder.linked(!entry.getLinks().isEmpty());

        if (entry.hasStatistics()) {
            List<String> taxonomiesWith = new ArrayList<>();
            TaxonomyStatistics statistics = entry.getStatistics();
            if (statistics.hasReviewedProteinCount()) {
                taxonomiesWith.add("1_uniprotkb");
                taxonomiesWith.add("2_reviewed");
            }
            if (statistics.hasUnreviewedProteinCount() && !statistics.hasReviewedProteinCount()) {
                taxonomiesWith.add("1_uniprotkb");
                taxonomiesWith.add("3_unreviewed");
            }
            if (statistics.hasReferenceProteomeCount()) {
                taxonomiesWith.add("4_reference");
                taxonomiesWith.add("5_proteome");
            }
            if (statistics.hasProteomeCount() && !statistics.hasReferenceProteomeCount()) {
                taxonomiesWith.add("5_proteome");
            }
            documentBuilder.taxonomiesWith(taxonomiesWith);
        }

        String superKingdom =
                entry.getLineages().stream()
                        .filter(lineage -> TaxonomyRank.SUPERKINGDOM == lineage.getRank())
                        .map(TaxonomyLineage::getScientificName)
                        .findFirst()
                        .orElse(null);
        documentBuilder.superkingdom(superKingdom);

        documentBuilder.host(
                entry.getHosts().stream().map(Taxonomy::getTaxonId).collect(Collectors.toList()));
        documentBuilder.ancestor(
                entry.getLineages().stream()
                        .map(TaxonomyLineage::getTaxonId)
                        .collect(Collectors.toList()));
        documentBuilder.strain(buildStrainList(entry.getStrains()));
        if (entry.hasOtherNames()) {
            documentBuilder.otherNames(entry.getOtherNames());
        }
        documentBuilder.taxonomyObj(getTaxonomyBinary(entry));

        return documentBuilder.build();
    }

    private List<String> buildStrainList(List<TaxonomyStrain> strainList) {
        return strainList.stream().map(this::mapStrain).collect(Collectors.toList());
    }

    private String mapStrain(TaxonomyStrain strain) {
        String result = strain.getName();
        if (strain.hasSynonyms()) {
            result += " ; " + String.join(" , ", strain.getSynonyms());
        }
        return result;
    }

    private byte[] getTaxonomyBinary(TaxonomyEntry entry) {
        try {
            return jsonMapper.writeValueAsBytes(entry);
        } catch (JsonProcessingException e) {
            throw new DocumentConversionException(
                    "Unable to parse TaxonomyEntry to binary json: ", e);
        }
    }
}
