package org.uniprot.store.search.document.taxonomy;

import java.util.List;
import java.util.stream.Collectors;

import org.uniprot.core.taxonomy.*;
import org.uniprot.core.taxonomy.impl.TaxonomyEntryBuilder;
import org.uniprot.core.uniprotkb.taxonomy.Taxonomy;
import org.uniprot.store.search.document.DocumentConversionException;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class TaxonomyDocumentConverter {

    private final ObjectMapper jsonMapper;

    public TaxonomyDocumentConverter(ObjectMapper objectMapper) {
        jsonMapper = objectMapper;
    }

    public TaxonomyDocument.TaxonomyDocumentBuilder convert(TaxonomyEntry entry) {
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

        return documentBuilder;
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
