package org.uniprot.store.indexer.proteome;

import static org.uniprot.core.util.Utils.notNull;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import lombok.extern.slf4j.Slf4j;

import org.uniprot.core.proteome.ProteomeEntry;
import org.uniprot.core.proteome.Superkingdom;
import org.uniprot.core.proteome.impl.ProteomeEntryBuilder;
import org.uniprot.core.taxonomy.TaxonomyLineage;
import org.uniprot.core.taxonomy.impl.TaxonomyLineageBuilder;
import org.uniprot.core.uniprotkb.taxonomy.Taxonomy;
import org.uniprot.core.uniprotkb.taxonomy.impl.TaxonomyBuilder;
import org.uniprot.core.xml.jaxb.proteome.ProteomeType;
import org.uniprot.core.xml.proteome.ProteomeConverter;
import org.uniprot.cv.taxonomy.TaxonomicNode;
import org.uniprot.cv.taxonomy.TaxonomyRepo;
import org.uniprot.store.indexer.util.TaxonomyRepoUtil;
import org.uniprot.store.job.common.StoringException;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;

/**
 * @author lgonzales
 * @since 19/11/2020
 */
@Slf4j
public class ProteomeEntryAdapter {

    private final TaxonomyRepo taxonomyRepo;
    private final String geneCentricDir;
    private final String geneCentricFileSuffix;
    private final ProteomeConverter proteomeConverter;

    public ProteomeEntryAdapter(
            TaxonomyRepo taxonomyRepo, String geneCentricDir, String geneCentricFileSuffix) {
        this.taxonomyRepo = taxonomyRepo;
        this.geneCentricDir = geneCentricDir;
        this.geneCentricFileSuffix = geneCentricFileSuffix;
        this.proteomeConverter = new ProteomeConverter();
    }

    public ProteomeEntry adaptEntry(ProteomeType source) {
        ProteomeEntry proteome = this.proteomeConverter.fromXml(source);
        ProteomeEntryBuilder builder = ProteomeEntryBuilder.from(proteome);
        if (notNull(proteome.getTaxonomy())) {
            int taxonId = (int) proteome.getTaxonomy().getTaxonId();
            Optional<TaxonomicNode> taxonomicNode = taxonomyRepo.retrieveNodeUsingTaxID(taxonId);
            if (taxonomicNode.isPresent()) {
                builder.taxonomy(getTaxonomy(taxonomicNode.get(), taxonId));
                List<TaxonomyLineage> lineageList = getLineage(taxonomicNode.get().id());
                builder.taxonLineagesSet(lineageList);

                // add superKingdom from lineage
                lineageList.stream()
                        .map(TaxonomyLineage::getScientificName)
                        .filter(Superkingdom::isSuperkingdom) // to avoid exception in typeOf
                        .map(Superkingdom::typeOf)
                        .findFirst()
                        .ifPresent(builder::superkingdom);
            }
            builder.geneCount(getGeneCount(source.getUpid(), taxonId));
        }
        return builder.build();
    }

    private Integer getGeneCount(String upid, int taxonId) {
        String filePath = geneCentricDir + upid + "_" + taxonId + geneCentricFileSuffix;
        int geneCount = 0;

        try (BufferedReader br = new BufferedReader(new FileReader(filePath))) {
            String thisLine;
            while ((thisLine = br.readLine()) != null) {
                if (thisLine.startsWith(">")) {
                    geneCount++;
                }
            }
        } catch (IOException e) {
            throw new StoringException(
                    "Unable to load genecentric fasta file, please check genecentric file dir config: "
                            + filePath);
        }

        return geneCount;
    }

    private Taxonomy getTaxonomy(TaxonomicNode node, long taxId) {

        TaxonomyBuilder builder = new TaxonomyBuilder();
        builder.taxonId(taxId).scientificName(node.scientificName());
        if (!Strings.isNullOrEmpty(node.commonName())) builder.commonName(node.commonName());
        if (!Strings.isNullOrEmpty(node.mnemonic())) builder.mnemonic(node.mnemonic());
        if (!Strings.isNullOrEmpty(node.synonymName())) {
            builder.synonymsAdd(node.synonymName());
        }
        return builder.build();
    }

    private List<TaxonomyLineage> getLineage(int taxId) {
        List<TaxonomicNode> nodes = TaxonomyRepoUtil.getTaxonomyLineage(taxonomyRepo, taxId);
        List<TaxonomyLineage> lineage =
                nodes.stream()
                        .skip(1)
                        .map(
                                node ->
                                        new TaxonomyLineageBuilder()
                                                .taxonId(node.id())
                                                .scientificName(node.scientificName())
                                                .build())
                        .collect(Collectors.toList());
        return Lists.reverse(lineage);
    }
}
