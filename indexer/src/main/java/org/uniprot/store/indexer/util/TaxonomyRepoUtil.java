package org.uniprot.store.indexer.util;

import org.uniprot.cv.taxonomy.TaxonomicNode;
import org.uniprot.cv.taxonomy.TaxonomyRepo;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * @author jluo
 * @date: 14 Aug 2019
 */
public class TaxonomyRepoUtil {
    public static List<TaxonomicNode> getTaxonomyLineage(TaxonomyRepo repo, int taxId) {
        List<TaxonomicNode> nodes = new ArrayList<>();
        Optional<TaxonomicNode> taxonomicNode = repo.retrieveNodeUsingTaxID(taxId);
        while (taxonomicNode.isPresent()) {
            nodes.add(taxonomicNode.get());
            taxonomicNode = getParentTaxon(repo, taxonomicNode.get().id());
        }
        return nodes;
    }

    private static Optional<TaxonomicNode> getParentTaxon(TaxonomyRepo repo, int taxId) {
        Optional<TaxonomicNode> optionalNode = repo.retrieveNodeUsingTaxID(taxId);
        return optionalNode.filter(TaxonomicNode::hasParent).map(TaxonomicNode::parent);
    }

    public static List<String> extractTaxonFromNode(TaxonomicNode node) {
        List<String> taxonmyItems = new ArrayList<>();
        if (node.scientificName() != null && !node.scientificName().isEmpty()) {
            taxonmyItems.add(node.scientificName());
        }
        if (node.commonName() != null && !node.commonName().isEmpty()) {
            taxonmyItems.add(node.commonName());
        }
        if (node.synonymName() != null && !node.synonymName().isEmpty()) {
            taxonmyItems.add(node.synonymName());
        }
        if (node.mnemonic() != null && !node.mnemonic().isEmpty()) {
            taxonmyItems.add(node.mnemonic());
        }
        return taxonmyItems;
    }

    public static List<String> extractTaxonFromNodeNoMnemonic(TaxonomicNode node) {
        List<String> taxonmyItems = new ArrayList<>();
        if (node.scientificName() != null && !node.scientificName().isEmpty()) {
            taxonmyItems.add(node.scientificName());
        }
        if (node.commonName() != null && !node.commonName().isEmpty()) {
            taxonmyItems.add(node.commonName());
        }
        if (node.synonymName() != null && !node.synonymName().isEmpty()) {
            taxonmyItems.add(node.synonymName());
        }
        return taxonmyItems;
    }
}
