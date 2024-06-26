package org.uniprot.store.indexer.converters;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.uniprot.core.uniref.UniRefEntry;
import org.uniprot.core.uniref.UniRefMember;
import org.uniprot.core.uniref.UniRefMemberIdType;
import org.uniprot.core.util.Utils;
import org.uniprot.core.xml.jaxb.uniref.Entry;
import org.uniprot.core.xml.uniref.UniRefEntryConverter;
import org.uniprot.cv.taxonomy.TaxonomicNode;
import org.uniprot.cv.taxonomy.TaxonomyRepo;
import org.uniprot.store.indexer.util.DateUtils;
import org.uniprot.store.indexer.util.TaxonomyRepoUtil;
import org.uniprot.store.search.document.DocumentConverter;
import org.uniprot.store.search.document.uniref.UniRefDocument;
import org.uniprot.store.search.document.uniref.UniRefDocument.UniRefDocumentBuilder;

/**
 * @author jluo
 * @date: 14 Aug 2019
 */
public class UniRefDocumentConverter implements DocumentConverter<Entry, UniRefDocument> {

    private final UniRefEntryConverter converter;
    private final TaxonomyRepo taxonomyRepo;

    public UniRefDocumentConverter(TaxonomyRepo taxonomyRepo) {
        this.converter = new UniRefEntryConverter();
        this.taxonomyRepo = taxonomyRepo;
    }

    @Override
    public UniRefDocument convert(Entry source) {
        UniRefEntry entry = converter.fromXml(source);
        UniRefDocumentBuilder builder = UniRefDocument.builder();
        builder.id(entry.getId().getValue())
                .identity(entry.getEntryType().getIdentity())
                .name(entry.getName())
                .count(entry.getMembers().size() + 1)
                .length(entry.getRepresentativeMember().getSequence().getLength())
                .updated(DateUtils.convertLocalDateToUTCDate(entry.getUpdated()))
                .updatedSort(DateUtils.convertLocalDateToUTCDate(entry.getUpdated()))
                .uniprotIds(getUniProtIds(entry))
                .upids(getUniParcIds(entry))
                .clusters(getClusterIds(entry))
                .organismSort(getOrganismNameForSort(entry));
        processTaxonomy(
                entry.getRepresentativeMember().getOrganismName(),
                entry.getRepresentativeMember().getOrganismTaxId(),
                builder);
        return builder.build();
    }

    private String getOrganismNameForSort(UniRefEntry entry) {
        List<String> result = new ArrayList<>();
        result.add(entry.getRepresentativeMember().getOrganismName());
        entry.getMembers().stream()
                .map(UniRefMember::getOrganismName)
                .distinct()
                .limit(5)
                .forEach(result::add);
        return String.join(" ", result);
    }

    private List<String> getUniParcIds(UniRefEntry entry) {
        List<String> result = new ArrayList<>();
        result.addAll(getUniParcIds(entry.getRepresentativeMember()));
        entry.getMembers().forEach(val -> result.addAll(getUniParcIds(val)));

        return result;
    }

    private List<String> getUniParcIds(UniRefMember member) {
        List<String> result = new ArrayList<>();
        if (member.getMemberIdType() == UniRefMemberIdType.UNIPARC) {
            result.add(member.getMemberId());
        }
        if (Utils.notNull(member.getUniParcId())) {
            result.add(member.getUniParcId().getValue());
        }
        return result;
    }

    private List<String> getUniProtIds(UniRefEntry entry) {
        List<String> result = new ArrayList<>();
        result.addAll(getUniProtIds(entry.getRepresentativeMember()));
        entry.getMembers().forEach(val -> result.addAll(getUniProtIds(val)));

        return result;
    }

    private List<String> getUniProtIds(UniRefMember member) {
        List<String> result = new ArrayList<>();
        if (member.getMemberIdType() != UniRefMemberIdType.UNIPARC) {
            result.add(member.getMemberId());
        }
        member.getUniProtAccessions().forEach(val -> result.add(val.getValue()));

        return result;
    }

    private Set<String> getClusterIds(UniRefEntry entry) {
        Set<String> result = getClusterIds(entry.getRepresentativeMember());
        entry.getMembers().forEach(val -> result.addAll(getClusterIds(val)));
        return result;
    }

    private Set<String> getClusterIds(UniRefMember member) {
        Set<String> result = new HashSet<>();

        if (Utils.notNull(member.getUniRef50Id())) {
            result.add(member.getUniRef50Id().getValue());
        }
        if (Utils.notNull(member.getUniRef90Id())) {
            result.add(member.getUniRef90Id().getValue());
        }
        if (Utils.notNull(member.getUniRef100Id())) {
            result.add(member.getUniRef100Id().getValue());
        }
        return result;
    }

    private void processTaxonomy(String organismName, long taxId, UniRefDocumentBuilder builder) {

        builder.taxLineageId((int) taxId);
        builder.organismTaxon(organismName);
        List<TaxonomicNode> nodes = TaxonomyRepoUtil.getTaxonomyLineage(taxonomyRepo, (int) taxId);
        nodes.forEach(
                node -> {
                    builder.taxLineageId(node.id());
                    List<String> names = TaxonomyRepoUtil.extractTaxonFromNode(node);
                    names.forEach(builder::organismTaxon);
                });
    }
}
