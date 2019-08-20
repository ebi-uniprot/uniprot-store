package org.uniprot.store.indexer.uniref;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.uniprot.core.cv.taxonomy.TaxonomicNode;
import org.uniprot.core.cv.taxonomy.TaxonomyRepo;
import org.uniprot.core.uniref.UniRefEntry;
import org.uniprot.core.uniref.UniRefMember;
import org.uniprot.core.uniref.UniRefMemberIdType;
import org.uniprot.core.xml.jaxb.uniref.Entry;
import org.uniprot.core.xml.uniref.UniRefEntryConverter;
import org.uniprot.store.indexer.converter.DocumentConverter;
import org.uniprot.store.indexer.util.DateUtils;
import org.uniprot.store.indexer.util.TaxonomyRepoUtil;
import org.uniprot.store.search.document.uniref.UniRefDocument;
import org.uniprot.store.search.document.uniref.UniRefDocument.UniRefDocumentBuilder;

import lombok.val;

/**
 *
 * @author jluo
 * @date: 14 Aug 2019
 *
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
		UniRefEntry entry  = converter.fromXml(source);
		UniRefDocumentBuilder builder   = UniRefDocument.builder();
		builder.id(entry.getId().getValue())
		.identity(entry.getEntryType().getIdentity())
		.name(entry.getName())
		.count(entry.getMembers().size()+1)
		.length(entry.getRepresentativeMember().getSequence().getLength())
		.created(DateUtils.convertLocalDateToDate(entry.getUpdated()))
		.uniprotIds(getUniProtIds(entry))
		.upis(getUniParcIds(entry))	
		.organismSort(getOrganismNameForSort(entry))
		;
		processTaxonomy(entry.getRepresentativeMember().getOrganismName(), entry.getRepresentativeMember().getOrganismTaxId(), builder);
		return builder.build();
	}
	
	private String getOrganismNameForSort(UniRefEntry entry){
		List<String> result = new ArrayList<>();
		result.add(entry.getRepresentativeMember().getOrganismName());
		entry.getMembers().stream()
		.map(val -> val.getOrganismName())
		.distinct().limit(5)
		.forEach(val -> result.add(val));
		return result.stream().collect(Collectors.joining(" "));
	}
	private List<String> getUniParcIds(UniRefEntry entry){
		List<String> result = new ArrayList<>();
		result.addAll(getUniParcIds(entry.getRepresentativeMember()));
		entry.getMembers().forEach(val ->result.addAll(getUniParcIds(val)));
		
		return result;
	}
	
	private List<String> getUniParcIds(UniRefMember member){
		List<String> result = new ArrayList<>();
		if(member.getMemberIdType() ==UniRefMemberIdType.UNIPARC) {
			result.add(member.getMemberId());
		}
		if(member.getUniParcId() !=null) {
			result.add(member.getUniParcId().getValue());
		}
		return result;
	}
	private List<String> getUniProtIds(UniRefEntry entry){
		List<String> result = new ArrayList<>();
		result.addAll(getUniProtIds(entry.getRepresentativeMember()));
		entry.getMembers().forEach(val ->result.addAll(getUniProtIds(val)));
		
		return result;
	}
	
	private List<String> getUniProtIds(UniRefMember member){
		List<String> result = new ArrayList<>();
		if(member.getMemberIdType() ==UniRefMemberIdType.UNIPROTKB) {
			result.add(member.getMemberId());
		}
		if(member.getUniProtAccession() !=null) {
			result.add(member.getUniProtAccession().getValue());
		}
		return result;
	}
	
	private void processTaxonomy(String organismName, long taxId, UniRefDocumentBuilder builder) {
		
		builder.taxLineageId((int) taxId);
		builder.organismTaxon(organismName);
		List<TaxonomicNode> nodes = TaxonomyRepoUtil.getTaxonomyLineage(taxonomyRepo, (int) taxId);
		nodes.forEach(node -> {
			builder.taxLineageId(node.id());
			List<String> names = TaxonomyRepoUtil.extractTaxonFromNode(node);
			names.forEach(val -> builder.organismTaxon(val));
		});
	}

}

