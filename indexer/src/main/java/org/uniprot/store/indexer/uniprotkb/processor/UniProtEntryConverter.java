package org.uniprot.store.indexer.uniprotkb.processor;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.base.Strings;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.uniprot.core.DBCrossReference;
import org.uniprot.core.Property;
import org.uniprot.core.Sequence;
import org.uniprot.core.Value;
import org.uniprot.core.citation.Citation;
import org.uniprot.core.citation.CitationXrefType;
import org.uniprot.core.citation.JournalArticle;
import org.uniprot.core.cv.chebi.Chebi;
import org.uniprot.core.cv.chebi.ChebiRepo;
import org.uniprot.core.cv.ec.ECRepo;
import org.uniprot.core.cv.keyword.KeywordCategory;
import org.uniprot.core.cv.pathway.UniPathway;
import org.uniprot.core.cv.taxonomy.TaxonomicNode;
import org.uniprot.core.cv.taxonomy.TaxonomyRepo;
import org.uniprot.core.flatfile.parser.impl.cc.CCLineBuilderFactory;
import org.uniprot.core.flatfile.parser.impl.ft.FeatureLineBuilderFactory;
import org.uniprot.core.flatfile.parser.impl.ra.RALineBuilder;
import org.uniprot.core.flatfile.parser.impl.rg.RGLineBuilder;
import org.uniprot.core.flatfile.writer.FFLineBuilder;
import org.uniprot.core.gene.Gene;
import org.uniprot.core.impl.SequenceImpl;
import org.uniprot.core.json.parser.uniprot.UniprotJsonConfig;
import org.uniprot.core.scorer.uniprotkb.UniProtEntryScored;
import org.uniprot.core.uniprot.*;
import org.uniprot.core.uniprot.builder.UniProtAccessionBuilder;
import org.uniprot.core.uniprot.builder.UniProtEntryBuilder;
import org.uniprot.core.uniprot.builder.UniProtIdBuilder;
import org.uniprot.core.uniprot.comment.*;
import org.uniprot.core.uniprot.description.*;
import org.uniprot.core.uniprot.evidence.Evidence;
import org.uniprot.core.uniprot.evidence.EvidenceTypeCategory;
import org.uniprot.core.uniprot.feature.Feature;
import org.uniprot.core.uniprot.feature.FeatureType;
import org.uniprot.core.uniprot.taxonomy.OrganismHost;
import org.uniprot.core.uniprot.xdb.UniProtDBCrossReference;
import org.uniprot.core.util.PublicationDateFormatter;
import org.uniprot.store.job.common.DocumentConversionException;
import org.uniprot.store.job.common.converter.DocumentConverter;
import org.uniprot.store.indexer.uniprot.go.GoRelationRepo;
import org.uniprot.store.indexer.uniprot.go.GoTerm;
import org.uniprot.store.indexer.uniprot.pathway.PathwayRepo;
import org.uniprot.store.indexer.util.DateUtils;
import org.uniprot.store.indexer.util.TaxonomyRepoUtil;
import org.uniprot.store.search.document.suggest.SuggestDictionary;
import org.uniprot.store.search.document.suggest.SuggestDocument;
import org.uniprot.store.search.document.uniprot.UniProtDocument;

import java.util.*;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static java.util.Arrays.asList;
import static org.uniprot.core.util.Utils.nonNull;
import static org.uniprot.core.util.Utils.nullOrEmpty;
import static org.uniprot.store.indexer.uniprot.go.GoRelationFileRepo.Relationship.IS_A;
import static org.uniprot.store.indexer.uniprot.go.GoRelationFileRepo.Relationship.PART_OF;

/**
 * Created 18/04/19
 *
 * @author Edd
 */
@Slf4j
public class UniProtEntryConverter implements DocumentConverter<UniProtEntry, UniProtDocument> {
	static final int SORT_FIELD_MAX_LENGTH = 30;
	static final int MAX_STORED_FIELD_LENGTH = 32766;
	private static final Logger LOGGER = LoggerFactory.getLogger(UniProtEntryConverter.class);

	private static final String COMMENT = "cc_";
	private static final String CC_EV = "ccev_";
	private static final String FEATURE = "ft_";
	private static final String FT_EV = "ftev_";
	private static final String FT_LENGTH = "ftlen_";
	private static final String DASH = "-";
	private static final String GO = "go_";
	private static final String XREF_COUNT = "xref_count_";
	private static final Map<Integer, String> POPULAR_ORGANIMS_TAX_NAME = Collections
			.unmodifiableMap(new HashMap<Integer, String>() {
				{
					put(9606, "Human");
					put(10090, "Mouse");
					put(10116, "Rat");
					put(9913, "Bovine");
					put(7955, "Zebrafish");
					put(7227, "Fruit fly");
					put(6239, "C. elegans");
					put(44689, "Slime mold");
					put(3702, "A. thaliana");
					put(39947, "Rice");
					put(83333, "E. coli K12");
					put(224308, "B. subtilis");
					put(559292, "S. cerevisiae");
				}
			});
	private static final Pattern PATTERN_FAMILY = Pattern.compile(
			"(?:In the .+? section; )?[Bb]elongs to the (.+?family)\\.(?: (.+?family)\\.)?(?: (.+?family)\\.)?(?: Highly divergent\\.)?");
	/**
	 * An enum set representing all of the organelles that are children of plastid
	 */
	private static final EnumSet<GeneEncodingType> PLASTID_CHILD = EnumSet.of(GeneEncodingType.APICOPLAST_PLASTID,
			GeneEncodingType.CHLOROPLAST_PLASTID, GeneEncodingType.CYANELLE_PLASTID,
			GeneEncodingType.NON_PHOTOSYNTHETIC_PLASTID, GeneEncodingType.CHROMATOPHORE_PLASTID);
	private static final String MANUAL_EVIDENCE = "manual";
	private static final List<String> MANUAL_EVIDENCE_MAP = asList("ECO_0000269", "ECO_0000303", "ECO_0000305",
			"ECO_0000250", "ECO_0000255", "ECO_0000244", "ECO_0000312");
	private static final String AUTOMATIC_EVIDENCE = "automatic";
	private static final List<String> AUTOMATIC_EVIDENCE_MAP = asList("ECO_0000256", "ECO_0000213", "ECO_0000313",
			"ECO_0000259");
	private static final String EXPERIMENTAL_EVIDENCE = "experimental";
	private static final List<String> EXPERIMENTAL_EVIDENCE_MAP = Collections.singletonList("ECO_0000269");
	private final TaxonomyRepo taxonomyRepo;
	private final RALineBuilder raLineBuilder;
	private final RGLineBuilder rgLineBuilder;
	private final GoRelationRepo goRelationRepo;
	private final PathwayRepo pathwayRepo;
	private final ChebiRepo chebiRepo;
	private final ECRepo ecRepo;
	private Map<String, SuggestDocument> suggestions;
	// private final UniProtUniRefMap uniprotUniRefMap;

	public UniProtEntryConverter(TaxonomyRepo taxonomyRepo, GoRelationRepo goRelationRepo, PathwayRepo pathwayRepo,
			ChebiRepo chebiRepo, ECRepo ecRepo, Map<String, SuggestDocument> suggestDocuments) {
		this.taxonomyRepo = taxonomyRepo;
		this.goRelationRepo = goRelationRepo;
		this.pathwayRepo = pathwayRepo;
		this.chebiRepo = chebiRepo;
		this.ecRepo = ecRepo;
		this.suggestions = suggestDocuments;
		// this.uniprotUniRefMap = uniProtUniRefMap;

		raLineBuilder = new RALineBuilder();
		rgLineBuilder = new RGLineBuilder();
	}

	static String truncatedSortValue(String value) {
		if (value.length() > SORT_FIELD_MAX_LENGTH) {
			return value.substring(0, SORT_FIELD_MAX_LENGTH);
		} else {
			return value;
		}
	}

	static String getDefaultBinaryValue(String base64Value) {
		if (base64Value.length() <= MAX_STORED_FIELD_LENGTH) {
			return base64Value;
		} else {
			return null;
		}
	}

	static String createSuggestionMapKey(SuggestDictionary dict, String id) {
		return dict.name() + ":" + id;
	}

	@Override
	public UniProtDocument convert(UniProtEntry source) {
		try {
			UniProtDocument japiDocument = new UniProtDocument();
			// 1 UniProtEntry creates a single JAPIDocument

			japiDocument.accession = source.getPrimaryAccession().getValue();
			if (japiDocument.accession.contains(DASH)) {
				if (this.isCanonicalIsoform(source)) {
					japiDocument.reviewed = null;
					japiDocument.isIsoform = null;
					return japiDocument;
				}
				japiDocument.isIsoform = true;
				// We are adding the canonical accession to the isoform entry as a secondary
				// accession.
				// this way when you search by an accession and ask to include isoforms, it will
				// find it.
				String canonicalAccession = japiDocument.accession.substring(0, japiDocument.accession.indexOf(DASH));
				japiDocument.secacc.add(canonicalAccession);
			} else {
				japiDocument.isIsoform = false;
			}
			japiDocument.reviewed = (source.getEntryType() == UniProtEntryType.SWISSPROT);

			setId(source, japiDocument);
			addValueListToStringList(japiDocument.secacc, source.getSecondaryAccessions());

			EntryAudit entryAudit = source.getEntryAudit();
			if (entryAudit != null) {
				japiDocument.firstCreated = DateUtils.convertLocalDateToDate(entryAudit.getFirstPublicDate());
				japiDocument.lastModified = DateUtils.convertLocalDateToDate(entryAudit.getLastAnnotationUpdateDate());
				japiDocument.sequenceUpdated = DateUtils.convertLocalDateToDate(entryAudit.getLastSequenceUpdateDate());
			}
			setProteinNames(source, japiDocument);
			setECNumbers(source, japiDocument);
			setKeywords(source, japiDocument);
			setOrganism(source, japiDocument);
			setLineageTaxons(source, japiDocument);
			setOrganismHosts(source, japiDocument);
			convertGeneNames(source, japiDocument);
			if (!japiDocument.geneNamesExact.isEmpty()) {
				japiDocument.geneNames.addAll(japiDocument.geneNamesExact);
				japiDocument.geneNamesSort = truncatedSortValue(String.join(" ", japiDocument.geneNames));
			}
			convertGoTerms(source, japiDocument);
			convertReferences(source, japiDocument);
			convertComment(source, japiDocument);
			convertXref(source, japiDocument);
			setOrganelle(source, japiDocument);
			convertFeature(source, japiDocument);
			setFragmentNPrecursor(source, japiDocument);
			setProteinExistence(source, japiDocument);
			setSequence(source, japiDocument);
			setScore(source, japiDocument);
			japiDocument.xrefCountMap = getXrefCount(source);
			japiDocument.sources = getEvidences(source);
			// TODO: 04/07/19 commented out for testing
			// setAvroDefaultEntry(source, japiDocument);
			setDefaultSearchContent(japiDocument);
			setUniRefClusters(japiDocument.accession, japiDocument);

			return japiDocument;
		} catch (IllegalArgumentException | NullPointerException e) {
			String message = "Error converting UniProt entry: " + source.getPrimaryAccession().getValue();
			log.error(message, e);
			throw new DocumentConversionException(message, e);
		}
	}

	private List<String> getEvidences(UniProtEntry uniProtEntry) {
		List<Evidence> evidences = uniProtEntry.gatherEvidences();
		return evidences.stream().map(val -> val.getSource()).filter(val -> val != null)
				.map(val -> val.getDatabaseType())
				.filter(val -> (val != null) && val.getDetail().getCategory() == EvidenceTypeCategory.A).map(val -> {
					String data = val.getName();
					if (data.equalsIgnoreCase("HAMAP-rule"))
						data = "HAMAP";
					return data;
				}).filter(val -> val != null).map(val -> val.toLowerCase()).collect(Collectors.toList());

	}

	private Map<String, Long> getXrefCount(UniProtEntry uniProtEntry) {
		return uniProtEntry.getDatabaseCrossReferences().stream()
				.map(val -> XREF_COUNT + val.getDatabaseType().getName().toLowerCase())
				.collect(Collectors.groupingBy(Function.identity(), Collectors.counting()));

	}

	public boolean isCanonicalIsoform(UniProtEntry uniProtEntry) {
		return uniProtEntry.getCommentByType(CommentType.ALTERNATIVE_PRODUCTS).stream()
				.map(comment -> (AlternativeProductsComment) comment).flatMap(comment -> comment.getIsoforms().stream())
				.filter(isoform -> isoform.getIsoformSequenceStatus() == IsoformSequenceStatus.DISPLAYED)
				.flatMap(isoform -> isoform.getIsoformIds().stream())
				.filter(isoformId -> isoformId.getValue().equals(uniProtEntry.getPrimaryAccession().getValue()))
				.count() == 1L;
	}

	void setId(UniProtEntry source, UniProtDocument japiDocument) {
		japiDocument.id = source.getUniProtId().getValue();
		String[] idParts = japiDocument.id.split("_");
		if (idParts.length == 2) {
			if (japiDocument.reviewed) {
				// first component of swiss-prot id is gene, which we want searchable in the
				// mnemonic
				japiDocument.idDefault = japiDocument.id;
			} else {
				// don't add first component for trembl entries, since this is the accession,
				// and
				// we do not want false boosting for default searches that match a substring of
				// the accession
				japiDocument.idDefault = idParts[1];
			}
		}
	}

	Map<String, SuggestDocument> getSuggestions() {
		return suggestions;
	}

	void setLineageTaxon(int taxId, UniProtDocument document) {
		if (taxId > 0) {
			List<TaxonomicNode> nodes = TaxonomyRepoUtil.getTaxonomyLineage(taxonomyRepo, taxId);
			nodes.forEach(node -> {
				int id = node.id();
				document.taxLineageIds.add(id);
				List<String> taxons = TaxonomyRepoUtil.extractTaxonFromNode(node);
				document.organismTaxon.addAll(taxons);
				addTaxonSuggestions(SuggestDictionary.TAXONOMY, id, taxons);
			});
		}
	}

	private static void addValueListToStringList(Collection<String> list, List<? extends Value> values) {
		if (values != null) {
			for (Value v : values) {
				addValueToStringList(list, v);
			}
		}
	}

	private static void addValueToStringList(Collection<String> list, Value value) {
		if ((value != null) && (!value.getValue().isEmpty())) {
			list.add(value.getValue());
		}
	}

	private void setDefaultSearchContent(UniProtDocument japiDocument) {
		japiDocument.content.add(japiDocument.accession);
		japiDocument.content.addAll(japiDocument.secacc);
		japiDocument.content.add(japiDocument.id); // mnemonic
		japiDocument.content.addAll(japiDocument.proteinNames);
		japiDocument.content.addAll(japiDocument.keywords);
		japiDocument.content.addAll(japiDocument.geneNames);

		japiDocument.content.add(String.valueOf(japiDocument.organismTaxId));
		japiDocument.content.addAll(japiDocument.organismName);
		japiDocument.content.addAll(japiDocument.organismHostNames);
		japiDocument.content
				.addAll(japiDocument.organismHostIds.stream().map(String::valueOf).collect(Collectors.toList()));

		japiDocument.content.addAll(japiDocument.organelles);

		japiDocument.content.addAll(japiDocument.referenceAuthors);
		japiDocument.content.addAll(japiDocument.referenceJournals);
		japiDocument.content.addAll(japiDocument.referenceOrganizations);
		japiDocument.content.addAll(japiDocument.referencePubmeds);
		japiDocument.content.addAll(japiDocument.referenceTitles);

		japiDocument.content.addAll(japiDocument.proteomes);
		japiDocument.content.addAll(japiDocument.proteomeComponents);

		japiDocument.content.addAll(japiDocument.xrefs);
		japiDocument.content.addAll(japiDocument.databases);

		// IMPORTANT: we also add information to content in other methods:
		// comments --> see convertComment method
		// features --> see convertFeature method
		// go Terms --> see convertGoTerms method
	}

	private void setUniRefClusters(String accession, UniProtDocument japiDocument) {
		// japiDocument.unirefCluster50 = uniprotUniRefMap.getMappings50(accession);
		// japiDocument.unirefCluster90 = uniprotUniRefMap.getMappings90(accession);
		// japiDocument.unirefCluster100 = uniprotUniRefMap.getMappings100(accession);
	}

	private void setAvroDefaultEntry(UniProtEntry source, UniProtDocument japiDocument) {
		try {
			UniProtAccession accession = new UniProtAccessionBuilder(source.getPrimaryAccession().getValue()).build();
			UniProtId uniProtId = new UniProtIdBuilder(source.getUniProtId().getValue()).build();
			UniProtEntry defaultObject = new UniProtEntryBuilder().primaryAccession(accession).uniProtId(uniProtId)
					.active().entryType(UniProtEntryType.valueOf(source.getEntryType().name()))
					.proteinDescription(source.getProteinDescription()).genes(source.getGenes())
					.organism(source.getOrganism()).sequence(new SequenceImpl(source.getSequence().getValue())).build();

			byte[] avroByteArray = UniprotJsonConfig.getInstance().getDefaultFullObjectMapper()
					.writeValueAsBytes(defaultObject);
			// can only store if it's size is small enough:
			// https://lucene.apache.org/core/7_5_0/core/org/apache/lucene/index/DocValuesType.html
			japiDocument.avro_binary = getDefaultBinaryValue(Base64.getEncoder().encodeToString(avroByteArray));
		} catch (JsonProcessingException exception) {
			String accession = source.getPrimaryAccession().getValue();
			LOGGER.warn("Error saving default uniprot object in avro_binary for accession: " + accession, exception);
		}
	}

	private void setScore(UniProtEntry source, UniProtDocument japiDocument) {
		UniProtEntryScored entryScored = new UniProtEntryScored(source);
		double score = entryScored.score();
		int q = (int) (score / 20d);
		japiDocument.score = q > 4 ? 5 : q + 1;
	}

	private void setSequence(UniProtEntry source, UniProtDocument japiDocument) {
		Sequence seq = source.getSequence();
		japiDocument.seqLength = seq.getLength();
		japiDocument.seqMass = seq.getMolWeight();
	}

	private void setProteinNames(UniProtEntry source, UniProtDocument japiDocument) {
		if (source.hasProteinDescription()) {
			ProteinDescription proteinDescription = source.getProteinDescription();
			List<String> names = extractProteinDescriptionValues(proteinDescription);
			japiDocument.proteinNames.addAll(names);
			japiDocument.proteinsNamesSort = truncatedSortValue(String.join(" ", names));
		}
	}

	private void setECNumbers(UniProtEntry source, UniProtDocument doc) {
		if (source.hasProteinDescription()) {
			ProteinDescription proteinDescription = source.getProteinDescription();
			List<String> ecNumbers = extractProteinDescriptionEcs(proteinDescription);
			doc.ecNumbers = ecNumbers;
			doc.ecNumbersExact = doc.ecNumbers;

			for (String ecNumber : ecNumbers) {
				ecRepo.getEC(ecNumber)
						.ifPresent(ec -> suggestions.putIfAbsent(createSuggestionMapKey(SuggestDictionary.EC, ecNumber),
								SuggestDocument.builder().id(ecNumber).value(ec.label())
										.dictionary(SuggestDictionary.EC.name()).build()));
			}
		}
	}

	private void setKeywords(UniProtEntry source, UniProtDocument japiDocument) {
		if (source.hasKeywords()) {
			source.getKeywords().forEach(keyword -> updateKeyword(keyword, japiDocument));
		}
	}

	private void updateKeyword(Keyword keyword, UniProtDocument japiDocument) {	
		japiDocument.keywords.add(keyword.getId());
		addValueToStringList(japiDocument.keywords, keyword);
		KeywordCategory kc = keyword.getCategory();		
		if (!japiDocument.keywords.contains(kc.getAccession())) {
			japiDocument.keywords.add(kc.getAccession());
			japiDocument.keywords.add(kc.getName());
		}

		suggestions.putIfAbsent(createSuggestionMapKey(SuggestDictionary.KEYWORD, keyword.getId()),
				SuggestDocument.builder().id(keyword.getId()).value(keyword.getValue())
						.dictionary(SuggestDictionary.KEYWORD.name()).build());

		suggestions.putIfAbsent(createSuggestionMapKey(SuggestDictionary.KEYWORD, kc.getAccession()),
				SuggestDocument.builder().id(kc.getAccession()).value(kc.getName())
						.dictionary(SuggestDictionary.KEYWORD.name()).build());
	}

	private void setOrganism(UniProtEntry source, UniProtDocument japiDocument) {
		if (source.hasOrganism()) {
			int taxonomyId = Math.toIntExact(source.getOrganism().getTaxonId());
			japiDocument.organismTaxId = taxonomyId;

			Optional<TaxonomicNode> taxonomicNode = taxonomyRepo.retrieveNodeUsingTaxID(taxonomyId);
			if (taxonomicNode.isPresent()) {

				TaxonomicNode node = taxonomicNode.get();
				List<String> extractedTaxoNode = TaxonomyRepoUtil.extractTaxonFromNode(node);
				japiDocument.organismName.addAll(extractedTaxoNode);
				japiDocument.organismSort = truncatedSortValue(String.join(" ", extractedTaxoNode));

				String popularOrgamism = POPULAR_ORGANIMS_TAX_NAME.get(taxonomyId);
				if (popularOrgamism != null) {
					japiDocument.popularOrganism = popularOrgamism;
				} else {
					if (node.mnemonic() != null && !node.mnemonic().isEmpty()) {
						japiDocument.otherOrganism = node.mnemonic();
					} else if (node.commonName() != null && !node.commonName().isEmpty()) {
						japiDocument.otherOrganism = node.commonName();
					} else {
						japiDocument.otherOrganism = node.scientificName();
					}
				}
				addTaxonSuggestions(SuggestDictionary.ORGANISM, taxonomyId, extractedTaxoNode);

			}
		}
	}

	private void setLineageTaxons(UniProtEntry source, UniProtDocument japiDocument) {
		if (source.hasOrganism()) {
			int taxId = Math.toIntExact(source.getOrganism().getTaxonId());
			setLineageTaxon(taxId, japiDocument);
		}
	}

	private void setOrganismHosts(UniProtEntry source, UniProtDocument japiDocument) {
		List<OrganismHost> hosts = source.getOrganismHosts();
		hosts.forEach(host -> {
			int taxonomyId = Math.toIntExact(host.getTaxonId());
			japiDocument.organismHostIds.add(taxonomyId);
			Optional<TaxonomicNode> taxonomicNode = taxonomyRepo.retrieveNodeUsingTaxID(taxonomyId);
			if (taxonomicNode.isPresent()) {
				TaxonomicNode node = taxonomicNode.get();
				List<String> extractedTaxoNode = TaxonomyRepoUtil.extractTaxonFromNode(node);
				japiDocument.organismHostNames.addAll(extractedTaxoNode);
				addTaxonSuggestions(SuggestDictionary.VIRUS_HOST, taxonomyId, extractedTaxoNode);
			}
		});

	}

	private void convertGeneNames(UniProtEntry source, UniProtDocument japiDocument) {
		if (source.hasGenes()) {
			for (Gene gene : source.getGenes()) {
				addValueToStringList(japiDocument.geneNamesExact, gene.getGeneName());
				addValueListToStringList(japiDocument.geneNamesExact, gene.getSynonyms());
				addValueListToStringList(japiDocument.geneNamesExact, gene.getOrderedLocusNames());
				addValueListToStringList(japiDocument.geneNamesExact, gene.getOrfNames());
			}
		}
	}

	private void convertGoTerms(UniProtEntry source, UniProtDocument japiDocument) {
		List<UniProtDBCrossReference> goTerms = source.getDatabaseCrossReferencesByType("GO");
		if (goTerms != null) {
			for (UniProtDBCrossReference go : goTerms) {
				String goTerm = go.getProperties().stream()
						.filter(property -> property.getKey().equalsIgnoreCase("GoTerm"))
						.map(property -> property.getValue().split(":")[1]).collect(Collectors.joining());
				String evType = go.getProperties().stream()
						.filter(property -> property.getKey().equalsIgnoreCase("GoEvidenceType"))
						.map(property -> property.getValue().split(":")[0].toLowerCase()).collect(Collectors.joining());

				addGoterm(evType, go.getId(), goTerm, japiDocument);
				addAncestors(evType, go.getId(), japiDocument);

				japiDocument.content.add(go.getId().substring(3));// id
				japiDocument.content.add(goTerm); // term
			}
		}
	}

	private void addAncestors(String evType, String goTerm, UniProtDocument doc) {
		Set<GoTerm> ancestors = goRelationRepo.getAncestors(goTerm, asList(IS_A, PART_OF));
		if(ancestors !=null)
			ancestors.forEach(ancestor -> addGoterm(evType, ancestor.getId(), ancestor.getName(), doc));
	}

	private void addGoterm(String evType, String goId, String term, UniProtDocument japiDocument) {
		String key = GO + evType;
		Collection<String> values = japiDocument.goWithEvidenceMaps.computeIfAbsent(key, k -> new HashSet<>());
		String idOnly = goId.substring(3);
		values.add(idOnly);
		values.add(term);

		japiDocument.goes.add(idOnly);
		japiDocument.goes.add(term);
		japiDocument.goIds.add(idOnly);

		suggestions.putIfAbsent(createSuggestionMapKey(SuggestDictionary.GO, idOnly),
				SuggestDocument.builder().id(idOnly).value(term).dictionary(SuggestDictionary.GO.name()).build());
	}

	private void convertRcs(List<ReferenceComment> referenceComments, UniProtDocument japiDocument) {
		referenceComments.forEach(referenceComment -> {
			if (referenceComment.hasValue()) {
				switch (referenceComment.getType()) {
				case STRAIN:
					japiDocument.rcStrain.add(referenceComment.getValue());
					break;
				case TISSUE:
					japiDocument.rcTissue.add(referenceComment.getValue());
					break;
				case PLASMID:
					japiDocument.rcPlasmid.add(referenceComment.getValue());
					break;
				case TRANSPOSON:
					japiDocument.rcTransposon.add(referenceComment.getValue());
					break;
				}
			}
		});
	}

	private void convertRp(UniProtReference uniProtReference, UniProtDocument japiDocument) {
		if (uniProtReference.hasReferencePositions()) {
			japiDocument.scopes.addAll(uniProtReference.getReferencePositions());
		}
	}

	private void convertReferences(UniProtEntry source, UniProtDocument japiDocument) {
		for (UniProtReference reference : source.getReferences()) {
			Citation citation = reference.getCitation();
			if (reference.hasReferenceComments()) {
				convertRcs(reference.getReferenceComments(), japiDocument);
			}
			convertRp(reference, japiDocument);
			if (citation.hasTitle()) {
				japiDocument.referenceTitles.add(citation.getTitle());
			}
			if (citation.hasAuthors()) {
				japiDocument.referenceAuthors.add(raLineBuilder.buildLine(citation.getAuthors(), false, false).get(0));
			}
			if (citation.hasAuthoringGroup()) {
				japiDocument.referenceOrganizations
						.add(rgLineBuilder.buildLine(citation.getAuthoringGroup(), false, false).get(0));
			}
			if (citation.hasPublicationDate()) {
				String pubDate = citation.getPublicationDate().getValue();
				try {
					PublicationDateFormatter dateFormatter = null;
					if (PublicationDateFormatter.DAY_DIGITMONTH_YEAR.isValidDate(pubDate)) {
						dateFormatter = PublicationDateFormatter.DAY_DIGITMONTH_YEAR;
					} else if (PublicationDateFormatter.DAY_THREE_LETTER_MONTH_YEAR.isValidDate(pubDate)) {
						dateFormatter = PublicationDateFormatter.DAY_THREE_LETTER_MONTH_YEAR;
					} else if (PublicationDateFormatter.YEAR_DIGIT_MONTH.isValidDate(pubDate)) {
						dateFormatter = PublicationDateFormatter.YEAR_DIGIT_MONTH;
					} else if (PublicationDateFormatter.THREE_LETTER_MONTH_YEAR.isValidDate(pubDate)) {
						dateFormatter = PublicationDateFormatter.THREE_LETTER_MONTH_YEAR;
					} else if (PublicationDateFormatter.YEAR.isValidDate(pubDate)) {
						dateFormatter = PublicationDateFormatter.YEAR;
					}
					if (dateFormatter != null) {
						japiDocument.referenceDates.add(dateFormatter.convertStringToDate(pubDate));
					}
				} catch (Exception e) {
					LOGGER.warn("There was a problem converting entry dates during indexing:", e);
				}
			}
			if (citation.getCitationXrefsByType(CitationXrefType.PUBMED).isPresent()) {
				DBCrossReference<CitationXrefType> pubmed = citation.getCitationXrefsByType(CitationXrefType.PUBMED)
						.get();
				japiDocument.referencePubmeds.add(pubmed.getId());
			}
			if (citation instanceof JournalArticle) {
				JournalArticle ja = (JournalArticle) citation;
				japiDocument.referenceJournals.add(ja.getJournal().getName());
			}
		}
	}

	private void convertComment(UniProtEntry source, UniProtDocument doc) {
		for (Comment comment : source.getComments()) {
			FFLineBuilder<Comment> fbuilder = CCLineBuilderFactory.create(comment);
			String field = getCommentField(comment);
			String evField = getCommentEvField(comment);
			Collection<String> value = doc.commentMap.computeIfAbsent(field, k -> new ArrayList<>());

			String commentVal = fbuilder.buildString(comment);
			value.add(commentVal);
			doc.content.add(commentVal);

			Collection<String> evValue = doc.commentEvMap.computeIfAbsent(evField, k -> new HashSet<>());
			Set<String> evidences = fetchEvidences(comment);
			evValue.addAll(evidences);

			doc.proteinsWith.add(comment.getCommentType().name().toLowerCase());

			switch (comment.getCommentType()) {
			case CATALYTIC_ACTIVITY:
				convertCatalyticActivity((CatalyticActivityComment) comment, doc);
				break;
			case COFACTOR:
				convertFactor((CofactorComment) comment, doc);
				break;
			case BIOPHYSICOCHEMICAL_PROPERTIES:
				convertCommentBPCP((BPCPComment) comment, doc);
				break;
			case PATHWAY:
				convertPathway((FreeTextComment) comment, doc);
				break;
			case INTERACTION:
				convertCommentInteraction((InteractionComment) comment, doc);
				break;
			case SUBCELLULAR_LOCATION:
				convertCommentSL((SubcellularLocationComment) comment, doc);
				break;
			case ALTERNATIVE_PRODUCTS:
				convertCommentAP((AlternativeProductsComment) comment, doc);
				break;
			case SIMILARITY:
				convertCommentFamily((FreeTextComment) comment, doc);
				break;
			case SEQUENCE_CAUTION:
				convertCommentSC((SequenceCautionComment) comment, doc);
				break;
			default:
				break;
			}
		}

		doc.proteinsWith.removeIf(this::filterUnnecessaryProteinsWithCommentTypes);
	}

	private boolean filterUnnecessaryProteinsWithCommentTypes(String commentType) {
		return commentType.equalsIgnoreCase(CommentType.MISCELLANEOUS.toString())
				|| commentType.equalsIgnoreCase(CommentType.SIMILARITY.toString())
				|| commentType.equalsIgnoreCase(CommentType.CAUTION.toString())
				|| commentType.equalsIgnoreCase(CommentType.SEQUENCE_CAUTION.toString())
				|| commentType.equalsIgnoreCase(CommentType.WEBRESOURCE.toString())
				|| commentType.equalsIgnoreCase(CommentType.UNKNOWN.toString());
	}

	private void convertCatalyticActivity(CatalyticActivityComment comment, UniProtDocument doc) {
		Reaction reaction = comment.getReaction();
		
		if (reaction.hasReactionReferences()) {
			String field = this.getCommentField(comment);
			List<DBCrossReference<ReactionReferenceType>> reactionReferences = reaction.getReactionReferences();
			reactionReferences.stream().filter(ref -> ref.getDatabaseType() == ReactionReferenceType.CHEBI)
					.forEach( val ->addCatalyticSuggestions(doc, field, val));
		}
	}

	private void addCatalyticSuggestions( UniProtDocument doc, String field, DBCrossReference<ReactionReferenceType> reactionReference) {
		if (reactionReference.getDatabaseType() == ReactionReferenceType.CHEBI) {
			String referenceId = reactionReference.getId();
			int firstColon = referenceId.indexOf(':');
			String fullId = referenceId.substring(firstColon + 1);
			Chebi chebi = chebiRepo.getById(fullId);
			if (nonNull(chebi)) {
				addChebiSuggestions(SuggestDictionary.CATALYTIC_ACTIVITY, referenceId, chebi);
				Collection<String> value = doc.commentMap.computeIfAbsent(field, k -> new ArrayList<>());
				value.add(referenceId);
			}
		}
	}

	private void addChebiSuggestions(SuggestDictionary dicType, String id, Chebi chebi) {
		SuggestDocument.SuggestDocumentBuilder suggestionBuilder = SuggestDocument.builder().id(id)
				.dictionary(dicType.name()).value(chebi.getName());
		if (!nullOrEmpty(chebi.getInchiKey())) {
			suggestionBuilder.altValue(chebi.getInchiKey());
		}
		suggestions.putIfAbsent(createSuggestionMapKey(dicType, id),
				suggestionBuilder.build());
	}
	private Set<String> fetchEvidences(Comment comment) {
		Set<String> evidences = new HashSet<>();
		if (comment instanceof FreeTextComment) {
			FreeTextComment toComment = (FreeTextComment) comment;
			evidences.addAll(extractEvidence(toComment.getTexts().stream().flatMap(val -> val.getEvidences().stream())
					.collect(Collectors.toList())));
		}
		CommentType type = comment.getCommentType();
		switch (type) {
		case DISEASE:
			DiseaseComment diseaseComment = (DiseaseComment) comment;
			if (diseaseComment.hasDefinedDisease()) {
				evidences.addAll(extractEvidence(diseaseComment.getDisease().getEvidences()));
				if (diseaseComment.hasNote() && diseaseComment.getNote().hasTexts()) {
					evidences.addAll(extractEvidence(diseaseComment.getNote().getTexts().stream()
							.flatMap(val -> val.getEvidences().stream()).collect(Collectors.toList())));
				}
			}
			break;
		case RNA_EDITING:
			RnaEditingComment reComment = (RnaEditingComment) comment;
			if (reComment.hasPositions()) {
				evidences.addAll(extractEvidence(reComment.getPositions().stream()
						.flatMap(val -> val.getEvidences().stream()).collect(Collectors.toList())));
			}
			if (reComment.hasNote()) {
				evidences.addAll(extractEvidence(reComment.getNote().getTexts().stream()
						.flatMap(val -> val.getEvidences().stream()).collect(Collectors.toList())));
			}
			break;
		case MASS_SPECTROMETRY:
			MassSpectrometryComment msComment = (MassSpectrometryComment) comment;
			evidences.addAll(extractEvidence(msComment.getEvidences()));
			break;
		}
		return evidences;
	}

	private void convertCommentInteraction(InteractionComment comment, UniProtDocument japiDocument) {
		comment.getInteractions().forEach(interaction -> {
			if (interaction.hasFirstInteractor()) {
				japiDocument.interactors.add(interaction.getFirstInteractor().getValue());
			}
			if (interaction.hasSecondInteractor()) {
				japiDocument.interactors.add(interaction.getSecondInteractor().getValue());
			}
			if (interaction.hasUniProtAccession()) {
				japiDocument.interactors.add(interaction.getUniProtAccession().getValue());
			}
		});
	}

	private void convertCommentFamily(FreeTextComment comment, UniProtDocument japiDocument) {
		comment.getTexts().stream().map(Value::getValue).forEach(val -> updateFamily(val, japiDocument));
	}

	private void convertPathway(FreeTextComment comment, UniProtDocument japiDocument) {
		comment.getTexts().stream().map(Value::getValue).forEach(val -> updatePathway(val, japiDocument));
	}

	private void updatePathway(String val, UniProtDocument japiDocument) {
		UniPathway unipathway = pathwayRepo.getFromName(val);
		if (unipathway != null) {
			japiDocument.pathway.add(unipathway.getAccession());
		}
	}

	private void updateFamily(String val, UniProtDocument japiDocument) {
		if (!val.endsWith(".")) {
			val += ".";
		}
		Matcher m = PATTERN_FAMILY.matcher(val);
		if (m.matches()) {
			StringBuilder line = new StringBuilder();
			line.append(m.group(1));
			if (m.group(2) != null)
				line.append(", ").append(m.group(2));
			if (m.group(3) != null)
				line.append(", ").append(m.group(3));
			japiDocument.familyInfo.add(line.toString());
		}
	}

	private void convertCommentSC(SequenceCautionComment comment, UniProtDocument japiDocument) {
		String val = comment.getSequenceCautionType().toDisplayName();
		if (comment.hasNote()) {
			val = comment.getNote();
		}
		Set<String> evidence = extractEvidence(comment.getEvidences());
		japiDocument.seqCaution.add(val);
		japiDocument.seqCautionEv.addAll(evidence);
		switch (comment.getSequenceCautionType()) {
		case FRAMESHIFT:
			japiDocument.seqCautionFrameshift.add(val);
			// japiDocument.seqCautionFrameshiftEv.addAll(evidence);
			break;
		case ERRONEOUS_INITIATION:
			japiDocument.seqCautionErInit.add(val);
			// japiDocument.seqCautionErInitEv.addAll(evidence);
			break;
		case ERRONEOUS_TERMIINATION:
			japiDocument.seqCautionErTerm.add(val);
			// japiDocument.seqCautionErTermEv.addAll(evidence);
			break;
		case ERRONEOUS_PREDICTION:
			japiDocument.seqCautionErPred.add(val);
			// japiDocument.seqCautionErPredEv.addAll(evidence);
			break;
		case ERRONEOUS_TRANSLATION:
			japiDocument.seqCautionErTran.add(val);
			// japiDocument.seqCautionErTranEv.addAll(evidence);
			break;
		case MISCELLANEOUS_DISCREPANCY:
			japiDocument.seqCautionMisc.add(val);
			japiDocument.seqCautionMiscEv.addAll(evidence);
			break;

		default:

		}

	}

	private void convertCommentBPCP(BPCPComment comment, UniProtDocument japiDocument) {
		if (comment.hasAbsorption()) {
			Absorption absorption = comment.getAbsorption();
			japiDocument.bpcpAbsorption.add("" + absorption.getMax());
			japiDocument.bpcpAbsorptionEv.addAll(extractEvidence(absorption.getEvidences()));
			if (absorption.getNote() != null) {
				japiDocument.bpcpAbsorption.addAll(
						absorption.getNote().getTexts().stream().map(Value::getValue).collect(Collectors.toList()));
				japiDocument.bpcpAbsorptionEv.addAll(extractEvidence(absorption.getNote().getTexts().stream()
						.flatMap(val -> val.getEvidences().stream()).collect(Collectors.toList())));
			}
			japiDocument.bpcp.addAll(japiDocument.bpcpAbsorption);
			japiDocument.bpcpEv.addAll(japiDocument.bpcpAbsorptionEv);
		}
		if (comment.hasKineticParameters()) {
			KineticParameters kp = comment.getKineticParameters();
			kp.getMaximumVelocities().stream().map(MaximumVelocity::getEnzyme)
					.filter(val -> !Strings.isNullOrEmpty(val)).forEach(japiDocument.bpcpKinetics::add);
			japiDocument.bpcpKineticsEv.addAll(extractEvidence(kp.getMaximumVelocities().stream()
					.flatMap(val -> val.getEvidences().stream()).collect(Collectors.toList())));
			kp.getMichaelisConstants().stream().map(MichaelisConstant::getSubstrate)
					.filter(val -> !Strings.isNullOrEmpty(val)).forEach(japiDocument.bpcpKinetics::add);
			japiDocument.bpcpKineticsEv.addAll(extractEvidence(kp.getMichaelisConstants().stream()
					.flatMap(val -> val.getEvidences().stream()).collect(Collectors.toList())));
			if (kp.getNote() != null) {
				japiDocument.bpcpKinetics
						.addAll(kp.getNote().getTexts().stream().map(Value::getValue).collect(Collectors.toList()));
				japiDocument.bpcpKineticsEv.addAll(extractEvidence(kp.getNote().getTexts().stream()
						.flatMap(val -> val.getEvidences().stream()).collect(Collectors.toList())));
			}
			if (japiDocument.bpcpKinetics.isEmpty()) {
				kp.getMaximumVelocities().stream().map(val -> "" + val.getVelocity())
						.forEach(japiDocument.bpcpKinetics::add);
				kp.getMichaelisConstants().stream().map(val -> "" + val.getConstant())
						.forEach(japiDocument.bpcpKinetics::add);
			}
			japiDocument.bpcp.addAll(japiDocument.bpcpKinetics);
			japiDocument.bpcpEv.addAll(japiDocument.bpcpKineticsEv);

		}
		if (comment.hasPhDependence()) {
			comment.getPhDependence().getTexts().stream().map(Value::getValue)
					.forEach(japiDocument.bpcpPhDependence::add);
			japiDocument.bpcpPhDependenceEv.addAll(extractEvidence(comment.getPhDependence().getTexts().stream()
					.flatMap(val -> val.getEvidences().stream()).collect(Collectors.toList())));
			japiDocument.bpcp.addAll(japiDocument.bpcpPhDependence);
			japiDocument.bpcpEv.addAll(japiDocument.bpcpPhDependenceEv);
		}
		if (comment.hasRedoxPotential()) {
			comment.getRedoxPotential().getTexts().stream().map(Value::getValue)
					.forEach(japiDocument.bpcpRedoxPotential::add);
			japiDocument.bpcpRedoxPotentialEv.addAll(extractEvidence(comment.getRedoxPotential().getTexts().stream()
					.flatMap(val -> val.getEvidences().stream()).collect(Collectors.toList())));
			japiDocument.bpcp.addAll(japiDocument.bpcpRedoxPotential);
			japiDocument.bpcpEv.addAll(japiDocument.bpcpRedoxPotentialEv);
		}
		if (comment.hasTemperatureDependence()) {
			comment.getTemperatureDependence().getTexts().stream().map(Value::getValue)
					.forEach(japiDocument.bpcpTempDependence::add);
			japiDocument.bpcpTempDependenceEv.addAll(extractEvidence(comment.getTemperatureDependence().getTexts()
					.stream().flatMap(val -> val.getEvidences().stream()).collect(Collectors.toList())));
			japiDocument.bpcp.addAll(japiDocument.bpcpTempDependence);
			japiDocument.bpcpEv.addAll(japiDocument.bpcpTempDependenceEv);
		}
	}

	private void convertCommentSL(SubcellularLocationComment comment, UniProtDocument japiDocument) {
		if (comment.hasSubcellularLocations()) {
			comment.getSubcellularLocations().forEach(subcellularLocation -> {
				if (subcellularLocation.hasLocation()) {
					SubcellularLocationValue location = subcellularLocation.getLocation();
					japiDocument.subcellLocationTerm.add(location.getValue());

					Set<String> locationEv = extractEvidence(location.getEvidences());
					japiDocument.subcellLocationTermEv.addAll(locationEv);
					japiDocument.subcellLocationTerm.add(location.getId());
					addSubcellSuggestion(location);
				}
				if (subcellularLocation.hasOrientation()) {
					SubcellularLocationValue orientation = subcellularLocation.getOrientation();
					japiDocument.subcellLocationTerm.add(orientation.getValue());

					Set<String> orientationEv = extractEvidence(orientation.getEvidences());
					japiDocument.subcellLocationTermEv.addAll(orientationEv);
					japiDocument.subcellLocationTerm.add(orientation.getId());
					addSubcellSuggestion(orientation);
				}
				if (subcellularLocation.hasTopology()) {
					SubcellularLocationValue topology = subcellularLocation.getTopology();
					japiDocument.subcellLocationTerm.add(topology.getValue());

					Set<String> topologyEv = extractEvidence(topology.getEvidences());
					japiDocument.subcellLocationTermEv.addAll(topologyEv);
					japiDocument.subcellLocationTerm.add(topology.getId());
					addSubcellSuggestion(topology);
				}
			});
		}
		if (comment.hasNote()) {
			comment.getNote().getTexts().stream().map(Value::getValue).forEach(japiDocument.subcellLocationNote::add);
			Set<String> noteEv = extractEvidence(comment.getNote().getTexts().stream()
					.flatMap(val -> val.getEvidences().stream()).collect(Collectors.toList()));
			japiDocument.subcellLocationNoteEv.addAll(noteEv);
		}
	}

	private void addSubcellSuggestion(SubcellularLocationValue location) {
		suggestions.putIfAbsent(createSuggestionMapKey(SuggestDictionary.SUBCELL, location.getId()),
				SuggestDocument.builder().id(location.getId()).value(location.getValue())
						.dictionary(SuggestDictionary.SUBCELL.name()).build());
	}

	private Set<String> extractEvidence(List<Evidence> evidences) {
		Set<String> extracted = evidences.stream().map(ev -> ev.getEvidenceCode().name()).collect(Collectors.toSet());
		if (extracted.stream().anyMatch(MANUAL_EVIDENCE_MAP::contains)) {
			extracted.add(MANUAL_EVIDENCE);
		}
		if (extracted.stream().anyMatch(AUTOMATIC_EVIDENCE_MAP::contains)) {
			extracted.add(AUTOMATIC_EVIDENCE);
		}
		if (extracted.stream().anyMatch(EXPERIMENTAL_EVIDENCE_MAP::contains)) {
			extracted.add(EXPERIMENTAL_EVIDENCE);
		}
		return extracted;
	}

	private void convertCommentAP(AlternativeProductsComment comment, UniProtDocument japiDocument) {
		List<String> values = new ArrayList<>();
		Set<String> evidence = new HashSet<>();
		if (comment.hasNote()) {
			comment.getNote().getTexts().stream().map(Value::getValue).forEach(values::add);

			evidence.addAll(extractEvidence(comment.getNote().getTexts().stream()
					.flatMap(val -> val.getEvidences().stream()).collect(Collectors.toList())));
		}

		List<String> events = new ArrayList<>();
		if (comment.hasEvents()) {
			comment.getEvents().stream().map(APEventType::getName).forEach(events::add);
			values.addAll(events);
		}

		japiDocument.ap.addAll(values);
		japiDocument.apEv.addAll(evidence);
		for (String event : events) {
			if ("alternative promoter usage".equalsIgnoreCase(event)) {
				japiDocument.apApu.addAll(values);
				japiDocument.apApuEv.addAll(evidence);
			}
			if ("alternative splicing".equalsIgnoreCase(event)) {
				japiDocument.apAs.addAll(values);
				japiDocument.apAsEv.addAll(evidence);
			}
			if ("alternative initiation".equalsIgnoreCase(event)) {
				japiDocument.apAi.addAll(values);
				japiDocument.apAiEv.addAll(evidence);
			}
			if ("ribosomal frameshifting".equalsIgnoreCase(event)) {
				japiDocument.apRf.addAll(values);
				japiDocument.apRfEv.addAll(evidence);
			}
		}

	}

	private void convertFactor(CofactorComment comment, UniProtDocument japiDocument) {
		
		if (comment.hasCofactors()) {
			comment.getCofactors().forEach(val -> {
				japiDocument.cofactorChebi.add(val.getName());
				if (val.getCofactorReference().getDatabaseType() == CofactorReferenceType.CHEBI) {
					String referenceId = val.getCofactorReference().getId();
					String id =referenceId;
					if (id.startsWith("CHEBI:"))
						id = id.substring("CHEBI:".length());
					japiDocument.cofactorChebi.add(id);
				
					Chebi chebi = chebiRepo.getById(id);
					if (nonNull(chebi)) {
						addChebiSuggestions(SuggestDictionary.CHEBI, referenceId, chebi);
						japiDocument.cofactorChebi.add(referenceId);
					}	
				}
				japiDocument.cofactorChebiEv.addAll(extractEvidence(val.getEvidences()));
			});
		}

		if ((comment.hasNote()) && (comment.getNote().hasTexts())) {
			comment.getNote().getTexts().forEach(val -> {
				japiDocument.cofactorNote.add(val.getValue());
				japiDocument.cofactorNoteEv.addAll(extractEvidence(val.getEvidences()));
			});
		}
	}

	private void convertXref(UniProtEntry source, UniProtDocument japiDocument) {
		boolean d3structure = false;
		for (UniProtDBCrossReference xref : source.getDatabaseCrossReferences()) {
			if (xref.getDatabaseType().getName().equalsIgnoreCase("PDB")) {
				d3structure = true;
			}
			String dbname = xref.getDatabaseType().getName().toLowerCase();
			japiDocument.databases.add(dbname);
			String id = xref.getId();
			if (!dbname.equalsIgnoreCase("PIR")) {
				addXrefId(id, dbname, japiDocument.xrefs);
			}
			switch (dbname.toLowerCase()) {
			case "embl":
				if (xref.hasProperties()) {
					Optional<String> proteinId = xref.getProperties().stream()
							.filter(property -> property.getKey().equalsIgnoreCase("ProteinId"))
							.filter(property -> !property.getValue().equalsIgnoreCase("-")).map(Property::getValue)
							.findFirst();
					proteinId.ifPresent(s -> addXrefId(s, dbname, japiDocument.xrefs));
				}
				break;
			case "refseq":
			case "pir":
			case "unipathway":
			case "ensembl":
				if (xref.hasProperties()) {
					List<String> properties = xref.getProperties().stream()
							.filter(property -> !property.getValue().equalsIgnoreCase("-")).map(Property::getValue)
							.collect(Collectors.toList());
					properties.forEach(s -> addXrefId(s, dbname, japiDocument.xrefs));
				}
				break;
			case "proteomes":
				japiDocument.proteomes.add(xref.getId());
				if (xref.hasProperties()) {
					japiDocument.proteomeComponents
							.addAll(xref.getProperties().stream().map(Property::getValue).collect(Collectors.toSet()));
				}
			default:

			}

		}
		japiDocument.d3structure = d3structure;
		if (d3structure) {
			japiDocument.proteinsWith.add("3dstructure");
		}
	}

	private void addXrefId(String id, String dbname, Collection<String> values) {
		values.add(id);
		values.add(dbname + "-" + id);
		if (id.indexOf(".") > 0) {
			String idMain = id.substring(0, id.indexOf("."));
			values.add(idMain);
			values.add(dbname + "-" + idMain);
		}
	}

	private void setOrganelle(UniProtEntry source, UniProtDocument japiDocument) {
		for (GeneLocation geneLocation : source.getGeneLocations()) {
			GeneEncodingType geneEncodingType = geneLocation.getGeneEncodingType();

			if (PLASTID_CHILD.contains(geneEncodingType)) {
				japiDocument.organelles.add(GeneEncodingType.PLASTID.getName());
			}

			String organelleValue = geneEncodingType.getName();
			if (geneLocation.getValue() != null && !geneLocation.getValue().isEmpty()) {
				organelleValue += " " + geneLocation.getValue();
			}
			organelleValue = organelleValue.trim();

			japiDocument.organelles.add(organelleValue);
		}
	}

	private void convertFeature(UniProtEntry source, UniProtDocument japiDocument) {
		for (Feature feature : source.getFeatures()) {
			FFLineBuilder<Feature> fbuilder = FeatureLineBuilderFactory.create(feature);

			String field = getFeatureField(feature, FEATURE);
			String evField = getFeatureField(feature, FT_EV);
			String lengthField = getFeatureField(feature, FT_LENGTH);
			Collection<String> featuresOfTypeList = japiDocument.featuresMap.computeIfAbsent(field,
					k -> new HashSet<>());
			String featureText = fbuilder.buildString(feature);
			featuresOfTypeList.add(featureText);
			japiDocument.content.add(featureText);
			japiDocument.proteinsWith.add(feature.getType().name().toLowerCase());

			// start and end of location
			int length = feature.getLocation().getEnd().getValue() - feature.getLocation().getStart().getValue() + 1;
			Set<String> evidences = extractEvidence(feature.getEvidences());
			Collection<Integer> lengthList = japiDocument.featureLengthMap.computeIfAbsent(lengthField,
					k -> new HashSet<>());
			lengthList.add(length);

			Collection<String> evidenceList = japiDocument.featureEvidenceMap.computeIfAbsent(evField,
					k -> new HashSet<>());
			evidenceList.addAll(evidences);
		}
		japiDocument.proteinsWith.removeIf(this::filterUnnecessaryProteinsWithFeatureTypes);
	}

	private boolean filterUnnecessaryProteinsWithFeatureTypes(String featureType) {
		return featureType.equalsIgnoreCase(FeatureType.SITE.toString())
				|| featureType.equalsIgnoreCase(FeatureType.UNSURE.toString())
				|| featureType.equalsIgnoreCase(FeatureType.CONFLICT.toString())
				|| featureType.equalsIgnoreCase(FeatureType.NON_CONS.toString())
				|| featureType.equalsIgnoreCase(FeatureType.NON_TER.toString());
	}

	private void setFragmentNPrecursor(UniProtEntry source, UniProtDocument japiDocument) {
		ProteinDescription description = source.getProteinDescription();
		boolean isFragment = false;
		boolean isPrecursor = false;
		if (source.hasProteinDescription() && description.hasFlag()) {
			Flag flag = description.getFlag();
			switch (flag.getType()) {
			case FRAGMENT:
			case FRAGMENTS:
				isFragment = true;
				break;
			case PRECURSOR:
				isPrecursor = true;
				break;
			case FRAGMENT_PRECURSOR:
			case FRAGMENTS_PRECURSOR:
				isPrecursor = true;
				isFragment = true;
				break;
			}
		}
		japiDocument.fragment = isFragment;
		japiDocument.precursor = isPrecursor;
	}

	private void setProteinExistence(UniProtEntry source, UniProtDocument japiDocument) {
		ProteinExistence proteinExistence = source.getProteinExistence();
		if (proteinExistence != null) {
			japiDocument.proteinExistence = proteinExistence.name();
		}
	}

	private List<String> extractProteinDescriptionEcs(ProteinDescription proteinDescription) {
		List<String> ecs = new ArrayList<>();
		if (proteinDescription.hasRecommendedName() && proteinDescription.getRecommendedName().hasEcNumbers()) {
			ecs.addAll(getEcs(proteinDescription.getRecommendedName().getEcNumbers()));
		}
		if (proteinDescription.hasSubmissionNames()) {
			proteinDescription.getSubmissionNames().stream().filter(ProteinSubName::hasEcNumbers)
					.flatMap(proteinSubName -> getEcs(proteinSubName.getEcNumbers()).stream()).forEach(ecs::add);
		}
		if (proteinDescription.hasAlternativeNames()) {
			proteinDescription.getAlternativeNames().stream().filter(ProteinAltName::hasEcNumbers)
					.flatMap(proteinAltName -> getEcs(proteinAltName.getEcNumbers()).stream()).forEach(ecs::add);
		}
		if (proteinDescription.hasContains()) {
			proteinDescription.getContains().stream()
					.flatMap(proteinSection -> getProteinSectionEcs(proteinSection).stream()).forEach(ecs::add);
		}
		if (proteinDescription.hasIncludes()) {
			proteinDescription.getIncludes().stream()
					.flatMap(proteinSection -> getProteinSectionEcs(proteinSection).stream()).forEach(ecs::add);
		}
		return ecs;
	}

	private List<String> getProteinSectionEcs(ProteinSection proteinSection) {
		List<String> ecs = new ArrayList<>();
		if (proteinSection.hasRecommendedName() && proteinSection.getRecommendedName().hasEcNumbers()) {
			ecs.addAll(getEcs(proteinSection.getRecommendedName().getEcNumbers()));
		}
		if (proteinSection.hasAlternativeNames()) {
			proteinSection.getAlternativeNames().stream().filter(ProteinAltName::hasEcNumbers)
					.flatMap(proteinAltName -> getEcs(proteinAltName.getEcNumbers()).stream()).forEach(ecs::add);
		}
		return ecs;
	}

	private List<String> getEcs(List<EC> ecs) {
		return ecs.stream().map(EC::getValue).collect(Collectors.toList());
	}

	private List<String> extractProteinDescriptionValues(ProteinDescription description) {
		List<String> values = new ArrayList<>();
		if (description.hasRecommendedName()) {
			values.addAll(getProteinRecNameNames(description.getRecommendedName()));
		}
		if (description.hasSubmissionNames()) {
			description.getSubmissionNames().stream().map(this::getProteinSubNameNames).forEach(values::addAll);
		}
		if (description.hasAlternativeNames()) {
			description.getAlternativeNames().stream().map(this::getProteinAltNameNames).forEach(values::addAll);
		}
		if (description.hasContains()) {
			description.getContains().stream().map(this::getProteinSectionValues).forEach(values::addAll);
		}
		if (description.hasIncludes()) {
			description.getIncludes().stream().map(this::getProteinSectionValues).forEach(values::addAll);
		}
		if (description.hasAllergenName()) {
			values.add(description.getAllergenName().getValue());
		}
		if (description.hasBiotechName()) {
			values.add(description.getBiotechName().getValue());
		}
		if (description.hasCdAntigenNames()) {
			description.getCdAntigenNames().stream().map(Value::getValue).forEach(values::add);
		}
		if (description.hasInnNames()) {
			description.getInnNames().stream().map(Value::getValue).forEach(values::add);
		}
		return values;
	}

	private List<String> getProteinSectionValues(ProteinSection proteinSection) {
		List<String> names = new ArrayList<>();
		if (proteinSection.hasRecommendedName()) {
			names.addAll(getProteinRecNameNames(proteinSection.getRecommendedName()));
		}
		if (proteinSection.hasAlternativeNames()) {
			proteinSection.getAlternativeNames().stream().map(this::getProteinAltNameNames).forEach(names::addAll);
		}
		if (proteinSection.hasCdAntigenNames()) {
			proteinSection.getCdAntigenNames().stream().map(Value::getValue).forEach(names::add);
		}
		if (proteinSection.hasAllergenName()) {
			names.add(proteinSection.getAllergenName().getValue());
		}
		if (proteinSection.hasInnNames()) {
			proteinSection.getInnNames().stream().map(Value::getValue).forEach(names::add);
		}
		if (proteinSection.hasBiotechName()) {
			names.add(proteinSection.getBiotechName().getValue());
		}
		return names;
	}

	private List<String> getProteinRecNameNames(ProteinRecName proteinRecName) {
		List<String> names = new ArrayList<>();
		if (proteinRecName.hasFullName()) {
			names.add(proteinRecName.getFullName().getValue());
		}
		if (proteinRecName.hasShortNames()) {
			proteinRecName.getShortNames().stream().map(Name::getValue).forEach(names::add);
		}
		return names;
	}

	private List<String> getProteinAltNameNames(ProteinAltName proteinAltName) {
		List<String> names = new ArrayList<>();
		if (proteinAltName.hasShortNames()) {
			proteinAltName.getShortNames().stream().map(Name::getValue).forEach(names::add);
		}
		if (proteinAltName.hasFullName()) {
			names.add(proteinAltName.getFullName().getValue());
		}
		return names;
	}

	private List<String> getProteinSubNameNames(ProteinSubName proteinAltName) {
		List<String> names = new ArrayList<>();
		if (proteinAltName.hasFullName()) {
			names.add(proteinAltName.getFullName().getValue());
		}
		return names;
	}

	private void addTaxonSuggestions(SuggestDictionary dicType, int id, List<String> taxons) {
		Iterator<String> taxonIterator = taxons.iterator();
		if (taxonIterator.hasNext()) {
			String idStr = Integer.toString(id);
			String key = createSuggestionMapKey(dicType, idStr);
			SuggestDocument doc;
			if (suggestions.containsKey(key)) {
				doc = suggestions.get(key);
			} else {
				SuggestDocument.SuggestDocumentBuilder documentBuilder = SuggestDocument.builder().id(idStr)
						.dictionary(dicType.name()).value(taxonIterator.next());
				while (taxonIterator.hasNext()) {
					documentBuilder.altValue(taxonIterator.next());
				}
				suggestions.put(key, documentBuilder.build());
				return;
			}

			String mainName = taxonIterator.next();
			if (doc.value != null && !doc.value.equals(mainName)) {
				doc.value = mainName;
			}

			List<String> currentSynonyms = new ArrayList<>(doc.altValues);
			while (taxonIterator.hasNext()) {
				String synonym = taxonIterator.next();
				if (!doc.altValues.contains(synonym)) {
					currentSynonyms.add(synonym);
				}
			}
			doc.altValues = currentSynonyms;
		}
	}

	private String getCommentField(Comment c) {
		String field = COMMENT + c.getCommentType().name().toLowerCase();
		return field.replaceAll(" ", "_");
	}
	private String getCommentEvField(Comment c) {
		String field = CC_EV + c.getCommentType().name().toLowerCase();
		return field.replaceAll(" ", "_");
	}

	private String getFeatureField(Feature feature, String type) {
		String field = type + feature.getType().name().toLowerCase();
		return field.replaceAll(" ", "_");
	}

}
