package org.uniprot.store.search;

import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.*;
import org.uniprot.store.search.field.SearchField;

import static org.uniprot.core.util.Utils.notNull;

import java.util.Arrays;
import java.util.List;

/**
 * This class helps to improve default solr query search to get a better score and results.
 * <p>
 * 1. to get a better score, we add boost OR queries based on SearchField configuration ENUM.
 * for example: P53 query will be converted to: +(content:p53 (taxonomy_name:p53)^2.0 (gene:p53)^2.0)
 * See test class for more examples.
 * <p>
 * 2. to get a more accurate result we check if the term value is a valid id value and replace it to an id query
 * for example: P21802 query will be converted to accession:P21802
 * See test class for more examples.
 *
 * @author lgonzales
 */
public class DefaultSearchHandler {

    private final SearchField defaultField;
    private final SearchField idField;
    private final List<SearchField> boostFields;

    public DefaultSearchHandler(SearchField defaultField, SearchField idField, List<SearchField> boostFields) {
        this.defaultField = defaultField;
        this.idField = idField;
        this.boostFields = boostFields;
    }

    public boolean hasDefaultSearch(String inputQuery) {
        boolean isValid = false;
        try {
            QueryParser qp = new QueryParser(defaultField.getName(), new WhitespaceAnalyzer());
            Query query = qp.parse(inputQuery);
            isValid = SolrQueryUtil.hasFieldTerms(query, defaultField.getName());
        } catch (Exception e) {
            //Syntax error is validated by ValidSolrQuerySyntax
        }
        return isValid;
    }

    public String optimiseDefaultSearch(String inputQuery) {
        try {
            QueryParser qp = new QueryParser(defaultField.getName(), new WhitespaceAnalyzer());
            // Default operator AND (same as in solrconfig). Note that this has no effect
            // on query since we use .toString(). We add this just for safety.
            qp.setDefaultOperator(QueryParser.Operator.AND);
            Query query = qp.parse(inputQuery);
            Query optimisedQuery = optimiseDefaultSearch(query);
            return optimisedQuery.toString();
        } catch (Exception e) {
            //Syntax error is validated by ValidSolrQuerySyntax
        }
        return null;
    }

    private Query optimiseDefaultSearch(Query query) {
        if (query instanceof TermQuery) {
            TermQuery termQuery = (TermQuery) query;
            String fieldName = termQuery.getTerm().field();
            if (isDefaultSearchTerm(fieldName)) {
                return rewriteDefaultTermQuery(termQuery);
            } else {
                return query;
            }
        } else if (query instanceof PhraseQuery) {
            PhraseQuery phraseQuery = (PhraseQuery) query;
            String fieldName = phraseQuery.getTerms()[0].field();
            if (isDefaultSearchTerm(fieldName)) {
                return rewriteDefaultPhraseQuery((PhraseQuery) query);
            } else {
                return query;
            }
        } else if (query instanceof BooleanQuery) {
            BooleanQuery.Builder builder = new BooleanQuery.Builder();
            BooleanQuery booleanQuery = (BooleanQuery) query;
            for (BooleanClause clause : booleanQuery.clauses()) {
                Query rewritedQuery = optimiseDefaultSearch(clause.getQuery());
                builder.add(new BooleanClause(rewritedQuery, clause.getOccur()));
            }
            return builder.build();
        } else {
            return query;
        }
    }

    private boolean isDefaultSearchTerm(String fieldName) {
        return fieldName.equalsIgnoreCase(defaultField.getName());
    }

    private Query rewriteDefaultPhraseQuery(PhraseQuery phraseQuery) {
        BooleanQuery.Builder builder = new BooleanQuery.Builder();
        builder.add(new BooleanClause(phraseQuery, BooleanClause.Occur.SHOULD));
        String[] values = Arrays.stream(phraseQuery.getTerms()).map(Term::text).toArray(String[]::new);
        boostFields.stream()
                .filter(searchField -> hasValidValue(phraseQuery, searchField))
                .forEach(field -> {
                    BoostQuery boostQuery = getBoostQuery(QueryType.PHRASE,
                                                          field,
                                                          values);
                    builder.add(new BooleanClause(boostQuery, BooleanClause.Occur.SHOULD));
                });

        return builder.build();
    }

    private Query rewriteDefaultTermQuery(TermQuery query) {
        if (idField.hasValidValue(query.getTerm().text())) {
            // if it is a valid id (accession) value for example... we search directly in id (accession) field...
            return new TermQuery(new Term(idField.getName(), query.getTerm().bytes()));
        } else {
            // we need to add all boosted fields to the query to return a better scored result.
            BooleanQuery.Builder builder = new BooleanQuery.Builder();
            builder.add(new BooleanClause(query, BooleanClause.Occur.SHOULD));
            boostFields.stream()
                    .filter(searchField -> hasValidValue(query, searchField))
                    .forEach(field -> {
                        BoostQuery boostQuery = getBoostQuery(QueryType.TERM,
                                                              field,
                                                              query.getTerm().text());
                        builder.add(new BooleanClause(boostQuery, BooleanClause.Occur.SHOULD));
                    });

            return builder.build();
        }
    }

    private BoostQuery getBoostQuery(QueryType queryType, SearchField field, String... values) {
        Query boostQuery;
        if (notNull(field.getBoostValue().getValue())) {
            String boostValue = field.getBoostValue().getValue();
            if (boostValue.contains(" ")) {
                boostQuery = new PhraseQuery(field.getName(), boostValue);
            } else {
                boostQuery = new TermQuery(new Term(field.getName(), boostValue));
            }
        } else {
            if (queryType == QueryType.PHRASE) {
                boostQuery = new PhraseQuery(field.getName(), values);
            } else {
                boostQuery = new TermQuery(new Term(field.getName(), values[0]));
            }
        }
        return new BoostQuery(boostQuery, field.getBoostValue().getBoost());
    }

    private boolean hasValidValue(TermQuery query, SearchField searchField) {
        if (notNull(searchField.getBoostValue().getValue())) {
            return searchField.hasValidValue(searchField.getBoostValue().getValue());
        } else {
            return searchField.hasValidValue(query.getTerm().text());
        }
    }

    private boolean hasValidValue(PhraseQuery query, SearchField searchField) {
        if (notNull(searchField.getBoostValue().getValue())) {
            return searchField.hasValidValue(searchField.getBoostValue().getValue());
        } else {
            String[] values = Arrays.stream(query.getTerms()).map(Term::text).toArray(String[]::new);
            return searchField.hasValidValue(String.join(" ", values));
        }
    }

    private enum QueryType {
        TERM, PHRASE
    }
}
