package org.uniprot.store.search;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.*;
import org.uniprot.core.util.Utils;

/**
 * This is a utility class to help extract information about solr query
 *
 * @author lgonzales
 */
public class SolrQueryUtil {

    private SolrQueryUtil() {}

    public static String getTermValue(String inputQuery, String term) {
        String result = "";
        List<String> results = getTermValues(inputQuery, term);
        if (!results.isEmpty()) {
            result = results.get(0);
        }
        return result;
    }

    public static List<String> getTermValues(String inputQuery, String term) {
        List<String> result = new ArrayList<>();
        try {
            QueryParser qp = new QueryParser("", new WhitespaceAnalyzer());
            qp.setAllowLeadingWildcard(true);
            Query query = qp.parse(inputQuery);
            result.addAll(getTermValues(query, term));
        } catch (Exception e) {
            // Syntax error is validated by ValidSolrQuerySyntax
        }
        return result;
    }

    public static String getTermValue(Query inputQuery, String term) {
        String result = "";
        List<String> results = getTermValues(inputQuery, term);
        if (!results.isEmpty()) {
            result = results.get(0);
        }
        return result;
    }

    public static List<String> getTermValues(Query inputQuery, String term) {
        List<String> result = new ArrayList<>();
        if (inputQuery instanceof TermQuery) {
            TermQuery termQuery = (TermQuery) inputQuery;
            String fieldName = termQuery.getTerm().field();
            if (fieldName.equals(term)) {
                result.add(termQuery.getTerm().text());
            }
        } else if (inputQuery instanceof WildcardQuery) {
            WildcardQuery wildcardQuery = (WildcardQuery) inputQuery;
            String fieldName = wildcardQuery.getTerm().field();
            if (fieldName.equals(term)) {
                result.add(wildcardQuery.getTerm().text());
            }
        } else if (inputQuery instanceof TermRangeQuery) {
            TermRangeQuery rangeQuery = (TermRangeQuery) inputQuery;
            String fieldName = rangeQuery.getField();
            if (fieldName.equals(term)) {
                result.add(rangeQuery.toString(fieldName));
            }
        } else if (inputQuery instanceof PhraseQuery) {
            PhraseQuery phraseQuery = (PhraseQuery) inputQuery;
            String fieldName = phraseQuery.getTerms()[0].field();
            if (fieldName.equals(term)) {
                result.add(
                        Arrays.stream(phraseQuery.getTerms())
                                .map(Term::text)
                                .collect(Collectors.joining(" ")));
            }
        } else if (inputQuery instanceof BooleanQuery) {
            BooleanQuery booleanQuery = (BooleanQuery) inputQuery;
            for (BooleanClause clause : booleanQuery.clauses()) {
                String value = getTermValue(clause.getQuery(), term);
                if (Utils.notNullNotEmpty(value)) {
                    result.add(value);
                }
            }
        }
        return result;
    }

    public static boolean hasFieldTerms(String inputQuery, String... terms) {
        boolean isValid = false;
        try {
            QueryParser qp = new QueryParser("", new StandardAnalyzer());
            qp.setAllowLeadingWildcard(true);
            Query query = qp.parse(inputQuery);
            isValid = hasFieldTerms(query, terms);
        } catch (Exception e) {
            // Syntax error is validated by ValidSolrQuerySyntax
        }
        return isValid;
    }

    public static boolean hasNegativeTerm(String inputQuery) {
        boolean hasNegativeTerm = false;
        try {
            QueryParser qp = new QueryParser("", new StandardAnalyzer());
            qp.setAllowLeadingWildcard(true);
            Query query = qp.parse(inputQuery);
            hasNegativeTerm = hasNegativeTerm(query);
        } catch (Exception e) {
            // Syntax error is validated by ValidSolrQuerySyntax
        }
        return hasNegativeTerm;
    }

    public static boolean hasNegativeTerm(Query inputQuery) {
        boolean hasNegativeTerm = false;
        if (inputQuery instanceof BooleanQuery) {
            BooleanQuery booleanQuery = (BooleanQuery) inputQuery;
            for (BooleanClause clause : booleanQuery.clauses()) {
                if (BooleanClause.Occur.MUST_NOT.equals(clause.getOccur())) {
                    hasNegativeTerm = true;
                    break;
                }
            }
        }
        return hasNegativeTerm;
    }

    public static boolean hasFieldTerms(Query inputQuery, String... terms) {
        boolean hasTerm = false;
        List<String> termList = Arrays.asList(terms);
        if (inputQuery instanceof TermQuery) {
            TermQuery termQuery = (TermQuery) inputQuery;
            String fieldName = termQuery.getTerm().field();
            hasTerm = termList.contains(fieldName);
        } else if (inputQuery instanceof WildcardQuery) {
            WildcardQuery wildcardQuery = (WildcardQuery) inputQuery;
            String fieldName = wildcardQuery.getTerm().field();
            hasTerm = termList.contains(fieldName);
        } else if (inputQuery instanceof TermRangeQuery) {
            TermRangeQuery rangeQuery = (TermRangeQuery) inputQuery;
            String fieldName = rangeQuery.getField();
            hasTerm = termList.contains(fieldName);
        } else if (inputQuery instanceof PhraseQuery) {
            PhraseQuery phraseQuery = (PhraseQuery) inputQuery;
            String fieldName = phraseQuery.getTerms()[0].field();
            hasTerm = termList.contains(fieldName);
        } else if (inputQuery instanceof BooleanQuery) {
            BooleanQuery booleanQuery = (BooleanQuery) inputQuery;
            for (BooleanClause clause : booleanQuery.clauses()) {
                if (hasFieldTerms(clause.getQuery(), terms)) {
                    hasTerm = true;
                }
            }
        }
        return hasTerm;
    }
}
