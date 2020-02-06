package org.uniprot.store.search;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

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

    public static String getTermValue(String inputQuery, String term) {
        String result = "";
        try {
            QueryParser qp = new QueryParser("", new StandardAnalyzer());
            Query query = qp.parse(inputQuery);
            result = getTermValue(query, term);
        } catch (Exception e) {
            // Syntax error is validated by ValidSolrQuerySyntax
        }
        return result;
    }

    public static String getTermValue(Query inputQuery, String term) {
        String result = "";
        if (inputQuery instanceof TermQuery) {
            TermQuery termQuery = (TermQuery) inputQuery;
            String fieldName = termQuery.getTerm().field();
            if (fieldName.equals(term)) {
                result = termQuery.getTerm().text();
            }
        } else if (inputQuery instanceof WildcardQuery) {
            WildcardQuery wildcardQuery = (WildcardQuery) inputQuery;
            String fieldName = wildcardQuery.getTerm().field();
            if (fieldName.equals(term)) {
                result = wildcardQuery.getTerm().text();
            }
        } else if (inputQuery instanceof TermRangeQuery) {
            TermRangeQuery rangeQuery = (TermRangeQuery) inputQuery;
            String fieldName = rangeQuery.getField();
            if (fieldName.equals(term)) {
                result = rangeQuery.toString("");
            }
        } else if (inputQuery instanceof PhraseQuery) {
            PhraseQuery phraseQuery = (PhraseQuery) inputQuery;
            String fieldName = phraseQuery.getTerms()[0].field();
            if (fieldName.equals(term)) {
                result =
                        Arrays.stream(phraseQuery.getTerms())
                                .map(Term::text)
                                .collect(Collectors.joining(" "));
            }
        } else if (inputQuery instanceof BooleanQuery) {
            BooleanQuery booleanQuery = (BooleanQuery) inputQuery;
            for (BooleanClause clause : booleanQuery.clauses()) {
                String value = getTermValue(clause.getQuery(), term);
                if (Utils.notNullNotEmpty(value)) {
                    result = value;
                    break;
                }
            }
        }
        return result;
    }

    public static boolean hasFieldTerms(String inputQuery, String... terms) {
        boolean isValid = false;
        try {
            QueryParser qp = new QueryParser("", new StandardAnalyzer());
            Query query = qp.parse(inputQuery);
            isValid = hasFieldTerms(query, terms);
        } catch (Exception e) {
            // Syntax error is validated by ValidSolrQuerySyntax
        }
        return isValid;
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
