package org.uniprot.store.search.field;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

public class QueryBuilder {
	  public static char[] escaping_char = {'+', '-', '&', '|', '!', '(', ')', '{', '}', '[', ']', '^', '"', '~', '?', ':', '/'};
	  private static final DateTimeFormatter DATE_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss'Z'");
	   
	  public static String and(String query1, String query2) {
		  return query1 + " AND " + query2;
	  }
	  
	  public static String or(String query1, String query2) {
		  return query1 + " OR " + query2;
	  }
	  public static String query(String field, String queryString) {
		  return query(field, queryString,false, false);
	  }
	  public static String before(String field, LocalDate date) {
		  return rangeQuery(field, "*", date.atStartOfDay().format(DATE_FORMAT));
	  }
	  public static String after(String field, LocalDate date) {
		  return rangeQuery(field, date.atStartOfDay().format(DATE_FORMAT), "*");
	  }
	  
	  public static String rangeQuery(String field, LocalDate start, LocalDate end) {
		  return rangeQuery(field, start.atStartOfDay().format(DATE_FORMAT), 
				  end.atStartOfDay().format(DATE_FORMAT));
	  }
	  public static String rangeQuery(String field, int start, int end) {
		  return rangeQuery(field, String.valueOf(start), String.valueOf(end), true, true);
	  }
	  public static String rangeQuery(String field, long start, long end) {
		  return rangeQuery(field, String.valueOf(start), String.valueOf(end), true, true);
	  }
	  public static String rangeQuery(String field, String start, String end) {
		  return rangeQuery(field, start, end, true, true);
	  }
	  public static String rangeQuery(String field, String start, String end, boolean includeStart, boolean includeEnd) {
		  StringBuilder sb = new StringBuilder();
	        //treat the initValue with space, if it has then it is a phase search.
	        sb.append(field).append(" : ");

	        sb.append(includeStart? "[":"{");
	        sb.append(start).append(" TO ");
	        sb.append(end);
	        sb.append(includeEnd? "]":"}");

	        return sb.toString();
	  }
	  
	public static String query(String field, String queryString, boolean isPhraseQuery, boolean negative) {
		
		StringBuilder sb = new StringBuilder();
		 if (negative) {
	            sb.append("!(");
	        } else {
	            sb.append("(");
	        }
	        sb.append(field).append(":");

	        boolean hasSpace = queryString.contains(" ");
	        if(hasSpace ) {
	        	if(isPhraseQuery)
	        		sb.append("\"");
	        	else
	        		sb.append("(");
	        }
	        sb.append(escapingString(queryString));
	        if(hasSpace) {
	        	if(isPhraseQuery)
	        		sb.append("\"");
	        	else
	        		sb.append(")");
	        }
	        sb.append(")");
	        return sb.toString();
	
	}
	
	 public static String escapingString(String input) {
	        if (input.equals("*")) return input;

	        StringBuilder stringBuilder = new StringBuilder();
	        for (int i = 0; i < input.length(); i++) {
	            char cc = input.charAt(i);
	            if (needEscape(cc)) {
	                stringBuilder.append('\\');
	            }
	            stringBuilder.append(cc);
	        }
	        return stringBuilder.toString();
	    }

	  private static boolean needEscape(char c) {
		  	for(char ch: escaping_char ) {
		  		if(ch ==c)
		  			return true;
		  	}
	       return false;
	    }
}
