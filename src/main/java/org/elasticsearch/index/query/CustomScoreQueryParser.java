/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.index.query;

import org.apache.lucene.search.Filter;
import org.apache.lucene.search.Query;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.lucene.search.XConstantScoreQuery;
import org.elasticsearch.common.lucene.search.function.FunctionScoreQuery;
import org.elasticsearch.common.lucene.search.function.ScriptScoreFunction;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.script.SearchScript;

import java.io.IOException;
import java.util.Map;

/**
 * @deprecated use {@link FunctionScoreQueryParser} instead.
 */
public class CustomScoreQueryParser implements QueryParser {

    public static final String NAME = "custom_score";

    @Inject
    public CustomScoreQueryParser() {
    }

    @Override
    public String[] names() {
        return new String[] { NAME, Strings.toCamelCase(NAME) };
    }

    @Override
    public Query parse(QueryParseContext parseContext) throws IOException, QueryParsingException {
        XContentParser parser = parseContext.parser();

        Query query = null;
        Filter filter = null;
        boolean queryOrFilterFound = false;
        float boost = 1.0f;
        String script = null;
        String scriptLang = null;
        Map<String, Object> vars = null;

        String currentFieldName = null;
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == XContentParser.Token.START_OBJECT) {
                if ("query".equals(currentFieldName)) {
                    query = parseContext.parseInnerQuery();
                    queryOrFilterFound = true;
                } else if ("filter".equals(currentFieldName)) {
                    filter = parseContext.parseInnerFilter();
                    queryOrFilterFound = true;
                } else if ("params".equals(currentFieldName)) {
                    vars = parser.map();
                } else {
                    throw new QueryParsingException(parseContext.index(), "[custom_score] query does not support [" + currentFieldName
                            + "]");
                }
            } else if (token.isValue()) {
                if ("script".equals(currentFieldName)) {
                    script = parser.text();
                } else if ("lang".equals(currentFieldName)) {
                    scriptLang = parser.text();
                } else if ("boost".equals(currentFieldName)) {
                    boost = parser.floatValue();
                } else {
                    throw new QueryParsingException(parseContext.index(), "[custom_score] query does not support [" + currentFieldName
                            + "]");
                }
            }
        }
        if (!queryOrFilterFound) {
            throw new QueryParsingException(parseContext.index(), "[custom_score] requires 'query' or 'filter' field");
        }
        if (script == null) {
            throw new QueryParsingException(parseContext.index(), "[custom_score] requires 'script' field");
        }
        if (query == null && filter == null) {
            return null;
        } else if (filter != null) {
            query = new XConstantScoreQuery(filter);
        }

        SearchScript searchScript;
        try {
            searchScript = parseContext.scriptService().search(parseContext.lookup(), scriptLang, script, vars);
        } catch (Exception e) {
            throw new QueryParsingException(parseContext.index(), "[custom_score] the script could not be loaded", e);
        }
        FunctionScoreQuery functionScoreQuery = new FunctionScoreQuery(query, new ScriptScoreFunction(script, vars, searchScript));
        functionScoreQuery.setBoost(boost);
        return functionScoreQuery;
    }
}