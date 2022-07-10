/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial agreement.
 */

package io.crate.expression.scalar.string;

import java.net.URL;
import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.elasticsearch.common.Strings;

import java.nio.charset.StandardCharsets;

import io.crate.data.Input;
import io.crate.expression.scalar.ScalarFunctionModule;
import io.crate.metadata.NodeContext;
import io.crate.metadata.Scalar;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.functions.Signature;
import io.crate.types.DataTypes;

public final class ParseURLFunction extends Scalar<Object, String> {

    private static final String NAME = "parse_url";

    public static void register(ScalarFunctionModule module) {
        module.register(
            Signature.scalar(
                NAME,
                DataTypes.STRING.getTypeSignature(),
                DataTypes.UNTYPED_OBJECT.getTypeSignature()
            ),
            ParseURLFunction::new
        );
    }

    private final Signature signature;
    private final Signature boundSignature;

    public ParseURLFunction(Signature signature, Signature boundSignature) {
        this.signature = signature;
        this.boundSignature = boundSignature;
    }

    @Override
    public Signature signature() {
        return signature;
    }

    @Override
    public Signature boundSignature() {
        return boundSignature;
    }

    @Override
    @SafeVarargs
    public final Object evaluate(TransactionContext txnCtx, NodeContext nodeCtx, Input<String>... args) {
        String url = args[0].value();
        if (url == null) {
            return null;
        }
        return parseURL(url);
    }

    private final Object parseURL(String urlText) {
        final Map<String, Object> urlMap = new LinkedHashMap<>();

        URL url = null;

        String scheme;
        String hostname;
        int port;
        String path;
        String query;
        String fragment;
        String userinfo;
        
        try {
            url = new URL(urlText);
            scheme = url.getProtocol();
            hostname = url.getHost();
            port = url.getPort() == -1 ? url.getDefaultPort() : url.getPort();
            path = url.getPath();
            query = url.getQuery();
            fragment = url.getRef();
            userinfo = url.getUserInfo();
        } catch (MalformedURLException e1) {
            throw new IllegalArgumentException(String.format(Locale.ENGLISH,
                                                            "unable to parse uri %s",
                                                             urlText));
        }
        
        urlMap.put("scheme", scheme);
        urlMap.put("hostname", hostname);
        urlMap.put("port", port == -1 ? null : port);
        urlMap.put("path", decodeText(path));
        urlMap.put("query", parseQuery(query));
        urlMap.put("fragment", fragment);
        urlMap.put("userinfo",  userinfo);

        return urlMap;
    }

    private final Map<String,List<String>> parseQuery(String query) {
        if (Strings.isNullOrEmpty(query)) {
            return null;
        }

        Map<String,List<String>> queryMap = new HashMap<String,List<String>>();
        Arrays.stream(query.split("&(?!amp)"))
                     .forEach((String param) -> parseQueryParameters(queryMap, param));

        return queryMap;
    }

    private final void parseQueryParameters(Map<String,List<String>> map, String params) {
        final int idx = params.indexOf("=");
        final String key = idx > 0 ? decodeText(params.substring(0, idx)) : decodeText(params);
        final String value = idx > 0 && params.length() > idx + 1 ? decodeText(params.substring(idx + 1)) : "";
        if (!map.containsKey(key)) {
            map.put(key, new ArrayList<String>());
        }
        map.get(key).add(value);
    }

    private final String decodeText(String text) {
        return java.net.URLDecoder.decode(text, StandardCharsets.UTF_8);
    }

}
