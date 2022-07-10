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

package io.crate.expression.scalar;

import org.junit.Test;

import io.crate.common.collections.MapBuilder;

import java.util.List;
import java.util.Map;

public class ParseURLFunctionTest extends ScalarTestCase {

    @Test
    public void test_null_input() {
        assertEvaluate("parse_url(null)", null);
    }

    @Test
    public void test_parse_url() {
        String uri = "https://crate.io/index.html";
        Map<String, Object> value = MapBuilder.<String, Object>newMapBuilder()
            .put("scheme", "https")
            .put("hostname", "crate.io")
            .put("path", "/index.html")
            .map();
        assertEvaluate(String.format("parse_url('%s')", uri), value);
    }

    @Test
    public void test_parse_url_userinfo() {
        String uri = "https://user:pwd@crate.io/";
        Map<String, Object> value = MapBuilder.<String, Object>newMapBuilder()
            .put("scheme", "https")
            .put("hostname", "crate.io")
            .put("path", "/")
            .put("user", "user")
            .put("password", "pwd")
            .map();
        assertEvaluate(String.format("parse_url('%s')", uri), value);
    }

    @Test
    public void test_parse_url_query() {
        String uri = "https://crate.io/?foo=bar&foo=bar2&foo2";
        Map<String, Object> value = MapBuilder.<String, Object>newMapBuilder()
            .put("scheme", "https")
            .put("hostname", "crate.io")
            .put("path", "/")
            .put("query", MapBuilder.<String,Object>newMapBuilder()
                                .put("foo",List.of("bar","bar2"))
                                .put("foo2",List.of(""))
                                .map())
            .map();
        assertEvaluate(String.format("parse_url('%s')", uri), value);
    }

    @Test
    public void test_parse_url_unsafe_character() {
        String uri = "https://crate.io/ /hello.gif";
        Map<String, Object> value = MapBuilder.<String, Object>newMapBuilder()
            .put("scheme", "https")
            .put("hostname", "crate.io")
            .put("path", "/ /hello.gif")
            .map();
        assertEvaluate(String.format("parse_url('%s')", uri), value);
    }

    @Test
    public void test_parse_url_complete_example() {
        String uri = "https://user:pw%26@testing.crate.io:4200/ /index.html?foo=bar&foo=&foo2=https%3A%2F%2Fcrate.io%2F%3Ffoo%3Dbar%26foo%3Dbar2%26foo2#ref";
        Map<String, Object> value = MapBuilder.<String, Object>newMapBuilder()
            .put("scheme", "https")
            .put("user", "user")
            .put("password", "pw&")
            .put("hostname", "testing.crate.io")
            .put("port", 4200)
            .put("path", "/ /index.html")
            .put("query", MapBuilder.<String,Object>newMapBuilder()
                                .put("foo",List.of("bar",""))
                                .put("foo2",List.of("https://crate.io/?foo=bar&foo=bar2&foo2"))
                                .map())
            .put("fragment", "ref")
            .map();
        assertEvaluate(String.format("parse_url('%s')", uri), value);
    }

}
