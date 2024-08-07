/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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

package org.elasticsearch.cluster.routing;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;

import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ESTestCase;

public class IndexShardRoutingTableTests extends ESTestCase {
    public void testEqualsAttributesKey() {
        List<String> attr1 = List.of("a");
        List<String> attr2 = List.of("b");
        IndexShardRoutingTable.AttributesKey attributesKey1 = new IndexShardRoutingTable.AttributesKey(attr1);
        IndexShardRoutingTable.AttributesKey attributesKey2 = new IndexShardRoutingTable.AttributesKey(attr1);
        IndexShardRoutingTable.AttributesKey attributesKey3 = new IndexShardRoutingTable.AttributesKey(attr2);
        String s = "Some random other object";
        assertThat(attributesKey1).isEqualTo(attributesKey2);
        assertThat(attributesKey2).isEqualTo(attributesKey1);
        assertThat(attributesKey1).isNotEqualTo(null);
        assertThat(attributesKey1).isNotEqualTo(s);
        assertThat(attributesKey1).isNotEqualTo(attributesKey3);
    }

    public void testEquals() {
        Index index = new Index("a", "b");
        ShardId shardId = new ShardId(index, 1);
        ShardId shardId2 = new ShardId(index, 2);
        IndexShardRoutingTable table1 = new IndexShardRoutingTable(shardId, new ArrayList<>());
        IndexShardRoutingTable table2 = new IndexShardRoutingTable(shardId, new ArrayList<>());
        IndexShardRoutingTable table3 = new IndexShardRoutingTable(shardId2, new ArrayList<>());
        String s = "Some other random object";
        assertThat(table1).isEqualTo(table2);
        assertThat(table2).isEqualTo(table1);
        assertThat(table1).isNotEqualTo(null);
        assertThat(table1).isNotEqualTo(s);
        assertThat(table1).isNotEqualTo(table3);
    }
}
