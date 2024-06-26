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
package org.elasticsearch.action.admin.cluster.configuration;


import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;

import org.elasticsearch.test.ESTestCase;

import io.crate.common.unit.TimeValue;

public class ClearVotingConfigExclusionsRequestTests extends ESTestCase {
    public void testSerialization() throws IOException {
        final ClearVotingConfigExclusionsRequest originalRequest = new ClearVotingConfigExclusionsRequest();
        if (randomBoolean()) {
            originalRequest.setWaitForRemoval(randomBoolean());
        }
        if (randomBoolean()) {
            originalRequest.setTimeout(TimeValue.timeValueMillis(randomLongBetween(0, 30000)));
        }
        final ClearVotingConfigExclusionsRequest deserialized
            = copyWriteable(originalRequest, writableRegistry(), ClearVotingConfigExclusionsRequest::new);
        assertThat(deserialized.getWaitForRemoval()).isEqualTo(originalRequest.getWaitForRemoval());
        assertThat(deserialized.getTimeout()).isEqualTo(originalRequest.getTimeout());
    }
}
