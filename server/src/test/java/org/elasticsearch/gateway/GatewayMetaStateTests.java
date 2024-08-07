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

package org.elasticsearch.gateway;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Objects;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexTemplateMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.MetadataIndexUpgradeService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.MetadataUpgrader;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.TestCustomMetadata;
import org.junit.Test;
import org.mockito.Mockito;

import io.crate.metadata.NodeContext;

public class GatewayMetaStateTests extends ESTestCase {

    public void testAddCustomMetadataOnUpgrade() throws Exception {
        Metadata metadata = randomMetadata();
        MetadataUpgrader metadataUpgrader = new MetadataUpgrader(
            Collections.singletonList(customs -> {
                customs.put(CustomMetadata1.TYPE, new CustomMetadata1("modified_data1"));
                return customs;
            }),
            Collections.emptyList()
        );
        Metadata upgrade = GatewayMetaState.upgradeMetadata(metadata, new MockMetadataIndexUpgradeService(false), metadataUpgrader);
        assertThat(upgrade != metadata).isTrue();
        assertThat(Metadata.isGlobalStateEquals(upgrade, metadata)).isFalse();
        assertThat(upgrade.custom(CustomMetadata1.TYPE) != null).isTrue();
        assertThat(((TestCustomMetadata) upgrade.custom(CustomMetadata1.TYPE)).getData()).isEqualTo("modified_data1");
    }

    public void testRemoveCustomMetadataOnUpgrade() throws Exception {
        Metadata metadata = randomMetadata(new CustomMetadata1("data"));
        MetadataUpgrader metadataUpgrader = new MetadataUpgrader(
            Collections.singletonList(customs -> {
                customs.remove(CustomMetadata1.TYPE);
                return customs;
            }),
            Collections.emptyList()
        );
        Metadata upgrade = GatewayMetaState.upgradeMetadata(metadata, new MockMetadataIndexUpgradeService(false), metadataUpgrader);
        assertThat(upgrade != metadata).isTrue();
        assertThat(Metadata.isGlobalStateEquals(upgrade, metadata)).isFalse();
        assertThat(upgrade.custom(CustomMetadata1.TYPE) == null).isTrue();
    }

    public void testUpdateCustomMetadataOnUpgrade() throws Exception {
        Metadata metadata = randomMetadata(new CustomMetadata1("data"));
        MetadataUpgrader metadataUpgrader = new MetadataUpgrader(
            Collections.singletonList(customs -> {
                customs.put(CustomMetadata1.TYPE, new CustomMetadata1("modified_data1"));
                return customs;
            }),
            Collections.emptyList()
        );
    }

    public void testNoMetadataUpgrade() {
        Metadata metadata = randomMetadata(new CustomMetadata1("data"));
        MetadataUpgrader metadataUpgrader = new MetadataUpgrader(List.of(), List.of());
        Metadata upgrade = GatewayMetaState.upgradeMetadata(metadata, new MockMetadataIndexUpgradeService(false), metadataUpgrader);
        assertThat(upgrade).isSameAs(metadata);
        assertThat(Metadata.isGlobalStateEquals(upgrade, metadata)).isTrue();
        for (IndexMetadata indexMetadata : upgrade) {
            assertThat(metadata.hasIndexMetadata(indexMetadata)).isTrue();
        }
    }

    public void testCustomMetadataValidation() {
        Metadata metadata = randomMetadata(new CustomMetadata1("data"));
        MetadataUpgrader metadataUpgrader = new MetadataUpgrader(List.of(), List.of());
        try {
            GatewayMetaState.upgradeMetadata(metadata, new MockMetadataIndexUpgradeService(false), metadataUpgrader);
        } catch (IllegalStateException e) {
            assertThat(e.getMessage()).isEqualTo("custom meta data too old");
        }
    }

    public void testIndexMetadataUpgrade() {
        Metadata metadata = randomMetadata();
        MetadataUpgrader metadataUpgrader = new MetadataUpgrader(List.of(), List.of());
        Metadata upgrade = GatewayMetaState.upgradeMetadata(metadata, new MockMetadataIndexUpgradeService(true), metadataUpgrader);
        assertThat(upgrade).isNotSameAs(metadata);
        assertThat(Metadata.isGlobalStateEquals(upgrade, metadata)).isTrue();
        for (IndexMetadata indexMetadata : upgrade) {
            assertThat(metadata.hasIndexMetadata(indexMetadata)).isFalse();
        }
    }

    public void testCustomMetadataNoChange() {
        Metadata metadata = randomMetadata(new CustomMetadata1("data"));
        MetadataUpgrader metadataUpgrader = new MetadataUpgrader(List.of(), List.of(HashMap::new));
        Metadata upgrade = GatewayMetaState.upgradeMetadata(metadata, new MockMetadataIndexUpgradeService(false), metadataUpgrader);
        assertThat(upgrade).isSameAs(metadata);
        assertThat(Metadata.isGlobalStateEquals(upgrade, metadata)).isTrue();
        for (IndexMetadata indexMetadata : upgrade) {
            assertThat(metadata.hasIndexMetadata(indexMetadata)).isTrue();
        }
    }

    public void testIndexTemplateValidation() {
        Metadata metadata = randomMetadata();
        MetadataUpgrader metadataUpgrader = new MetadataUpgrader(List.of(), List.of(
            customs -> {
                throw new IllegalStateException("template is incompatible");
            }));
        assertThatThrownBy(() -> GatewayMetaState.upgradeMetadata(metadata, new MockMetadataIndexUpgradeService(false), metadataUpgrader))
            .isExactlyInstanceOf(IllegalStateException.class)
            .hasMessage("template is incompatible");
    }


    @Test
    public void testMultipleIndexTemplateUpgrade() throws Exception {
        final Metadata metadata;
        switch (randomIntBetween(0, 2)) {
            case 0:
                metadata = randomMetadataWithIndexTemplates("template1", "template2");
                break;
            case 1:
                metadata = randomMetadataWithIndexTemplates(randomBoolean() ? "template1" : "template2");
                break;
            case 2:
                metadata = randomMetadata();
                break;
            default:
                throw new IllegalStateException("should never happen");
        }
        MetadataUpgrader metadataUpgrader = new MetadataUpgrader(
            Collections.emptyList(),
            Arrays.asList(
                indexTemplateMetadatas -> {
                    try {
                        indexTemplateMetadatas.put("template1", IndexTemplateMetadata.builder("template1")
                            .patterns(randomIndexPatterns())
                            .settings(Settings.builder().put(IndexMetadata.INDEX_NUMBER_OF_SHARDS_SETTING.getKey(), 20).build())
                            .putMapping("{}")
                            .build());
                    } catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                    return indexTemplateMetadatas;

                },
                indexTemplateMetadatas -> {
                    try {
                        indexTemplateMetadatas.put("template2", IndexTemplateMetadata.builder("template2")
                            .patterns(randomIndexPatterns())
                            .putMapping("{}")
                            .settings(Settings.builder().put(IndexMetadata.INDEX_NUMBER_OF_REPLICAS_SETTING.getKey(), 10).build()).build());
                    } catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                    return indexTemplateMetadatas;
            }
        ));
        Metadata upgrade = GatewayMetaState.upgradeMetadata(metadata, new MockMetadataIndexUpgradeService(false), metadataUpgrader);
        assertThat(upgrade != metadata).isTrue();
        assertThat(Metadata.isGlobalStateEquals(upgrade, metadata)).isFalse();
        assertThat(upgrade.templates().get("template1")).isNotNull();
        assertThat(IndexMetadata.INDEX_NUMBER_OF_SHARDS_SETTING.get(upgrade.templates().get("template1").settings())).isEqualTo(20);
        assertThat(upgrade.templates().get("template2")).isNotNull();
        assertThat(IndexMetadata.INDEX_NUMBER_OF_REPLICAS_SETTING.get(upgrade.templates().get("template2").settings())).isEqualTo(10);
        for (IndexMetadata indexMetadata : upgrade) {
            assertThat(metadata.hasIndexMetadata(indexMetadata)).isTrue();
        }
    }

    public static class MockMetadataIndexUpgradeService extends MetadataIndexUpgradeService {
        private final boolean upgrade;

        public MockMetadataIndexUpgradeService(boolean upgrade) {
            super(Mockito.mock(NodeContext.class), null, null);
            this.upgrade = upgrade;
        }

        @Override
        public IndexMetadata upgradeIndexMetadata(IndexMetadata indexMetadata, IndexTemplateMetadata indexTemplateMetadata, Version minimumIndexCompatibilityVersion) {
            return upgrade ? IndexMetadata.builder(indexMetadata).build() : indexMetadata;
        }
    }

    private static class CustomMetadata1 extends TestCustomMetadata {
        public static final String TYPE = "custom_md_1";

        protected CustomMetadata1(String data) {
            super(data);
        }

        @Override
        public String getWriteableName() {
            return TYPE;
        }

        @Override
        public Version getMinimalSupportedVersion() {
            return Version.CURRENT;
        }

        @Override
        public EnumSet<Metadata.XContentContext> context() {
            return EnumSet.of(Metadata.XContentContext.GATEWAY);
        }
    }

    public static Metadata randomMetadata(TestCustomMetadata... customMetadatas) {
        Metadata.Builder builder = Metadata.builder();
        for (TestCustomMetadata customMetadata : customMetadatas) {
            builder.putCustom(customMetadata.getWriteableName(), customMetadata);
        }
        for (int i = 0; i < randomIntBetween(1, 5); i++) {
            builder.put(
                IndexMetadata.builder(randomAlphaOfLength(10))
                    .settings(settings(Version.CURRENT))
                    .numberOfReplicas(randomIntBetween(0, 3))
                    .numberOfShards(randomIntBetween(1, 5))
            );
        }
        return builder.build();
    }

    private static Metadata randomMetadataWithIndexTemplates(String... templates) throws IOException {
        Metadata.Builder builder = Metadata.builder();
        for (String template : templates) {
            IndexTemplateMetadata templateMetadata = IndexTemplateMetadata.builder(template)
                .settings(settings(Version.CURRENT)
                    .put(IndexMetadata.INDEX_NUMBER_OF_REPLICAS_SETTING.getKey(), randomIntBetween(0, 3))
                    .put(IndexMetadata.INDEX_NUMBER_OF_SHARDS_SETTING.getKey(), randomIntBetween(1, 5)))
                .patterns(randomIndexPatterns())
                .putMapping("{}")
                .build();
            builder.put(templateMetadata);
        }
        for (int i = 0; i < randomIntBetween(1, 5); i++) {
            builder.put(
                IndexMetadata.builder(randomAlphaOfLength(10))
                    .settings(settings(Version.CURRENT))
                    .numberOfReplicas(randomIntBetween(0, 3))
                    .numberOfShards(randomIntBetween(1, 5))
            );
        }
        return builder.build();
    }

    private static List<String> randomIndexPatterns() {
        return Arrays.asList(Objects.requireNonNull(generateRandomStringArray(10, 100, false, false)));
    }
}
