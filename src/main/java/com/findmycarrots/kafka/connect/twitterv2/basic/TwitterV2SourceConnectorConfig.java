/**
 * Copyright © 2016 Jeremy Custenborder (jcustenborder@gmail.com)
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.findmycarrots.kafka.connect.twitterv2.basic;

import com.github.jcustenborder.kafka.connect.utils.config.ConfigKeyBuilder;
import com.github.jcustenborder.kafka.connect.utils.config.ConfigUtils;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;

import java.util.*;


public class TwitterV2SourceConnectorConfig extends AbstractConfig {

  public static final String TWITTER_V2_BEARER_TOKEN_CONF = "twitterv2.bearerToken";
  public static final String TWITTER_V2_REFRESH_INTERVAL_CONF = "twitterv2.refreshInterval";
  public static final String FILTER_KEYWORDS_CONF = "filter.keywords";
  public static final String FILTER_USER_IDS_CONF = "filter.userIds";
  public static final String KAFKA_STATUS_TOPIC_CONF = "kafka.status.topic";
  public static final String KAFKA_STATUS_TOPIC_DOC = "Kafka topic to write the statuses to.";
  public static final String QUEUE_EMPTY_MS_CONF = "queue.empty.ms";
  public static final String QUEUE_BATCH_SIZE_CONF = "queue.batch.size";
  public static final String TWITTER_V2_BEARER_TOKEN_DOC = "Twitter V2 Bearer Token";
  public static final String TWITTER_V2_REFRESH_INTERVAL_DOC = "Twitter V2 Refresh Interval (in seconds)";
  private static final String FILTER_KEYWORDS_DOC = "Twitter keywords to filter for.";
  private static final String FILTER_USER_IDS_DOC = "Twitter user IDs to follow.";
  public static final String QUEUE_EMPTY_MS_DOC = "The amount of time to wait if there are no records in the queue.";
  public static final String QUEUE_BATCH_SIZE_DOC = "The number of records to return in a single batch.";


  public final String twitterV2BearerToken;
  public final int twitterV2RefreshIntervalInSecs;
  public final String topic;
  public final Set<String> filterKeywords;
  public final Set<String> filterUserIds;
  public final int queueEmptyMs;
  public final int queueBatchSize;


  public TwitterV2SourceConnectorConfig(Map<String, String> parsedConfig) {
    super(conf(), parsedConfig);
    this.twitterV2BearerToken = this.getString(TWITTER_V2_BEARER_TOKEN_CONF);
    this.twitterV2RefreshIntervalInSecs = getInt(TWITTER_V2_REFRESH_INTERVAL_CONF);
    this.topic = this.getString(KAFKA_STATUS_TOPIC_CONF);
    this.filterKeywords = ConfigUtils.getSet(this, FILTER_KEYWORDS_CONF);
    this.filterUserIds = ConfigUtils.getSet(this, FILTER_USER_IDS_CONF);
    this.queueBatchSize = getInt(QUEUE_BATCH_SIZE_CONF);
    this.queueEmptyMs = getInt(QUEUE_EMPTY_MS_CONF);
  }

  public static ConfigDef conf() {
    return new ConfigDef()
        .define(TWITTER_V2_BEARER_TOKEN_CONF, Type.STRING, Importance.HIGH, TWITTER_V2_BEARER_TOKEN_DOC)
        .define(TWITTER_V2_REFRESH_INTERVAL_CONF, Type.INT, Importance.HIGH, TWITTER_V2_REFRESH_INTERVAL_DOC)
        .define(FILTER_KEYWORDS_CONF, Type.LIST, Importance.HIGH, FILTER_KEYWORDS_DOC)
        .define(
            ConfigKeyBuilder.of(FILTER_USER_IDS_CONF, Type.LIST)
                .importance(Importance.HIGH)
                .documentation(FILTER_USER_IDS_DOC)
                .defaultValue(Collections.emptyList())
                .build()
        )
        .define(KAFKA_STATUS_TOPIC_CONF, Type.STRING, Importance.HIGH, KAFKA_STATUS_TOPIC_DOC)
        .define(
            ConfigKeyBuilder.of(QUEUE_EMPTY_MS_CONF, Type.INT)
                .importance(Importance.LOW)
                .documentation(QUEUE_EMPTY_MS_DOC)
                .defaultValue(100)
                .validator(ConfigDef.Range.atLeast(10))
                .build()
        )
        .define(
            ConfigKeyBuilder.of(QUEUE_BATCH_SIZE_CONF, Type.INT)
                .importance(Importance.LOW)
                .documentation(QUEUE_BATCH_SIZE_DOC)
                .defaultValue(100)
                .validator(ConfigDef.Range.atLeast(1))
                .build()
        );
  }
}
