package com.findmycarrots.kafka.connect.twitterv2.basic;

import com.github.jcustenborder.kafka.connect.utils.data.SourceRecordDeque;
import com.github.jcustenborder.kafka.connect.utils.data.SourceRecordDequeBuilder;
import com.twitter.clientlib.TwitterCredentialsBearer;
import com.twitter.clientlib.api.TwitterApi;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class TwitterV2SourceTask extends SourceTask {
    static final Logger log = LoggerFactory.getLogger(TwitterV2SourceTask.class);
    SourceRecordDeque messageQueue;
    private TwitterApi twitterApi;
    TwitterV2SourceConnectorConfig config;
    V2TweetsSyncFetcher v2TweetsSyncFetcher;
    private Set<String> tweetFields, userFields, mediaFields, placeFields, pollFields, expansionFields, expansionUserFields;

    @Override
    public String version() {
        return "0.1.0.0";
    }

    @Override
    public void start(Map<String, String> props) {
        this.config = new TwitterV2SourceConnectorConfig(props);
        this.messageQueue = SourceRecordDequeBuilder.of()
                .emptyWaitMs(this.config.queueEmptyMs)
                .batchSize(this.config.queueBatchSize)
                .build();
        init();
        v2TweetsSyncFetcher = new V2TweetsSyncFetcher(twitterApi, messageQueue, config);
        new Thread(v2TweetsSyncFetcher).start();
    }

    @Override
    public List<SourceRecord> poll() throws InterruptedException {
        return this.messageQueue.getBatch();
    }

    @Override
    public void stop() {
        if (log.isInfoEnabled()) {
            log.info("Shutting down twitter stream.");
        }
        if (v2TweetsSyncFetcher != null) v2TweetsSyncFetcher.stopMe();
    }

    public void init() {
        twitterApi = new TwitterApi(new TwitterCredentialsBearer(this.config.twitterV2BearerToken));
        initFields();
    }

    private void initFields() {
        tweetFields = new HashSet<>();
        tweetFields.add("attachments");
        tweetFields.add("author_id");
        tweetFields.add("context_annotations");
        tweetFields.add("conversation_id");
        tweetFields.add("created_at");
        tweetFields.add("edit_controls");
        tweetFields.add("entities");
        tweetFields.add("geo");
        tweetFields.add("id");
        tweetFields.add("in_reply_to_user_id");
        tweetFields.add("lang");
        // tweetFields.add("non_public_metrics");
        tweetFields.add("public_metrics");
        // tweetFields.add("organic_metrics");
        // tweetFields.add("promoted_metrics");
        tweetFields.add("possibly_sensitive");
        tweetFields.add("referenced_tweets");
        tweetFields.add("reply_settings");
        tweetFields.add("source");
        tweetFields.add("text");
        tweetFields.add("withheld");

        userFields = new HashSet<>();
        userFields.add("created_at");
        userFields.add("description");
        userFields.add("entities");
        userFields.add("id");
        userFields.add("location");
        userFields.add("most_recent_tweet_id");
        userFields.add("name");
        userFields.add("pinned_tweet_id");
        userFields.add("profile_image_url");
        userFields.add("protected");
        userFields.add("public_metrics");
        userFields.add("url");
        userFields.add("username");
        userFields.add("verified");
        userFields.add("verified_type");
        userFields.add("withheld");

        mediaFields = new HashSet<>();
        mediaFields.add("duration_ms");
        mediaFields.add("height");
        mediaFields.add("media_key");
        mediaFields.add("preview_image_url");
        mediaFields.add("type");
        mediaFields.add("url");
        mediaFields.add("width");
        mediaFields.add("public_metrics");
        // mediaFields.add("non_public_metrics");
        // mediaFields.add("organic_metrics");
        // mediaFields.add("promoted_metrics");
        mediaFields.add("alt_text");
        mediaFields.add("variants");

        placeFields = new HashSet<>();
        placeFields.add("contained_within");
        placeFields.add("country");
        placeFields.add("country_code");
        placeFields.add("full_name");
        placeFields.add("geo");
        placeFields.add("id");
        placeFields.add("name");
        placeFields.add("place_type");

        pollFields = new HashSet<>();
        pollFields.add("duration_minutes");
        pollFields.add("end_datetime");
        pollFields.add("id");
        pollFields.add("options");
        pollFields.add("voting_status");

        expansionFields = new HashSet<>();
        expansionFields.add("attachments.poll_ids");
        expansionFields.add("attachments.media_keys");
        expansionFields.add("author_id");
        expansionFields.add("edit_history_tweet_ids");
        expansionFields.add("entities.mentions.username");
        expansionFields.add("geo.place_id");
        expansionFields.add("in_reply_to_user_id");
        expansionFields.add("referenced_tweets.id");
        expansionFields.add("referenced_tweets.id.author_id");

        expansionUserFields = new HashSet<>();
        expansionUserFields.add("pinned_tweet_id");
    }
}
