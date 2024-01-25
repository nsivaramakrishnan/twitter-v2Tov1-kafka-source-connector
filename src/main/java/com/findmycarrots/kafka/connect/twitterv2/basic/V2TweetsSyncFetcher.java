package com.findmycarrots.kafka.connect.twitterv2.basic;

import com.findmycarrots.kafka.connect.twitterv2.StatusConverter;
import com.github.jcustenborder.kafka.connect.utils.data.SourceRecordDeque;
import com.google.common.collect.ImmutableMap;
import com.twitter.clientlib.ApiException;
import com.twitter.clientlib.api.TweetsApi;
import com.twitter.clientlib.api.TwitterApi;
import com.twitter.clientlib.model.*;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import twitter4j.JSONObject;
import twitter4j.Status;
import twitter4j.TweetV2ToV1Converter;

import java.util.*;

public class V2TweetsSyncFetcher implements Runnable {
    static final Logger logger = LoggerFactory.getLogger(V2TweetsSyncFetcher.class);
    private static final String KEYWORDS_KEY_STR = "__^^KEYWORDS^^__";
    private boolean stopped = false;
    private Map<String, Long> userNamesToFetch = new TreeMap<>();
    private Map<String, String> userNamesToLastFetchedIdMapping = new TreeMap<>();
    private Set<String> keywordsToFetch = new TreeSet<>();
    private TwitterApi twitterApi;
    private SourceRecordDeque messageQueue;
    private TwitterV2SourceConnectorConfig config;
    // private String topic;
    // private int refreshIntervalInSecs = 600;
    private static Set<String> tweetFields, userFields, mediaFields, placeFields, pollFields, expansionFields, expansionUserFields;
    static {
        initFields();
    }

    public V2TweetsSyncFetcher(TwitterApi twitterApi, SourceRecordDeque messageQueue,
                               TwitterV2SourceConnectorConfig config) {
        this.twitterApi = twitterApi;
        this.messageQueue = messageQueue;
        this.config = config;
    }
    public void stopMe() {
        this.stopped = true;
    }

    private void populateDetails() {
        for (String userName : config.filterUserIds) {
            if (!userNamesToFetch.containsKey(userName))
                userNamesToFetch.put(userName, 0L);
        }
        List<String> userNamesToRemove = new ArrayList<>();
        for (String userName : userNamesToFetch.keySet()) {
            if (!config.filterUserIds.contains(userName))
                userNamesToRemove.add(userName);
        }
        for (String userNameToRemove : userNamesToRemove)
            userNamesToFetch.remove(userNameToRemove);
        logger.info("Final list of user names::" + userNamesToFetch.keySet());

        this.keywordsToFetch.clear(); this.keywordsToFetch.addAll(config.filterKeywords);
        if (!keywordsToFetch.isEmpty() && !userNamesToFetch.containsKey(KEYWORDS_KEY_STR)) {
            userNamesToFetch.put(KEYWORDS_KEY_STR, 0L);
        }
    }

    @Override
    public void run() {
        while (!stopped) {
            try {
                populateDetails();
                String userName = getNextUserNameToFetch(false);
                if (userName.equals(KEYWORDS_KEY_STR)) {
                    logger.info("Going to fetch for keywords");
                    fetchTweetsWithKeywords();
                } else {
                    logger.info("Going to fetch the tweets for " + userName);
                    fetchTweetsForHandle(userName);
                }
                Thread.sleep(config.twitterV2RefreshIntervalInSecs * 1000L);
            } catch (Exception ex) {
                logger.error("Error while fetching tweets", ex);
            }
        }
    }

    private String getNextUserNameToFetch(boolean excludeKeyWords) {
        String userNameToReturn = null; Long lastFetchedTimeForSelectedUser = null;
        for (String userName : userNamesToFetch.keySet()) {
            if (excludeKeyWords && userName.equals(KEYWORDS_KEY_STR)) continue;
            if (userNameToReturn == null) {
                userNameToReturn = userName;
                lastFetchedTimeForSelectedUser = userNamesToFetch.get(userName);
            } else {
                if (lastFetchedTimeForSelectedUser > userNamesToFetch.get(userName)) {
                    userNameToReturn = userName;
                    lastFetchedTimeForSelectedUser = userNamesToFetch.get(userName);
                }
            }
        }
        if (KEYWORDS_KEY_STR.equals(userNameToReturn) && keywordsToFetch.isEmpty())
            return getNextUserNameToFetch(true);
        return userNameToReturn;
    }


    private void fetchTweetsForHandle(String handle) {
        logger.info("Entering fetchTweets for " + handle + " with " + userNamesToLastFetchedIdMapping);
        try {
            Get2UsersByUsernameUsernameResponse userResult = twitterApi.users().findUserByUsername(handle)
                    .tweetFields(tweetFields)
                    .userFields(userFields)
                    .expansions(expansionUserFields)
                    .execute();
            if (userResult.getErrors() != null && !userResult.getErrors().isEmpty()) {
                logger.error("Error while reading user details:");

                userResult.getErrors().forEach(e -> {
                    logger.error(e.toString());
                    if (e instanceof ResourceUnauthorizedProblem) {
                        logger.error(((ResourceUnauthorizedProblem) e).getTitle() + " " + ((ResourceUnauthorizedProblem) e).getDetail());
                    }
                });
            } else {
                logger.info("findUserByUsername - User details: " + userResult);
                String userId = Objects.requireNonNull(userResult.getData()).getId();
                if (userId != null) {
                    TweetsApi.APIusersIdTweetsRequest apIusersIdTweetsRequest = twitterApi.tweets().usersIdTweets(userId)
                            .tweetFields(tweetFields)
                            .userFields(userFields)
                            .mediaFields(mediaFields)
                            .placeFields(placeFields)
                            .pollFields(pollFields)
                            .expansions(expansionFields)
                            .maxResults(5);
                    if (userNamesToLastFetchedIdMapping.containsKey(handle)) {
                        logger.info("Going to fetch latest 5 tweets since last tweet ID " +
                                userNamesToLastFetchedIdMapping.get(handle) + " for " + handle);
                        apIusersIdTweetsRequest.sinceId(userNamesToLastFetchedIdMapping.get(handle));
                    } else {
                        logger.info("Going to fetch latest 5 tweets for " + handle);
                    }
                    Get2UsersIdTweetsResponse tweetsResult = apIusersIdTweetsRequest.execute();
                    if(tweetsResult.getErrors() != null && !tweetsResult.getErrors().isEmpty()) {
                        logger.error("Error while reading tweets:");

                        tweetsResult.getErrors().forEach(e -> {
                            logger.error(e.toString());
                            if (e instanceof ResourceUnauthorizedProblem) {
                                logger.error(((ResourceUnauthorizedProblem) e).getTitle() + " " + ((ResourceUnauthorizedProblem) e).getDetail());
                            }
                        });
                    } else {
                        logger.info("usersIdTweets - Tweet Text: " + tweetsResult);
                        boolean firstPost = true;
                        if (tweetsResult.getData() != null) {
                            logger.info(tweetsResult.getData().size() + " tweets read for " + handle);
                            for (Tweet v2Tweet : Objects.requireNonNull(tweetsResult.getData())) {
                                try {
                                    JSONObject v1FormattedJson = TweetV2ToV1Converter.convertV2ToV1Format(v2Tweet, tweetsResult.getIncludes());
                                    if (logger.isDebugEnabled())
                                        logger.debug("V1 formatted V2 data::" + v1FormattedJson);
                                    postStatus(TweetV2ToV1Converter.getStatus(v1FormattedJson));
                                    if (firstPost) {
                                        userNamesToLastFetchedIdMapping.put(handle, v2Tweet.getId());
                                        firstPost = false;
                                    }
                                } catch (Exception e) {
                                    logger.error("Error while converting the twitter V2 data to V1 format", e);
                                }
                            }
                        } else {
                            logger.info("No latest tweets found for " + handle);
                        }
                    }
                }
            }
        } catch(ApiException e) {
            logger.error("Error Status code: " + e.getCode() + ". Reason: " + e.getResponseBody() + ". Response headers: " +
                    e.getResponseHeaders());
            logger.error("Exception", e);
        } finally {
            userNamesToFetch.put(handle, System.currentTimeMillis());
        }
    }

    private void fetchTweetsWithKeywords() {
        logger.info("Entering fetchTweetsWithKeywords for " + keywordsToFetch);
        try {
            int numOfTweets = keywordsToFetch.size() * 4;
            if (numOfTweets < 10) numOfTweets = 10;
            StringBuilder query = new StringBuilder("("); boolean firstTime = true;
            for (String keyWord : keywordsToFetch) {
                if (firstTime) {
                    query.append(keyWord);
                    firstTime = false;
                } else {
                    query.append(" OR ").append(keyWord);
                }
            }
            query.append(") -is:retweet");
            TweetsApi.APItweetsRecentSearchRequest apItweetsRecentSearchRequest = twitterApi.tweets().tweetsRecentSearch(query.toString())
                    .tweetFields(tweetFields)
                    .userFields(userFields)
                    .mediaFields(mediaFields)
                    .placeFields(placeFields)
                    .pollFields(pollFields)
                    .expansions(expansionFields)
                    .maxResults(numOfTweets);
            logger.info("Going to fetch latest " + numOfTweets + " tweets with the keywords");
            Get2TweetsSearchRecentResponse tweetsResult = apItweetsRecentSearchRequest.execute();
            if(tweetsResult.getErrors() != null && !tweetsResult.getErrors().isEmpty()) {
                logger.error("Error while reading tweets using keywords:");

                tweetsResult.getErrors().forEach(e -> {
                    logger.error(e.toString());
                    if (e instanceof ResourceUnauthorizedProblem) {
                        logger.error(((ResourceUnauthorizedProblem) e).getTitle() + " " + ((ResourceUnauthorizedProblem) e).getDetail());
                    }
                });
            } else {
                logger.info("keywordsTweets - Tweet Text: " + tweetsResult);
                boolean firstPost = true;
                if (tweetsResult.getData() != null) {
                    logger.info(tweetsResult.getData().size() + " tweets read for the keywords");
                    for (Tweet v2Tweet : Objects.requireNonNull(tweetsResult.getData())) {
                        try {
                            JSONObject v1FormattedJson = TweetV2ToV1Converter.convertV2ToV1Format(v2Tweet, tweetsResult.getIncludes());
                            if (logger.isDebugEnabled())
                                logger.debug("V1 formatted V2 data::" + v1FormattedJson);
                            postStatus(TweetV2ToV1Converter.getStatus(v1FormattedJson));
                        } catch (Exception e) {
                            logger.error("Error while converting the twitter V2 data to V1 format", e);
                        }
                    }
                } else {
                    logger.info("No latest tweets found for the keywords");
                }
            }
        } catch(ApiException e) {
            logger.error("Error Status code: " + e.getCode() + ". Reason: " + e.getResponseBody() + ". Response headers: " +
                    e.getResponseHeaders());
            logger.error("Exception", e);
        } finally {
            userNamesToFetch.put(KEYWORDS_KEY_STR, System.currentTimeMillis());
        }
    }

    private void postStatus(Status status) {
        try {
            if (logger.isDebugEnabled()) logger.debug(status.getUser().getName() + " : " + status.getText());
            Struct keyStruct = new Struct(StatusConverter.STATUS_SCHEMA_KEY);
            Struct valueStruct = new Struct(StatusConverter.STATUS_SCHEMA);

            StatusConverter.convertKey(status, keyStruct);
            logger.info("Value of valueStruct::" + valueStruct);
            StatusConverter.convert(status, valueStruct);

            Map<String, ?> sourcePartition = ImmutableMap.of();
            Map<String, ?> sourceOffset = ImmutableMap.of();

            SourceRecord record = new SourceRecord(sourcePartition, sourceOffset, config.topic,
                    StatusConverter.STATUS_SCHEMA_KEY, keyStruct, StatusConverter.STATUS_SCHEMA, valueStruct);
            messageQueue.add(record);
        } catch (Exception ex) {
            if (logger.isErrorEnabled()) {
                logger.error("Exception thrown", ex);
            }
        }
    }

    private static void initFields() {
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
        // tweetFields.add("non_public_metrics"); TODO Not supported in Basic account setup
        tweetFields.add("public_metrics");
        // tweetFields.add("organic_metrics"); TODO Not supported in Basic account setup
        // tweetFields.add("promoted_metrics"); TODO Not supported in Basic account setup
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
        // mediaFields.add("non_public_metrics"); TODO Not supported in Basic account setup
        // mediaFields.add("organic_metrics"); TODO Not supported in Basic account setup
        // mediaFields.add("promoted_metrics"); TODO Not supported in Basic account setup
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
