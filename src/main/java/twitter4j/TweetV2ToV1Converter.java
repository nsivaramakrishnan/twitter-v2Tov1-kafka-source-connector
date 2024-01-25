package twitter4j;

import com.twitter.clientlib.model.HashtagEntity;
import com.twitter.clientlib.model.Place;
import com.twitter.clientlib.model.User;
import com.twitter.clientlib.model.*;

import java.time.format.DateTimeFormatter;
import java.util.Objects;

public class TweetV2ToV1Converter {
    private static final DateTimeFormatter DTF = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    public static Status getStatus(JSONObject jsonObject) throws TwitterException {
        return new V2StatusJSONImpl(jsonObject);
    }

    // Changes done as per https://developer.twitter.com/en/docs/twitter-api/migrate/data-formats/standard-v1-1-to-v2
    public static JSONObject convertV2ToV1Format(Tweet v2Tweet, Expansions includes) throws JSONException {
        JSONObject jsonObject = new JSONObject();
        jsonObject.put("id", v2Tweet.getId());
        jsonObject.put("source", v2Tweet.getSource());
        if (v2Tweet.getCreatedAt() != null) jsonObject.put("created_at", v2Tweet.getCreatedAt().format(DTF));
        if (v2Tweet.getReferencedTweets() != null && !v2Tweet.getReferencedTweets().isEmpty()) {
            if (v2Tweet.getReferencedTweets().get(0).getType() == TweetReferencedTweets.TypeEnum.QUOTED) {
                JSONObject statusJson = new JSONObject();
                statusJson.put("id", v2Tweet.getReferencedTweets().get(0).getId());
                jsonObject.put("quoted_status", statusJson);
                jsonObject.put("quoted_status_id", v2Tweet.getReferencedTweets().get(0).getId());
            } else if (v2Tweet.getReferencedTweets().get(0).getType() == TweetReferencedTweets.TypeEnum.REPLIED_TO) {
                jsonObject.put("in_reply_to_status_id", v2Tweet.getReferencedTweets().get(0).getId());
            } else if (v2Tweet.getReferencedTweets().get(0).getType() == TweetReferencedTweets.TypeEnum.RETWEETED) {
                JSONObject statusJson = new JSONObject();
                statusJson.put("id", v2Tweet.getReferencedTweets().get(0).getId());
                jsonObject.put("retweeted_status", statusJson);
            }
        }
        jsonObject.put("in_reply_to_user_id", v2Tweet.getInReplyToUserId());
        for (User user : Objects.requireNonNull(includes.getUsers())) {
            if (user.getId().equals(v2Tweet.getInReplyToUserId())) {
                jsonObject.put("in_reply_to_screen_name", user.getUsername());
                // populateUserData(jsonObject, user);
                break;
            }
        }
        for (User user : Objects.requireNonNull(includes.getUsers())) {
            if (user.getId().equals(v2Tweet.getAuthorId())) {
                populateUserData(jsonObject, user);
                break;
            }
        }

        populateEntities(jsonObject, v2Tweet, includes);
        populateExtendedEntities(jsonObject, v2Tweet, includes);
        if (v2Tweet.getPublicMetrics() != null) {
            jsonObject.put("retweet_count", v2Tweet.getPublicMetrics().getRetweetCount());
            jsonObject.put("favorite_count", v2Tweet.getPublicMetrics().getLikeCount());
        }
        jsonObject.put("possibly_sensitive", v2Tweet.getPossiblySensitive());
        jsonObject.put("text", v2Tweet.getText());
        jsonObject.put("full_text", v2Tweet.getText());
        jsonObject.put("lang", v2Tweet.getLang());
        populateLocation(jsonObject, v2Tweet, includes);
        return jsonObject;
    }

    private static void populateLocation(JSONObject jsonObject, Tweet v2Tweet, Expansions includes) throws JSONException {
        if (v2Tweet.getGeo() == null) return;
        String placeId = Objects.requireNonNull(v2Tweet.getGeo()).getPlaceId();
        if (placeId == null) return;
        JSONObject placeJson = new JSONObject();
        for (Place place : Objects.requireNonNull(includes.getPlaces())) {
            if (place.getId().equals(placeId)) {
                // Place place = includes.getPlaces().get(0);
                /*placeJson.put("name", place.getName());
                placeJson.put("id", place.getId());
                placeJson.put("country_code", place.getCountryCode());
                placeJson.put("country", place.getCountry());
                placeJson.put("place_type", Objects.requireNonNull(place.getPlaceType()).toString());
                placeJson.put("full_name", place.getFullName());*/
                populatePlace(place, placeJson);
                /*if (place.getContainedWithin() != null) {
                    placeJson.put("contained_within", place.getContainedWithin()); // TODO will be taken up later for handling all possible cases
                }
                JSONObject boundingBoxJson = new JSONObject();
                boundingBoxJson.put("type", Objects.requireNonNull(place.getGeo()).getType().toString());
                place.getGeo().getBbox(); // TODO
                if (boundingBoxJson.length() > 0)
                    placeJson.put("bounding_box", boundingBoxJson);*/
                break;
            }
        }
        if (placeJson.length() > 0) {
            jsonObject.put("place", placeJson);
        }
    }

    private static void populatePlace(Place place, JSONObject placeJson) throws JSONException {
        placeJson.put("name", place.getName());
        placeJson.put("id", place.getId());
        placeJson.put("country_code", place.getCountryCode());
        placeJson.put("country", place.getCountry());
        placeJson.put("place_type", Objects.requireNonNull(place.getPlaceType()).toString());
        placeJson.put("full_name", place.getFullName());
    }
    private static void populateExtendedEntities(JSONObject jsonObject, Tweet v2Tweet, Expansions includes) throws JSONException {
        JSONObject extendedEntitiesJson = new JSONObject();

        if (v2Tweet.getAttachments() != null && v2Tweet.getAttachments().getMediaKeys() != null) {
            JSONArray mediaEntitiesJsonArray = new JSONArray();
            // TODO
            for (String mediaKey : Objects.requireNonNull(Objects.requireNonNull(v2Tweet.getAttachments()).getMediaKeys())) {
                if (mediaKey == null) continue;
                for (Media media : Objects.requireNonNull(includes.getMedia())) {
                    if (Objects.equals(media.getMediaKey(), mediaKey)) {
                        JSONObject mediaEntityJson = new JSONObject();
                        mediaEntityJson.put("id", media.getMediaKey());
                        mediaEntityJson.put("type", media.getType());
                        for (UrlEntity urlEntity : Objects.requireNonNull(Objects.requireNonNull(v2Tweet.getEntities()).getUrls())) {
                            if (mediaKey.equals(urlEntity.getMediaKey())) {
                                JSONArray indicesJsonArray = new JSONArray();
                                indicesJsonArray.put(urlEntity.getStart());
                                indicesJsonArray.put(urlEntity.getEnd());
                                mediaEntityJson.put("indices", indicesJsonArray);
                                if (media.getType().equals("video")) {
                                    mediaEntityJson.put("url", urlEntity.getExpandedUrl());
                                    mediaEntityJson.put("expanded_url", urlEntity.getExpandedUrl());
                                    mediaEntityJson.put("display_url", urlEntity.getExpandedUrl());
                                    mediaEntityJson.put("media_url", urlEntity.getExpandedUrl());
                                    mediaEntityJson.put("media_url_https", urlEntity.getExpandedUrl());
                                }
                                break;
                            }
                        }
                        if (media.getType().equals("photo")) {
                            Photo photo = (Photo) media;
                            mediaEntityJson.put("url", photo.getUrl());
                            mediaEntityJson.put("expanded_url", photo.getUrl());
                            mediaEntityJson.put("display_url", photo.getUrl());
                            mediaEntityJson.put("media_url", photo.getUrl());
                            mediaEntityJson.put("media_url_https", photo.getUrl());
                            JSONObject sizesJson = new JSONObject();
                            {
                                JSONObject largeSizeJson = new JSONObject();
                                largeSizeJson.put("w", photo.getWidth());
                                largeSizeJson.put("h", photo.getHeight());
                                largeSizeJson.put("resize", MediaEntity.Size.FIT);
                                sizesJson.put("large", largeSizeJson);
                            }
                            {
                                JSONObject largeSizeJson = new JSONObject();
                                largeSizeJson.put("w", photo.getWidth());
                                largeSizeJson.put("h", photo.getHeight());
                                largeSizeJson.put("resize", MediaEntity.Size.FIT);
                                sizesJson.put("medium", largeSizeJson);
                            }
                            {
                                JSONObject largeSizeJson = new JSONObject();
                                largeSizeJson.put("w", photo.getWidth());
                                largeSizeJson.put("h", photo.getHeight());
                                largeSizeJson.put("resize", MediaEntity.Size.FIT);
                                sizesJson.put("small", largeSizeJson);
                            }
                            {
                                JSONObject largeSizeJson = new JSONObject();
                                largeSizeJson.put("w", photo.getWidth());
                                largeSizeJson.put("h", photo.getHeight());
                                largeSizeJson.put("resize", MediaEntity.Size.FIT);
                                sizesJson.put("thumb", largeSizeJson);
                            }
                            mediaEntityJson.put("sizes", sizesJson);
                    /*} else if (media.getType().equals("video")) {
                        Video video = (Video)media;
                        JSONObject sizesJson = new JSONObject();
                        JSONObject largeSizeJson = new JSONObject();
                        largeSizeJson.put("w", photo.getWidth());
                        largeSizeJson.put("h", photo.getHeight());
                        sizesJson.put("large", largeSizeJson);
                        mediaEntityJson.put("sizes", sizesJson);*/
                        }
                        mediaEntitiesJsonArray.put(mediaEntityJson);
                        break;
                    }
                }
            }
            if (mediaEntitiesJsonArray.length() > 0) {
                extendedEntitiesJson.put("media", mediaEntitiesJsonArray);
            }
        }

        if (extendedEntitiesJson.length() > 0) {
            jsonObject.put("extended_entities", extendedEntitiesJson);
        }
    }

    private static void populateEntities(JSONObject jsonObject, Tweet v2Tweet, Expansions includes) throws JSONException {
        JSONObject entitiesJson = new JSONObject();
        if (v2Tweet.getEntities() != null) {
            if (v2Tweet.getEntities().getMentions() != null) {
                JSONArray mentionEntitiesJsonArray = new JSONArray();
                for (MentionEntity mentionEntity : Objects.requireNonNull(Objects.requireNonNull(v2Tweet.getEntities()).getMentions())) {
                    JSONObject mentionEntityJson = new JSONObject();
                    mentionEntityJson.put("id", mentionEntity.getId());
                    mentionEntityJson.put("screen_name", mentionEntity.getUsername());
                    JSONArray indicesJsonArray = new JSONArray();
                    indicesJsonArray.put(mentionEntity.getStart());
                    indicesJsonArray.put(mentionEntity.getEnd());
                    mentionEntityJson.put("indices", indicesJsonArray);
                    mentionEntitiesJsonArray.put(mentionEntityJson);
                }
                if (mentionEntitiesJsonArray.length() > 0) {
                    entitiesJson.put("user_mentions", mentionEntitiesJsonArray);
                }
            }

            if (v2Tweet.getEntities().getUrls() != null) {
                JSONArray urlEntitiesJsonArray = new JSONArray();
                for (UrlEntity urlEntity : Objects.requireNonNull(Objects.requireNonNull(v2Tweet.getEntities()).getUrls())) {
                    JSONObject urlEntityJson = new JSONObject();
                    urlEntityJson.put("url", urlEntity.getUrl());
                    urlEntityJson.put("expanded_url", urlEntity.getExpandedUrl());
                    urlEntityJson.put("display_url", urlEntity.getDisplayUrl());
                    JSONArray indicesJsonArray = new JSONArray();
                    indicesJsonArray.put(urlEntity.getStart());
                    indicesJsonArray.put(urlEntity.getEnd());
                    urlEntityJson.put("indices", indicesJsonArray);
                    urlEntitiesJsonArray.put(urlEntityJson);
                }
                if (urlEntitiesJsonArray.length() > 0) {
                    entitiesJson.put("urls", urlEntitiesJsonArray);
                }
            }

            if (v2Tweet.getEntities().getHashtags() != null) {
                JSONArray hashtagsEntitiesJsonArray = new JSONArray();
                for (HashtagEntity hashtagEntity : Objects.requireNonNull(Objects.requireNonNull(v2Tweet.getEntities()).getHashtags())) {
                    JSONObject hashtagEntityJson = new JSONObject();
                    hashtagEntityJson.put("text", hashtagEntity.getTag());
                    JSONArray indicesJsonArray = new JSONArray();
                    indicesJsonArray.put(hashtagEntity.getStart());
                    indicesJsonArray.put(hashtagEntity.getEnd());
                    hashtagEntityJson.put("indices", indicesJsonArray);
                    hashtagsEntitiesJsonArray.put(hashtagEntityJson);
                }
                if (hashtagsEntitiesJsonArray.length() > 0) {
                    entitiesJson.put("hashtags", hashtagsEntitiesJsonArray);
                }
            }

            if (v2Tweet.getEntities().getCashtags() != null) {
                JSONArray cashtagsEntitiesJsonArray = new JSONArray();
                for (CashtagEntity cashtagEntity : Objects.requireNonNull(Objects.requireNonNull(v2Tweet.getEntities()).getCashtags())) {
                    JSONObject cashtagEntityJson = new JSONObject();
                    cashtagEntityJson.put("text", cashtagEntity.getTag());
                    JSONArray indicesJsonArray = new JSONArray();
                    indicesJsonArray.put(cashtagEntity.getStart());
                    indicesJsonArray.put(cashtagEntity.getEnd());
                    cashtagEntityJson.put("indices", indicesJsonArray);
                    cashtagsEntitiesJsonArray.put(cashtagEntityJson);
                }
                if (cashtagsEntitiesJsonArray.length() > 0) {
                    entitiesJson.put("symbols", cashtagsEntitiesJsonArray);
                }
            }
        }

        if (v2Tweet.getAttachments() != null && v2Tweet.getAttachments().getMediaKeys() != null) {
            JSONArray mediaEntitiesJsonArray = new JSONArray();
            for (String mediaKey : Objects.requireNonNull(Objects.requireNonNull(v2Tweet.getAttachments()).getMediaKeys())) {
                if (mediaKey == null) continue;
                for (Media media : Objects.requireNonNull(includes.getMedia())) {
                    if (Objects.equals(media.getMediaKey(), mediaKey)) {
                        JSONObject mediaEntityJson = new JSONObject();
                        mediaEntityJson.put("id", media.getMediaKey());
                        mediaEntityJson.put("type", media.getType());
                        for (UrlEntity urlEntity : Objects.requireNonNull(Objects.requireNonNull(v2Tweet.getEntities()).getUrls())) {
                            if (mediaKey.equals(urlEntity.getMediaKey())) {
                                JSONArray indicesJsonArray = new JSONArray();
                                indicesJsonArray.put(urlEntity.getStart());
                                indicesJsonArray.put(urlEntity.getEnd());
                                mediaEntityJson.put("indices", indicesJsonArray);
                                if (media.getType().equals("video")) {
                                    mediaEntityJson.put("url", urlEntity.getExpandedUrl());
                                    mediaEntityJson.put("expanded_url", urlEntity.getExpandedUrl());
                                    mediaEntityJson.put("display_url", urlEntity.getExpandedUrl());
                                    mediaEntityJson.put("media_url", urlEntity.getExpandedUrl());
                                    mediaEntityJson.put("media_url_https", urlEntity.getExpandedUrl());
                                }
                                break;
                            }
                        }
                        if (media.getType().equals("photo")) {
                            Photo photo = (Photo) media;
                            mediaEntityJson.put("url", photo.getUrl());
                            mediaEntityJson.put("expanded_url", photo.getUrl());
                            mediaEntityJson.put("media_url", photo.getUrl());
                            mediaEntityJson.put("media_url_https", photo.getUrl());
                            mediaEntityJson.put("display_url", photo.getUrl());
                            mediaEntityJson.put("ext_alt_text", photo.getAltText());
                            JSONObject sizesJson = new JSONObject();
                            {
                                JSONObject largeSizeJson = new JSONObject();
                                largeSizeJson.put("w", photo.getWidth());
                                largeSizeJson.put("h", photo.getHeight());
                                largeSizeJson.put("resize", MediaEntity.Size.FIT);
                                sizesJson.put("large", largeSizeJson);
                            }
                            {
                                JSONObject largeSizeJson = new JSONObject();
                                largeSizeJson.put("w", photo.getWidth());
                                largeSizeJson.put("h", photo.getHeight());
                                largeSizeJson.put("resize", MediaEntity.Size.FIT);
                                sizesJson.put("medium", largeSizeJson);
                            }
                            {
                                JSONObject largeSizeJson = new JSONObject();
                                largeSizeJson.put("w", photo.getWidth());
                                largeSizeJson.put("h", photo.getHeight());
                                largeSizeJson.put("resize", MediaEntity.Size.FIT);
                                sizesJson.put("small", largeSizeJson);
                            }
                            {
                                JSONObject largeSizeJson = new JSONObject();
                                largeSizeJson.put("w", photo.getWidth());
                                largeSizeJson.put("h", photo.getHeight());
                                largeSizeJson.put("resize", MediaEntity.Size.FIT);
                                sizesJson.put("thumb", largeSizeJson);
                            }
                            mediaEntityJson.put("sizes", sizesJson);
//                        } else if (media.getType().equals("video")) {
//                            Video video = (Video)media;
//                            JSONObject sizesJson = new JSONObject();
//                            JSONObject largeSizeJson = new JSONObject();
//                            largeSizeJson.put("w", photo.getWidth());
//                            largeSizeJson.put("h", photo.getHeight());
//                            sizesJson.put("large", largeSizeJson);
//                            mediaEntityJson.put("sizes", sizesJson);
                        }
                        mediaEntitiesJsonArray.put(mediaEntityJson);
                        break;
                    }
                }
            }
            if (mediaEntitiesJsonArray.length() > 0) {
                entitiesJson.put("media", mediaEntitiesJsonArray);
            }
        }

        if (entitiesJson.length() > 0) {
            jsonObject.put("entities", entitiesJson);
        }
    }

    private static void populateUserData(JSONObject jsonObject, User user) throws JSONException {
        JSONObject userJson = new JSONObject();
        userJson.put("id", user.getId());
        userJson.put("name", user.getName());
        userJson.put("screen_name", user.getUsername());
        userJson.put("location", user.getLocation());
        userJson.put("description", user.getDescription());
        userJson.put("profile_image_url_https", user.getProfileImageUrl());
        userJson.put("url", user.getUrl());
        userJson.put("protected", user.getProtected());
        userJson.put("verified", user.getVerified());
        userJson.put("followers_count", Objects.requireNonNull(user.getPublicMetrics()).getFollowersCount());

        userJson.put("friends_count", Objects.requireNonNull(user.getPublicMetrics()).getFollowingCount());
        userJson.put("created_at", user.getCreatedAt());
        if (user.getCreatedAt() != null) userJson.put("created_at", user.getCreatedAt().format(DTF));
        userJson.put("statuses_count", Objects.requireNonNull(user.getPublicMetrics()).getTweetCount());
        userJson.put("listed_count", Objects.requireNonNull(user.getPublicMetrics()).getListedCount());

        if (user.getWithheld() != null) {
            JSONArray withheldCountries = new JSONArray();
            for (String countryCode : user.getWithheld().getCountryCodes())
                withheldCountries.put(countryCode);
            userJson.put("withheld_in_countries", withheldCountries);
        }

        if (user.getEntities() != null) {
            UserEntitiesUrl userEntitiesUrl = user.getEntities().getUrl();
            if (userEntitiesUrl != null && !Objects.requireNonNull(userEntitiesUrl.getUrls()).isEmpty()) {
                JSONArray urlEntitiesJsonArray = new JSONArray();
                for (UrlEntity urlEntity : userEntitiesUrl.getUrls()) {
                    JSONObject urlEntityJson = new JSONObject();
                    urlEntityJson.put("url", urlEntity.getUrl());
                    urlEntityJson.put("expanded_url", urlEntity.getExpandedUrl());
                    urlEntityJson.put("display_url", urlEntity.getDisplayUrl());
                    JSONArray urlIndicesJsonArray = new JSONArray();
                    urlIndicesJsonArray.put(urlEntity.getStart());
                    urlIndicesJsonArray.put(urlEntity.getEnd());
                    urlEntityJson.put("indices", urlIndicesJsonArray);
                    urlEntitiesJsonArray.put(urlEntityJson);
                }
                JSONObject urlJson = new JSONObject();
                urlJson.put("urls", urlEntitiesJsonArray);
                userJson.put("url", urlJson);
            }
        }
        jsonObject.put("user", userJson);
    }
}
