# twitter-v2Tov1-kafka-source-connector
Twitter V2 To V1 Source Connector for Kafka

This is based on Twitter V1 Kafka Source Connector provided by jcustenborder (https://github.com/jcustenborder/kafka-connect-twitter).
This connector fetches the tweets using Twitter V2 APIs and converts to twitter4j format. This is mainly developed for those who had used the earlier connector provided by jcustenbrder which was based on twitter4j.
This will make their migration work easier (from the Twitter V1 based jcustenborder's plugin to Twitter V2 APIs).

As of now, we have this connector working for the Twitter(X) basic plan where there is no streaming available. So, there is a thread which periodically fetches for the configured set of user IDs (names) and keywords, and posts the tweets onto the configured Kafka topic.

1) Checkout the code and run "mvn clean install -DskipTests -Dcheckstyle.skip" to generate the (dependencies) jar file in target folder.
2) Copy the jar file (in the target folder) to a new folder created under kafka-plugins folder (configured in connect avro properties file).
3) Once the jar is copied to the newly created folder, create an empty file name "sdk.properties" in the same folder.
Contents of the folder:

   $ ls -ltr

   total 30108

   -rw-rw-r-- 1 user1 user1        0 Jan 16 06:49 sdk.properties

   -rw-r--r-- 1 user1 user1 30830192 Jan 25 07:46 twitter-v2Tov1-kafka-source-connector-0.1.0.0-jar-with-dependencies.jar
4) (Re)Start the connect avro.

Command to create the source connector (sample):
create source connector TWITTER_V2TOV1_SOURCE_01 with (

'connector.class' = 'com.findmycarrots.kafka.connect.twitterv2.basic.TwitterV2SourceConnector',

'kafka.status.topic' = 'twitter5',

'twitterv2.bearerToken' = '####################################',

'value.converter.schemas.enable' = false,

'key.converter.schemas.enable' = false,

'filter.keywords' = 'india out,maldives',

'filter.userIds' = 'ndtv,timesofindia,AuliUttarakhand,imVkohli,msdhoni',

'twitterv2.refreshInterval' = '3600',

'kafka.batch.maxSize' = '10',

'kafka.batch.maxIntervalMs' = '1000',

'key.converter' = 'io.confluent.connect.avro.AvroConverter',

'key.converter.schema.registry.url' = 'http://192.168.1.146:8081',

'value.converter' = 'io.confluent.connect.avro.AvroConverter',

'value.converter.schema.registry.url' = 'http://192.168.1.146:8081',

'tasks.max' = '1'

);

## TODO:

1. Streaming support to be added (Twitter pro users).
2. Geo (place) details to be integrated.
3. To be thoroughly tested!


Feel free to provide your comments to [sivaramakrishnan.nageswaran@findmycarrots.com]()
