# What it does

A sample project to understand kafka streams.
A kafka stream listens to a topic `twitter_topic`, consumes msgs from the topic, executes some filters and reduces the msgs, and finally publishes into another topic `imp_topics`.

# How to execute
1. Start the zookeeper
2. start the broker
3. create the topic `twitter_topic`
4. create a topic `imp_topics`
5. Run the twitter tweets producer
6. Run this app

# Validate
In the `imp_topics` the msgs with more foller_count will br published.



