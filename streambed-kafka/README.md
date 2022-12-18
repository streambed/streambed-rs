# kafka

Provides commit log functionality to connect with the Kafka HTTP API, with one difference...
Kafka's HTTP API does not appear to provide a means of streaming consumer records. We have
therefore introduced a new API call to subscribe to topics and consume their events by
assuming that the server will keep the connection open.