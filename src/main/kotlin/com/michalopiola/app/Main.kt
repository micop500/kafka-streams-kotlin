package com.michalopiola.app

import com.michalopiola.app.kafka.StreamsProcessor

fun main() {
    StreamsProcessor("localhost:9021", "http://localhost:8081").process()
}