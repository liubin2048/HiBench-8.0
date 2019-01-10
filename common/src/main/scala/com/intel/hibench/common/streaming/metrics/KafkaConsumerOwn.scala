/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.intel.hibench.common.streaming.metrics

import java.util
import java.util.{Collections, Properties}

import kafka.api.{FetchRequestBuilder, OffsetRequest}
import kafka.common.ErrorMapping._
import kafka.common.TopicAndPartition
import kafka.consumer.{ConsumerConfig, SimpleConsumer}
import kafka.message.MessageAndOffset
import kafka.utils.ZkUtils
import org.apache.kafka.clients.admin.KafkaAdminClient
import org.apache.kafka.clients.consumer.{ConsumerRecord, KafkaConsumer}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.utils.Utils

class KafkaConsumerOwn(bootstrapServers: String, topic: String, partition: Int) {

  private val CLIENT_ID = "metrics_reader"

  private val props = new Properties()
  props.put("key.deserializer", classOf[StringDeserializer])
  props.put("value.deserializer", classOf[StringDeserializer])
  props.put("group.id", CLIENT_ID)
  props.put("bootstrap.servers", bootstrapServers)
  props.put("enable.auto.commit", (true: java.lang.Boolean))
  props.put("auto.offset.reset", "latest")

  private val consumer = createConsumer

  val tp = new TopicPartition(topic, partition)
  private val earliestOffset = consumer.beginningOffsets(util.Arrays.asList(tp)).get(tp)


//    .earliestOrLatestOffset(TopicAndPartition(topic, partition), OffsetRequest.EarliestTime, -1)
  private var nextOffset: Long = earliestOffset

//  private var iterator: Iterator[MessageAndOffset] = getIterator(nextOffset)
  private var iterator: util.Iterator[ConsumerRecord[String, String]] = getIterator(nextOffset)

  def next(): Array[Byte] = {
    val record = iterator.next()
    nextOffset = record.offset()+1
    record.value().getBytes
  }

//  def next(): Array[Byte] = {
//    val mo = iterator.next()
//    val message = mo.message
//
//    nextOffset = mo.nextOffset
//
//    Utils.readBytes(message.payload)
//  }

  def hasNext: Boolean = {
        @annotation.tailrec
        def hasNextHelper(iter: util.Iterator[ConsumerRecord[String, String]], newIterator: Boolean): Boolean = {
          if (iter.hasNext) true
          else if (newIterator) false
          else {
            iterator = getIterator(nextOffset)
            hasNextHelper(iterator, newIterator = true)
          }
        }

        hasNextHelper(iterator, newIterator = false)
  }

//  def hasNext: Boolean = {
//    @annotation.tailrec
//    def hasNextHelper(iter: Iterator[MessageAndOffset], newIterator: Boolean): Boolean = {
//      if (iter.hasNext) true
//      else if (newIterator) false
//      else {
//        iterator = getIterator(nextOffset)
//        hasNextHelper(iterator, newIterator = true)
//      }
//    }
//
//    hasNextHelper(iterator, newIterator = false)
//  }

  def close(): Unit = {
    consumer.close()
  }

  private def createConsumer: KafkaConsumer[String, String] = {
    val kafkaComsumer = new KafkaConsumer[String, String](props)
    kafkaComsumer.subscribe(Collections.singleton(topic))
    kafkaComsumer
  }

  private def getIterator(offset: Long): util.Iterator[ConsumerRecord[String, String]] = {
    consumer.seek(tp, offset)
    val records = consumer.poll(6000)
    records.iterator()
  }

//  private def getIterator(offset: Long): Iterator[MessageAndOffset] = {
//
//    val request = new FetchRequestBuilder()
//      .addFetch(topic, partition, offset, config.fetchMessageMaxBytes)
//      .build()
//
//    val response = consumer.fetch(request)
//    response.errorCode(topic, partition) match {
//      case NoError => response.messageSet(topic, partition).iterator
//      case error => throw exceptionFor(error)
//    }
//  }
}
