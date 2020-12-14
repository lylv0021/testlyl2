package com.atguigu.spkStr

import java.util.Properties
import java.util.concurrent.Future

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord, RecordMetadata}

import scala.collection.mutable.ListBuffer
import scala.util.Random

object BlackList_Producer {
  def main(args: Array[String]): Unit = {
    val prop = new Properties()
    prop.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"hadoop102:9092")
    prop.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer")
    prop.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer")
    val kakPro = new KafkaProducer[String,String](prop)
    val topic="atguigu1210"
    while (true){
      for(x<-this.getData()){
        val proRe = new ProducerRecord[String,String](topic,x)
        println(x)
        val value: Future[RecordMetadata] = kakPro.send(proRe)
      }
      Thread.sleep(2000)
    }
  }

  def getData()={
    var aeras=ListBuffer("华南","华北","华东")
    var cities=ListBuffer("北京","上海","天津")
    var list=ListBuffer[String]()
    for(x<- 1 to Random.nextInt(9)){
      var aera=aeras(Random.nextInt(3))
      var city=cities(Random.nextInt(3))
      var userid=Random.nextInt(6)
      var adid=Random.nextInt(9)
      val dataString = s"${System.currentTimeMillis()} ${aera} ${city} ${userid} ${adid}"
      list.append(dataString)
    }
    list
  }

}
