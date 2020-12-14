package com.atguigu.spkStr

import java.sql.{Connection, PreparedStatement, ResultSet}
import java.text.SimpleDateFormat

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable.ListBuffer

object BlackList_Consumer {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("testblc")
    val scc = new StreamingContext(sparkConf,Seconds(3))
    val kafkaPara: Map[String, Object] = Map[String, Object](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "hadoop102:9092,hadoop103:9092,hadoop104:9092",
      ConsumerConfig.GROUP_ID_CONFIG -> "atguigu",
      "key.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
      "value.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer"
    )

    //4.读取Kafka数据创建DStream
    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](scc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](Set("atguigu1210"), kafkaPara))
    val ds: DStream[String] = kafkaDStream.map(
      t => {
        t.value()
      }
    )
    //TODO 对黑名单周期性遍历
    //如果此批次的数据已经在黑名单中，就将其过滤掉，
    // 如果没有在黑名单中，再对数据进行进一步处理
    val ds2= ds.transform(
      rdd => {
        val blc_List = ListBuffer[String]()
        val conn: Connection = JdbcUtil.getConnect()
        val state: PreparedStatement = conn.prepareStatement("select userid  from black_list")
        val rs: ResultSet = state.executeQuery()
        while (rs.next()) {
          blc_List.append(rs.getString(1))
        }
        rs.close()
        state.close()
        conn.close()

        val afterFRdd: RDD[String] = rdd.filter(
          line => {
            val datas: Array[String] = line.split(" ")
            var userid = datas(3)
            !blc_List.contains(userid)
          }

        )

        val reduRDD: RDD[((String, String, String), Int)] = afterFRdd.map(
          line => {
            val datas: Array[String] = line.split(" ")
            val dForm = new SimpleDateFormat("yyyy-MM-dd")
            //  val day = sdf.format(new java.util.Date(datas(0).toLong))
            val day = dForm.format(new java.util.Date(datas(0).toLong))
            val user = datas(3)
            val ad = datas(4)
            ((day, user, ad), 1)
          }
        ).reduceByKey(_ + _)
        reduRDD
      }
    )
    ds2.print()
    //TODO 对同一批数据的信息进行筛选
    ds2.foreachRDD(
      rdd=>{
        rdd.foreach{
          case ((day,user,ad),count)=>{
            if(count>20){
              //同一批次的数据
              // 如果总点击量
              // 超出了阈值，则直接拉入黑名单
              var conn1=JdbcUtil.getConnect()
              val state1: PreparedStatement = conn1.prepareStatement(
                """
                  |insert into black_list(userid)  values(?)
                  |ON DUPLICATE KEY
                  |UPDATE userid=?
                """.stripMargin)
              state1.setString(1,user)
              state1.setString(2,user)
            state1.executeUpdate()
              state1.close()
              conn1.close()
            }else{//如果没有超出阈值，
              val conn2: Connection = JdbcUtil.getConnect()
              val state2: PreparedStatement = conn2.prepareStatement("select count  from user_ad_count where dt=? and userid=? and adid=?")
              state2.setString(1,day)
              state2.setString(2,user)
              state2.setString(3,ad)
              val rs2 = state2.executeQuery()
              if(rs2.next()){// 就判断原表中该数据是否已经存在，
                //如果存在
                // 就将该数据的点击数量取出，
                if((count+rs2.getLong(1))>20){ // 如果该值与新增的数量的和超过阈值，则拉入黑名单
                  var conn4=JdbcUtil.getConnect()
                  val state4: PreparedStatement = conn4.prepareStatement(
                    """insert into black_list(userid)  values(?)
                      |ON DUPLICATE KEY
                      |UPDATE userid=?
                    """.stripMargin)
                  state4.setString(1,user)
                  state4.setString(2,user)
                  state4.executeUpdate()
                  state4.close()
                  conn4.close()
                }else{// 如果没有超过阈值，就正常更新对应的数据

                  val conn5: Connection = JdbcUtil.getConnect()
                  val state5: PreparedStatement = conn5.prepareStatement(
                    """
                      |update user_ad_count
                      |set count=?
                      |where dt =? and userid =? and adid =?
                    """.stripMargin)
                  var coNum=count+rs2.getLong(1)
                  state5.setLong(1,coNum)
                  state5.setString(2,day)
                  state5.setString(3,user)
                  state5.setString(4,ad)
                  state5.executeUpdate()
                  state5.close()
                  conn5.close()
                }
              }else{
                // 如果不存在
                // 将该数据正常插入表中
                val conn6: Connection = JdbcUtil.getConnect()
                val state6  = conn6.prepareStatement(
                  """
                    |insert into
                    |user_ad_count(dt,userid,adid,count)
                    |values(?,?,?,?)
                  """.stripMargin)
                state6.setString(1,day)
                state6.setString(2,user)
                state6.setString(3,ad)
                state6.setLong(4,count)
                state6.executeUpdate()
                state6.close()
                conn6.close()
              }
              rs2.close()
              state2.close()
              conn2.close()
            }
          }
        }
      }
    )


    scc.start()
    scc.awaitTermination()
  }

}
