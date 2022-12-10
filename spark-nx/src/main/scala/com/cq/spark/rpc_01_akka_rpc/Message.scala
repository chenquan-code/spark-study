package com.cq.spark.rpc_01_akka_rpc

// TODO 注释： 注册消息   SparkWorker -> SparkMaster
case class RegisterSparkWorker(val sparkWorkerId: String, val memory: Int, val cpu: Int)

// TODO 注释： 注册完成消息 SparkMaster -> SparkWorker
case class RegisteredSparkWorker(val sparkMasterHostname: String)

// TODO 注释： 心跳消息  SparkWorker -> SparkMaster
case class Heartbeat(val sparkWorkerId: String)

// TODO 注释： NodeManager 信息类
class SparkWorkerInfo(val sparkWorkerId: String, val memory: Int, val cpu: Int) {
    
    // TODO 注释： 上一次心跳时间
    var lastHeartBeatTime: Long = _
    
    override def toString: String = {
        sparkWorkerId + "," + memory + "," + cpu
    }
}

// TODO 注释： 一个发送心跳的信号
case object SendMessage

// TODO 注释： 一个检查信号
case object CheckTimeOut