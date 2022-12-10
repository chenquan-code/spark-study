package com.cq.spark.rpc_01_akka_rpc

/**
 * TODO 马中华 https://blog.csdn.net/zhongqi2513
 *  注释：
 *  1、spark rpc 生命周期方法： onStart receive onStop
 *  2、akka  rpc 生命周期方法： preStart receive postStop()
 */
class SparkWorker(val sparkWorkerHostname: String, val sparkMasterHostname: String, val sparkMasterPort: Int,
    val memory: Int, val cpu: Int) extends Actor {
    
    var sparkWorkerid: String = sparkWorkerHostname
    var sparkMasterRef: ActorSelection = _
    
    // TODO 注释： 会提前执行一次
    // TODO 注释： 当前NM启动好了之后，就应该给 RM 发送一个注册消息
    // TODO 注释： 发给谁，就需要获取这个谁的一个ref实例
    override def preStart(): Unit = {
        
        // TODO 注释： 获取消息发送对象的一个ref实例
        // 远程path:    akka.tcp://（ActorSystem的名称）@（远程地址的IP）:（远程地址的端口）/user/（Actor的名称）
        sparkMasterRef = context.actorSelection(s"akka.tcp://${
            Constant.SMAS
        }@${sparkMasterHostname}:${sparkMasterPort}/user/${Constant.SMA}")
        
        // TODO 注释： 发送消息
        println(sparkWorkerid + " 正在注册")
        sparkMasterRef ! RegisterSparkWorker(sparkWorkerid, memory, cpu)
    }
    
    // TODO 注释： 正常服务方法
    override def receive: Receive = {
        
        // TODO 注释： 接收到注册成功的消息
        case RegisteredSparkWorker(masterURL) => {
            println(masterURL);
            
            // TODO 注释： initialDelay: FiniteDuration, 多久以后开始执行
            // TODO 注释： interval:     FiniteDuration, 每隔多长时间执行一次
            // TODO 注释： receiver:     ActorRef, 给谁发送这个消息
            // TODO 注释： message:      Any  发送的消息是啥
            context.system.scheduler.schedule(0 millis, 4000 millis, self, SendMessage)
        }
        
        // TODO 注释： 发送心跳
        case SendMessage => {
            // TODO 注释： 向主节点发送心跳信息
            sparkMasterRef ! Heartbeat(sparkWorkerid)
            println(Thread.currentThread().getId)
        }
    }
}

/** ***********************************************
 * TODO 马中华 https://blog.csdn.net/zhongqi2513
 *  注释： 启动类
 *  运行的时候，需要制定参数:
 *  localhost localhost 6789 64 32 9911 bigdata01
 */
object SparkWorker {
    def main(args: Array[String]): Unit = {
        
        // TODO 注释： 远程主机名称
        val HOSTNAME = args(0)
        
        // TODO 注释：  SparkMaster 的 hostname 和 port
        val SPARK_MASTER_HOSTNAME = args(1)
        val SPARK_MASTER_PORT = args(2).toInt
        
        // TODO 注释：  抽象的内存资源 和 CPU 个数
        val SPARK_WORKER_MEMORY = args(3).toInt
        val SPARK_WORKER_CORE = args(4).toInt
        
        // TODO 注释：  当前 SparkWorker 的 hostname 和 port
        var SPARK_WORKER_PORT = args(5).toInt
        var SPARK_WORKER_HOSTNAME = args(6)
        
        // TODO 注释：  指定主机名称和端口号相关的配置
        val str =
            s"""
               |akka.actor.provider = "akka.remote.RemoteActorRefProvider"
               |akka.remote.netty.tcp.hostname = ${HOSTNAME}
               |akka.remote.netty.tcp.port = ${SPARK_WORKER_PORT}
            """.stripMargin
        val conf = ConfigFactory.parseString(str)
        
        // TODO 注释： 启动一个 ActorSystem
        val actorSystem = ActorSystem(Constant.SWAS, conf)
        
        // TODO 注释： 启动一个Actor
        // TODO 注释： 这句代码执行的时候，会创建 SparkWorker 的 actor 对象
        // TODO 注释： 同时再跳转到这个 actor 的声明周期方法： preStart()
        // TODO 注释： 每个actor的内部有三个声明周期方法： preStart(), receive(), onStop()
        actorSystem.actorOf(Props(
            new SparkWorker(SPARK_WORKER_HOSTNAME, SPARK_MASTER_HOSTNAME, SPARK_MASTER_PORT, SPARK_WORKER_MEMORY,
                SPARK_WORKER_CORE)), Constant.SWA)
    }
}