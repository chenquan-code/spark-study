package com.cq.spark.rpc_01_akka_rpc

/*************************************************
 * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
 *  注释： 集群主节点抽象
 *  1、receive 方法        接收其他 actor 发送过来的消息，然后进行模式匹配，进行消息处理，有可能返回消息
 *  2、preStart() 方法     对象在构建成功之后，就会触发执行 preStart
 *  3、postStop 方法       在对象销毁之前，会执行一次
 *  -
 *  必须了解的知识：
 *  1、伴生类 class A 和 伴生对象 object A（定义的方法，都是静态方法）
 *  2、关于 scala 中定义的一个类的构造方法：
 *      构造器： 类名后面的括号
 *      代码实现： {} 中的一切能执行的代码
 *          变量的初始化
 *          代码块
 *          静态代码块
 *          不能执行的代码： 定义的方法（未调用， 内部类）
 */
class SparkMaster(var hostname: String, var port: Int) extends Actor {
    
    // TODO_MA 注释： 用来存储每个注册的NodeManager节点的信息
    private var id2SparkWorkerInfoMap = new mutable.HashMap[String, SparkWorkerInfo]()
    
    // TODO_MA 注释： 对所有注册的NodeManager进行去重，其实就是一个HashSet
    private var sparkWorkerInfoesSet = new mutable.HashSet[SparkWorkerInfo]()
    
    // TODO_MA 注释： actor在最开始的时候，会执行一次
    override def preStart(): Unit = {
        
        // TODO_MA 注释： 调度一个任务， 每隔五秒钟执行一次
        context.system.scheduler.schedule(0 millis, 5000 millis, self, CheckTimeOut)
    }
    
    // TODO_MA 注释： 正经服务方法
    override def receive: Receive = {
        
        // TODO_MA 注释： 接收 注册消息
        case RegisterSparkWorker(sparkWorkerId, memory, cpu) => {
            val sparkWorkerInfo = new SparkWorkerInfo(sparkWorkerId, memory, cpu)
            println(s"节点 ${sparkWorkerId} 上线")
            
            // TODO_MA 注释： 对注册的 SparkWorker 节点进行存储管理
            id2SparkWorkerInfoMap.put(sparkWorkerId, sparkWorkerInfo)
            sparkWorkerInfoesSet += sparkWorkerInfo
            
            // TODO_MA 注释： 把信息存到 zookeeper
            // TODO_MA 注释： sender() 谁给我发消息，sender方法返回的就是谁
            sender() ! RegisteredSparkWorker(hostname + ":" + port)
        }
        
            // TODO_MA 注释： 接收心跳消息
        case Heartbeat(sparkWorkerId) => {
            val currentTime = System.currentTimeMillis()
            val sparkWorkerInfo = id2SparkWorkerInfoMap(sparkWorkerId)
            sparkWorkerInfo.lastHeartBeatTime = currentTime
    
            id2SparkWorkerInfoMap(sparkWorkerId) = sparkWorkerInfo
            sparkWorkerInfoesSet += sparkWorkerInfo
        }
        
        // TODO_MA 注释： 检查过期失效的 NodeManager
        case CheckTimeOut => {
            val currentTime = System.currentTimeMillis()
            
            // TODO_MA 注释： 15 秒钟失效
            sparkWorkerInfoesSet.filter(sparkWorker => {
                val heartbeatTimeout = 15000
                val bool = currentTime - sparkWorker.lastHeartBeatTime > heartbeatTimeout
                if (bool) {
                    println(s"节点 ${sparkWorker.sparkWorkerId} 下线")
                }
                bool
            }).foreach(deadSparkWorker => {
                sparkWorkerInfoesSet -= deadSparkWorker
                id2SparkWorkerInfoMap.remove(deadSparkWorker.sparkWorkerId)
            })
            println("当前注册成功的节点数" + sparkWorkerInfoesSet.size + "\t分别是：" + sparkWorkerInfoesSet.map(x => x.toString)
              .mkString(","));
        }
    }
}

/*************************************************
 * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
 *  注释： 启动入口， 伴生对象！
 *  akka scala 语言的网络编程库
 *  -
 *  当前这个 RPC 系统的三个时间：
 *  1、心跳时间：3s
 *  2、检查时间：5s
 *  3、超时时间：15s
 */
object SparkMaster {
    def main(args: Array[String]): Unit = {
        
        // TODO_MA 注释： 地址参数
        val str =
        s"""
           |akka.actor.provider = "akka.remote.RemoteActorRefProvider"
           |akka.remote.netty.tcp.hostname = localhost
           |akka.remote.netty.tcp.port = 6789
      """.stripMargin
        val conf = ConfigFactory.parseString(str)
        
        // TODO_MA 注释：ActorSystem
        val actorSystem = ActorSystem(Constant.SMAS, conf)
        
        // TODO_MA 注释：启动了一个actor ： SparkMaster
        actorSystem.actorOf(Props(new SparkMaster("localhost", 6789)), Constant.SMA)
        
        /**
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： actor 的生命周期
         *  1、SparkMaster actor 的构造方法
         *  2、preStart()  当 actor 实例创建成功的时候，就会马上调用这个 actor 的 preStart() 来执行
         */
    }
}
