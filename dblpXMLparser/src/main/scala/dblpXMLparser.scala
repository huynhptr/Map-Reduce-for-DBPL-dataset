import java.io.{BufferedWriter, File, FileWriter}

import com.typesafe.config.ConfigFactory
import org.slf4j.LoggerFactory

import scala.collection.mutable.ArrayBuffer
import scala.xml.{Node, XML}
import scala.xml.Utility.trim
import java.util.concurrent.{Callable, Executors, Future, TimeUnit}

object dblpXMLparser {
  def main(args: Array[String]): Unit = {
    //increase limit for parsing large files
    System.setProperty("entityExpansionLimit", String.valueOf(Integer.MAX_VALUE))

    //create a logger
    val logger = LoggerFactory.getLogger("XMLparser")

    //load configuration from config file
    logger.info("Loading config file...")
    val config = ConfigFactory.parseResources("application.conf")

    logger.info("Loading XML input file...")
    val dblp = XML.loadFile(config.getString("paths.input"))

    logger.info("Processing file...")

    //var children = dblp.child
    var children = dblp.child.splitAt(7900004/*continue here*/)._2
    val childrenNum = children.length

    var bwl = new BufferedWriter(new FileWriter(new File(config.getString("paths.logFile")), true))
    bwl.write("Number of publications: " + childrenNum + "\n")
    bwl.close()


    val numSplits = 800
    val numThread = 8
    val splitLength : Int = childrenNum / numSplits

    bwl = new BufferedWriter(new FileWriter(new File(config.getString("paths.logFile")), true))
    bwl.write("Number of publications per split: " + splitLength)
    bwl.close()

    //val remainLength = childrenNum % (splitLength*numSplits)
    val fuList = new ArrayBuffer[Future[String]]
    val executorService = Executors.newFixedThreadPool(numThread)

    for(i <- 1 to numSplits){
      val split = children.slice(0,splitLength)
      val future = executorService.submit(new xmlProcessingJob(split))
      fuList.addOne(future)

      children = children.splitAt(splitLength)._2
    }

    val split = children.slice(splitLength*numSplits,childrenNum)
    val future = executorService.submit(new xmlProcessingJob(split))
    fuList.addOne(future)

    bwl = new BufferedWriter(new FileWriter(new File(config.getString("paths.logFile")), true))
    bwl.write("Last split is assigned to a thread.")
    bwl.close()

    for((item,idx) <- fuList.zipWithIndex){
      var ret = ""
      try {
        ret = item.get()
      } catch {
        case e: Exception =>
          // interrupts if there is any possible error
          item.cancel(true)
      }

      val bw = new BufferedWriter(new FileWriter(new File(config.getString("paths.output")), true))
      bw.write(ret)
      bw.close

      bwl = new BufferedWriter(new FileWriter(new File(config.getString("paths.logFile")), true))
      bwl.write("Split " + idx + " is finished!")
      bwl.close()
    }

    executorService.shutdown
    executorService.awaitTermination(1, TimeUnit.SECONDS)
  }


  class xmlProcessingJob(var input: Seq[Node]) extends Callable[String] {

    var outputText = ""
    var counter = 0
    @throws[Exception]
    override def call() : String = {
      for(child <- input){
        outputText += trim(child).toString() + "\n"
        println(Thread.currentThread().getName + " count " + counter)
        counter += 1
      }

      outputText
    }
  }

}