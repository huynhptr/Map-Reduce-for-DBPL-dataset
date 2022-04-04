//Standard Java imports
import java.io.IOException

import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.mapreduce.Mapper
import org.apache.hadoop.mapreduce.Reducer
import org.slf4j.LoggerFactory
import com.typesafe.config.ConfigFactory

import scala.collection.immutable.ListMap
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters._
import scala.util.Try
import scala.xml.NodeSeq

//Hadoop imports;
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.mapreduce.Job
import scala.xml.XML

object Homework2 {
  class MapTask1 extends Mapper[LongWritable, Text, Text, IntWritable]{
    val accumulator = new IntWritable(1)

    var keyForReducer = new Text() // the key consists of venue and author seperated by comma

    @throws(classOf[IOException])
    override def map(key: LongWritable, value: Text,
                     context: Mapper[LongWritable, Text, Text, IntWritable]#Context): Unit = {
      //super.map(key, value, context)
      val xml = XML.loadString(value.toString)

      //Figure out the venue for each record based on discussions on piazza
      val venue = xml.label match {
        case "article"
              => if ((xml \ "journal").text.length != 0) (xml \ "journal").text else (xml \ "booktitle").text
        case "inproceedings" | "incollection" => (xml \ "booktitle").text
        case "book" | "proceedings"
              => if ((xml \ "publisher").text.length != 0) (xml \ "publisher").text else (xml \ "booktitle").text
        case "phdthesis" | "mastersthesis" => (xml \ "school").text
        case "www" => xml.attributes("key").toString().split('/').slice(0,3).mkString("/")
        case _ => "No venue available"
      }

      var authorSeq = xml \ "author"
      //If a publication has no author, consider its editors as authors
      if(authorSeq.length == 0){
        authorSeq= xml \ "editor"
      }

      //venue and author name separated by comma as the key
      //value is one count for one publication
      for(n <- authorSeq){
        keyForReducer.set(venue + "," + n.text)
        context.write(keyForReducer,accumulator)
      }
    }
  }
  class ReduceTask1 extends Reducer[Text, IntWritable, Text, IntWritable]{

    //for each venue, there is a list of authors mapping to his/her number of publications
    var venueToAuthors: mutable.Map[String, mutable.Map[String, Int]]
                                    = mutable.Map[String, mutable.Map[String, Int]]()

    @throws(classOf[IOException])
    override def reduce(key: Text, values: java.lang.Iterable[IntWritable],
                        context: Reducer[Text, IntWritable, Text, IntWritable]#Context): Unit ={
      //super.reduce(key, values, context)
      var publishedCount = 0
      for(value <- values.asScala){
        publishedCount += value.get()
      }

      //extract venue and author name from key
      val venue = key.toString.split(',')(0)
      val authorName = key.toString.split(',')(1)

      //add to global map
      addToMap(venue, authorName, publishedCount)
    }

    //add author to map if his/her number of publications is in top ten of
    //the corresponding venue
    def addToMap(venue: String, authorName: String, publishedCount: Int): Unit ={
      if(venueToAuthors.contains(venue)){
        //if the venue does not have enough 10 authors, add him/her in with his/her publications count
        if(venueToAuthors(venue).size < 10){
          venueToAuthors(venue) += (authorName->publishedCount.toInt)
        }
        else{
          //if the venue already has 10 authors, add this author in,
          //and delete one author with least number of publications
          val minName = minAuthor(venueToAuthors(venue))
          if(publishedCount > venueToAuthors(venue)(minName)){
            venueToAuthors(venue) -= minName
            venueToAuthors(venue) += (authorName->publishedCount)
          }
        }
      }
      else{//if the venue is not already in the global map, create one, and add this author in
        val newAuthorsList = mutable.Map[String, Int](authorName->publishedCount)
        venueToAuthors += (venue -> newAuthorsList)
      }
    }

    //get the name of the author with least publications in the map
    def minAuthor(authorsMap : mutable.Map[String, Int]) : String = {
      var name = ""
      var min = Int.MaxValue

      for((k,v) <- authorsMap){
        if(v < min){
          min = v
          name = k
        }
      }
      name
    }

    @throws[IOException]
    @throws[InterruptedException]
    //loop through the global map to output the venue, its authors and their corresponding publications count
    override def cleanup(context: Reducer[Text, IntWritable, Text, IntWritable]#Context): Unit = {
      super.cleanup(context)
      for((venue,auMap) <- venueToAuthors){
        val sorted = ListMap(auMap.toSeq.sortWith(_._2 > _._2):_*) //sort by publications count
        for(tup <- sorted){
          if (venue != "" && tup._1 != ""){//filter out garbage based on venue assumption
            //output key as 2 column: venue and author
            //output value is publications count for the author
            context.write(new Text(venue + "," + tup._1),new IntWritable(tup._2))
          }
        }
      }
    }
  }

  class MapTask2 extends Mapper[LongWritable, Text, Text, IntWritable]{

    val valueForReducer = new IntWritable()  // year of publication
    var keyForReducer = new Text() // author name

    @throws(classOf[IOException])
    override def map(key: LongWritable, value: Text,
                     context: Mapper[LongWritable, Text, Text, IntWritable]#Context): Unit = {
      //super.map(key, value, context)
      val xml = XML.loadString(value.toString)

      var authorSeq = xml \ "author"
      //If a publication has no author, consider its editors as authors
      if(authorSeq.length == 0){
        authorSeq= xml \ "editor"
      }

      val yearEle = xml \ "year"
      if(yearEle.length != 1){
        return //skip ambiguous record has 0, or more than 1 year element
      }
      val year = yearEle.text.toInt

      //output author name and year of this publication to reducer
      for(author <- authorSeq){
        keyForReducer.set(author.text)
        valueForReducer.set(year)
        context.write(keyForReducer,valueForReducer)
      }
    }
  }

  class ReduceTask2 extends Reducer[Text, IntWritable, Text, IntWritable]{
    //Map the name of authors to his/her number of years of the longest continuous publication
    var authorToYears : mutable.Map[String,Int] = mutable.Map[String,Int]()

    @throws(classOf[IOException])
    override def reduce(key: Text, values: java.lang.Iterable[IntWritable],
                        context: Reducer[Text, IntWritable, Text, IntWritable]#Context): Unit ={
      //super.reduce(key, values, context)

      //convert to Int array
      var array = ArrayBuffer[Int]()
      for(value <- values.asScala){
        array += value.get
      }

      val authorName = key.toString

      val continuousYears = yearsOfLongestContinuum(array)

      //only take authors that have published more than 10 continuous years
      if(continuousYears >= 10){
        if (authorName != ""){//filter out garbage based on venue assumption
          context.write(new Text(authorName), new IntWritable(continuousYears))
        }
      }
    }

    //find the longest continuous segment of the sorted collection(list of years)
    def yearsOfLongestContinuum(yearsList : ArrayBuffer[Int]) : Int = {
      val sorted= yearsList.sorted
      val size = sorted.size

      var longest = 0
      var count = 0

      // find the longest length by traversing the array
      for( i <- 0 until size){
        // if the current element is equal
        // to previous element +1
        if (i > 0 && (sorted(i) == sorted(i-1)+1)){
          count += 1
        }
        else { // reset the count
          count = 1
        }
        // update the longest
        longest = if (longest > count) longest else count
      }
      longest
    }
  }

  class MapTask3 extends Mapper[LongWritable, Text, Text, Text]{

    @throws(classOf[IOException])
    override def map(key: LongWritable, value: Text,
                     context: Mapper[LongWritable, Text, Text, Text]#Context): Unit = {
      //super.map(key, value, context)
      val xml = XML.loadString(value.toString)

      //Figure out the venue for each record based on discussions on piazza
      val venue = xml.label match {
        case "article"
              => if ((xml \ "journal").text.length != 0) (xml \ "journal").text else (xml \ "booktitle").text
        case "inproceedings" | "incollection" => (xml \ "booktitle").text
        case "book" | "proceedings"
              => if ((xml \ "publisher").text.length != 0) (xml \ "publisher").text else (xml \ "booktitle").text
        case "phdthesis" | "mastersthesis" => (xml \ "school").text
        case "www" => xml.attributes("key").toString().split('/').slice(0,3).mkString("/")
        case _ => "No venue available"
      }

      var authorSeq = xml \ "author"
      //If a publication has no author, consider its editors as authors
      if(authorSeq.length == 0){
        authorSeq= xml \ "editor"
      }

      //take this publication if it only has 1 author
      if(authorSeq.length == 1){
        //output venue and title of this publication
        context.write(new Text(venue), new Text((xml \ "title").text))
      }
    }
  }
  class ReduceTask3 extends Reducer[Text, Text, Text, Text]{
    var venueToAuthors: mutable.Map[String, mutable.Map[String, Int]]
    = mutable.Map[String, mutable.Map[String, Int]]()

    @throws(classOf[IOException])
    override def reduce(key: Text, values: java.lang.Iterable[Text],
                        context: Reducer[Text, Text, Text, Text]#Context): Unit ={
      //super.reduce(key, values, context)

      var titles = ""
      //concatenate publications titles separated by '|' as one long string
      for(value <- values.asScala){
        titles += "|" + value.toString
      }

      if (key.toString!="" && titles!=""){//filter out garbage based on venue assumption
        context.write(key, new Text(titles))
      }
    }
  }
  class MapTask4 extends Mapper[LongWritable, Text, Text, IntWritable]{

    //for each venue, there is a list of publications with its number of authors
    var venueToTitles: mutable.Map[String, mutable.Map[String, Int]]
                              = mutable.Map[String, mutable.Map[String,Int]]()

    @throws(classOf[IOException])
    override def map(key: LongWritable, value: Text,
                     context: Mapper[LongWritable, Text, Text, IntWritable]#Context): Unit = {
      //super.map(key, value, context)
      val xml = XML.loadString(value.toString)

      //Figure out the venue for each record based on discussions on piazza
      val venue = xml.label match {
        case "article"
              => if ((xml \ "journal").text.length != 0) (xml \ "journal").text else (xml \ "booktitle").text
        case "inproceedings" | "incollection" => (xml \ "booktitle").text
        case "book" | "proceedings"
              => if ((xml \ "publisher").text.length != 0) (xml \ "publisher").text else (xml \ "booktitle").text
        case "phdthesis" | "mastersthesis" => (xml \ "school").text
        case "www" => xml.attributes("key").toString().split('/').slice(0,3).mkString("/")
        case _ => "No venue available"
      }

      val title = (xml \ "title").text

      var authorSeq = xml \ "author"
      if(authorSeq.length == 0){
        authorSeq= xml \ "editor"
      }

      val authorsCount = authorSeq.length

      //add to global map
      addToMap(venue, title, authorsCount)
    }

    //add title to map if its co-author number is highest
    def addToMap(venue: String, title: String, authorsCount: Int): Unit ={
      if(venueToTitles.contains(venue)){
        //The venue is already containning the publications with max number of authors
        val aCount = venueToTitles(venue).head._2 //get this max number from any author
        if(authorsCount == aCount){//add this publication if it has the same max authors
          venueToTitles(venue) += (title->authorsCount)
        }
        //If this publication has more authors than existing publications,
        //discard them because they are no longer have max authors. Then
        //add this new publication in.
        else if(authorsCount > aCount){
          venueToTitles(venue) = mutable.Map[String, Int](title->authorsCount)
        }
      }
      else{//if the venue is not already in the global map, create one, and add this publication in
        val newTitleMap = mutable.Map[String, Int](title->authorsCount)
        venueToTitles += (venue -> newTitleMap)
      }
    }

    @throws[IOException]
    @throws[InterruptedException]
    //loop through the global map to output the venue, its publications and their corresponding authors count
    override def cleanup(context: Mapper[LongWritable, Text, Text, IntWritable]#Context): Unit = {
      for((venue,tToC) <- venueToTitles){
        val sorted = ListMap(tToC.toSeq.sortBy(_._2):_*) //sort by authors count
        for((title,authors) <- sorted){
          if (venue!="" && title!="" && authors!=0){//filter out garbage based on venue assumption
            //output key as 2 column: venue and publication title
            //output value is authors count for the publication
            context.write(new Text(venue + "|" + title),new IntWritable(authors))
          }
        }
      }
    }
  }
  class ReduceTask4 extends Reducer[Text, IntWritable, Text, IntWritable]{

    @throws(classOf[IOException])
    override def reduce(key: Text, values: java.lang.Iterable[IntWritable],
                        context: Reducer[Text, IntWritable, Text, IntWritable]#Context): Unit ={

      //Do nothing, mappers already create satisfied outputs
      context.write(key,values.asScala.head)
    }
  }

  class MapTask5 extends Mapper[LongWritable, Text, Text, IntWritable]{
    var authorToCo: mutable.Map[String, mutable.Set[Int]] = mutable.Map[String, mutable.Set[Int]]()

      @throws(classOf[IOException])
      override def map(key: LongWritable, value: Text,
                       context: Mapper[LongWritable, Text, Text, IntWritable]#Context): Unit = {
        val xml = XML.loadString(value.toString)

        var authorSeq = xml \ "author"
        //If a publication has no author, consider its editors as authors
        if(authorSeq.length == 0){
          authorSeq= xml \ "editor"
        }
        if( authorSeq.length == 0) return

        //simpler way of understanding it: based on Professor answer on post @307_f1 on piazza
        for(author <- authorSeq){
          context.write(new Text(author.text), new IntWritable(authorSeq.length))
        }
      }

    //function to convert and NodSeq to a Set[Int]
    def makeIntSet(nodeSeq : NodeSeq) : mutable.Set[Int] = {
      var set = mutable.Set[Int]()
      for(node <- nodeSeq){
        set += node.text.hashCode
      }
      set
    }
  }

  class ReduceTask5 extends Reducer[Text, IntWritable, Text, IntWritable]{

    var authorToCo: mutable.Map[String, Int] = mutable.Map[String, Int]()

    @throws(classOf[IOException])
    override def reduce(key: Text, values: java.lang.Iterable[IntWritable],
                        context: Reducer[Text, IntWritable, Text, IntWritable]#Context): Unit ={
      //simpler way of understanding it: based on Professor answer on post @307_f1 on piazza
      var count = 0
      for(value <-values.asScala){
        count += value.get
      }

      if(authorToCo.contains(key.toString)){
        if(authorToCo.size < 100){
          authorToCo += key.toString->count
        }
        else{
          val min = minAuthor(authorToCo)
          if(authorToCo(min) < count){
            authorToCo -= min
            authorToCo += (key.toString->count)
          }
        }
      }
      else{
        authorToCo += (key.toString->count)
      }

      def minAuthor(map : mutable.Map[String,Int]) : String = {
        val sortedAscending = ListMap(map.toSeq.sortWith(_._2 < _._2):_*)
        sortedAscending.head._1
      }
    }
    @throws[IOException]
    @throws[InterruptedException]
    override def cleanup(context: Reducer[Text, IntWritable, Text, IntWritable]#Context): Unit = {
      val sortedByName = ListMap(authorToCo.toSeq.sortBy(_._1):_*) //sort by author name

      //then sort by number of co-authors. This is stable sort, so the previous sort result will retain in case of tie
      val sortedByNumCo = ListMap(sortedByName.toSeq.sortWith(_._2 > _._2):_*)

      for((name,count) <- sortedByNumCo){
        context.write(new Text(name), new IntWritable(count))
      }
    }
  }

  class MapTask6 extends Mapper[LongWritable, Text, Text, IntWritable]{

    var venueToTitles: mutable.Map[String, mutable.Map[String, Int]]
    = mutable.Map[String, mutable.Map[String,Int]]()

    @throws(classOf[IOException])
    override def map(key: LongWritable, value: Text,
                     context: Mapper[LongWritable, Text, Text, IntWritable]#Context): Unit = {
      val xml = XML.loadString(value.toString)

      var authorSeq = xml \ "author"
      //If a publication has no author, consider its editors as authors
      if(authorSeq.length == 0){
        authorSeq= xml \ "editor"
      }

      //output author and publication count of one if she has no co-author
      if(authorSeq.length == 1){
        context.write(new Text(authorSeq.head.text), new IntWritable(1))
      }
      else if(authorSeq.length != 0){
        for(author <- authorSeq){//emit these author with zero, it means they have co-author
          context.write(new Text(author.text), new IntWritable(0))
        }
      }
    }

  }
  class ReduceTask6 extends Reducer[Text, IntWritable, Text, IntWritable] {

    //map to hold author name and her number of publications
    var authorToCount: mutable.Map[String, Int] = mutable.Map[String, Int]()

    @throws(classOf[IOException])
    override def reduce(key: Text, values: java.lang.Iterable[IntWritable],
                        context: Reducer[Text, IntWritable, Text, IntWritable]#Context): Unit = {
      //count total publications of an author
      var publicationsCount = 0
      for(value <- values.asScala){
        val v = value.get
        if(v != 0){
          publicationsCount += v
        }
        else{
          return
        }
      }

      authorToCount += (key.toString->publicationsCount)
    }

    @throws[IOException]
    @throws[InterruptedException]
    override def cleanup(context: Reducer[Text, IntWritable, Text, IntWritable]#Context): Unit = {

      val sortedByCount = ListMap(authorToCount.toSeq.sortWith(_._2 > _._2):_*)

      var top100 = ListMap[String,Int]()
      if (sortedByCount.size > 100){//take the top 100 authors
        top100 = sortedByCount.slice(0,100)
      }
      else{//take all the authors if there are less than 100 authors
        top100 = sortedByCount
      }
      //loop to make output
      for((name,pubCount) <- top100){
        if(name != ""){
          context.write(new Text(name), new IntWritable(pubCount))
        }
      }

    }
  }
  @throws(classOf[Exception])
  def main(args: Array[String]) : Unit = {
    //create a logger
    val logger = LoggerFactory.getLogger("DBLPLogger")

    //To load configuration from file
    logger.info("Loading config file...")
    val programConfig = ConfigFactory.parseResources("application.conf")



    logger.info("Getting task number from config file...")
    val task = programConfig.getInt("task")

    logger.info("Setting up task " + task + " Homework 2...")
    //Create job according to assigned task number
    val job : Job = task match {
      case 1 => getJobTask1()
      case 2 => getJobTask2()
      case 3 => getJobTask3()
      case 4 => getJobTask4()
      case 5 => getJobTask5()
      case 6 => getJobTask6()
    }

    job.setJarByClass(this.getClass)

    job.setInputFormatClass(classOf[TextInputFormat])
    job.setOutputFormatClass(classOf[TextOutputFormat[_,_]])

    //File Input argument passed as a command line argument
    FileInputFormat.setInputPaths(job, new Path(args(0)))
    //File Output argument passed as a command line argument
    FileOutputFormat.setOutputPath(job, new Path(args(1)))

    //statement to execute the job
    logger.info("Task " + task + " starts running...")
    System.exit(if (job.waitForCompletion(true))  0 else 1)
  }

  def getJobTask1(): Job = {
    //Configuration for jobs
    val conf = new Configuration()

    //Uncomment this line for output separated by ',' , otherwise '\t' is separater
    conf.set("mapreduce.output.textoutputformat.separator", ",")

    val job = Job.getInstance(conf, "DBLP Processing")

    job.setMapperClass(classOf[MapTask1])
    job.setReducerClass(classOf[ReduceTask1])

    job.setMapOutputKeyClass(classOf[Text])
    job.setMapOutputValueClass(classOf[IntWritable])

    job.setOutputKeyClass(classOf[Text])
    job.setOutputValueClass(classOf[IntWritable])

    job
  }

  def getJobTask2(): Job = {
    //Configuration for jobs
    val conf = new Configuration()

    //Uncomment this line for output separated by ',' , otherwise '\t' is separater
    conf.set("mapreduce.output.textoutputformat.separator", ",")

    val job = Job.getInstance(conf, "DBLP Processing")

    job.setMapperClass(classOf[MapTask2])
    job.setReducerClass(classOf[ReduceTask2])

    job.setMapOutputKeyClass(classOf[Text])
    job.setMapOutputValueClass(classOf[IntWritable])

    job.setOutputKeyClass(classOf[Text])
    job.setOutputValueClass(classOf[IntWritable])

    job
  }

  def getJobTask3(): Job = {
    //Configuration for jobs
    val conf = new Configuration()

    //Uncomment this line for output separated by ',' , otherwise '\t' is separater
    conf.set("mapreduce.output.textoutputformat.separator", ",")

    val job = Job.getInstance(conf, "DBLP Processing")

    job.setMapperClass(classOf[MapTask3])
    job.setReducerClass(classOf[ReduceTask3])

    job.setMapOutputKeyClass(classOf[Text])
    job.setMapOutputValueClass(classOf[Text])

    job.setOutputKeyClass(classOf[Text])
    job.setOutputValueClass(classOf[Text])

    job
  }

  def getJobTask4(): Job = {
    //Configuration for jobs
    val conf = new Configuration()

    //Uncomment this line for output separated by ',' , otherwise '\t' is separater
    conf.set("mapreduce.output.textoutputformat.separator", "|")

    val job = Job.getInstance(conf, "DBLP Processing")

    job.setMapperClass(classOf[MapTask4])
    job.setReducerClass(classOf[ReduceTask4])

    job.setMapOutputKeyClass(classOf[Text])
    job.setMapOutputValueClass(classOf[IntWritable])

    job.setOutputKeyClass(classOf[Text])
    job.setOutputValueClass(classOf[IntWritable])

    job
  }

  def getJobTask5(): Job = {
    //Configuration for jobs
    val conf = new Configuration()

    //Uncomment this line for output separated by ',' , otherwise '\t' is separater
    conf.set("mapreduce.output.textoutputformat.separator", ",")

    val job = Job.getInstance(conf, "DBLP Processing")

    job.setMapperClass(classOf[MapTask5])
    job.setReducerClass(classOf[ReduceTask5])

    job.setMapOutputKeyClass(classOf[Text])
    job.setMapOutputValueClass(classOf[IntWritable])

    job.setOutputKeyClass(classOf[Text])
    job.setOutputValueClass(classOf[IntWritable])

    job
  }

  def getJobTask6(): Job = {
    //Configuration for jobs
    val conf = new Configuration()

    //Uncomment this line for output separated by ',' , otherwise '\t' is separater
    conf.set("mapreduce.output.textoutputformat.separator", ",")

    val job = Job.getInstance(conf, "DBLP Processing")

    job.setMapperClass(classOf[MapTask6])
    job.setReducerClass(classOf[ReduceTask6])

    job.setMapOutputKeyClass(classOf[Text])
    job.setMapOutputValueClass(classOf[IntWritable])

    job.setOutputKeyClass(classOf[Text])
    job.setOutputValueClass(classOf[IntWritable])

    job
  }

}