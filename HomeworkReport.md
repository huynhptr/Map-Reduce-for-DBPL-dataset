# Cuong Huynh - Homework 2
## Parser:
It is used to parse each xml publication(e.i article, inproceedings, etc) in to one line of text. Then we can use embedded XML processing facility of Scala on it.
I have included the project to parse the dblp XML file. It used multithreading to speed up the process. It can parse the dblp.xml file all at once(given the machine has enough ram 32GB recommended). I also used Typesafe configuration and Logging in this parser project.

Note: make sure to set the paths for input, output, and log file in src/main/resources/application.conf
## Instruction of running tasks:
* In the project folder src/main/resources/, edit the application.conf file to specify which homework task(1 to 6) to build the .jar file.
* Then on the command line, run:
```sbt clean compile assembly```
* When it finishes running, move the .jar file from target/scala-2.13/ to the regular file system(RegFS) in the VM. Use command:
```scp -P 2222 <local path> <username>@<host>:<remotepath>```
to move the file.
* Then go to the RegFS that the .jar file was sent to.
* Run the command:
```hadoop jar <.jar file> <input file(HDFS path)> <output file(HDFS path)```
* Before running the above command, we might need to put the input file(in this case dblp.txt) in HDFS by running:
```hadoop fs -put <input file path on RegFS> <input file path on HDFS>```
* Also we might need to delete the already existed output directory in HDFS by running:
```hadoop fs -rm -r <output directory of HDFS>```
* After the program finishes running, we can open the output file by a few options:
1. Run: ```hadoop fs -cat <path to the file on HDFS>```
2. Move the file to local by accessing it on HDFS on Ambari to download it. Then open it with an editor(largest output file is around 130MBs).
3. Move the file by other methods.


## Files location:
The main input file is dblp.txt is in the top level of the project folder Homework2. It is parsed. It contains 1 record per line.
The output file for 6 tasks are in the result folder.
### task1Result.csv : 
* First column is venue
* Second column is author name
* Third column is his/her number of publications
### task2Result.csv :
* First column is author name
* Second column is the longest consecutive years that s/he have publications
### task3Result.csv :
* First column is venue
* Second column is publication tiles separated by ‘|’
### task4Result.csv :
* First column is venue
* Second column is publication tiles separated by ‘|’
* Third column is the number of authors
### task5Result.csv :
* First column is author name
* Second column is number of his/her co-authors
### task6Result.csv :
* First column is author name
* Second column is his/her number of publications
