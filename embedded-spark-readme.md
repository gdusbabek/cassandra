# Embedding Spark

* Spark version is controlled in the pom, in the dependencies section.
* Main class for Spark master is `org.apache.spark.deploy.master.Master`.
* Main class for Spark worker is `org.apache.spark.deploy.worker.Worker`.

## Master

Classpath for master is this:

/Library/Java/JavaVirtualMachines/jdk1.8.0_25.jdk/Contents/Home/bin/java 
-cp /opt/spark-1.5.2-bin-hadoop2.6/sbin/../conf/
:/opt/spark-1.5.2-bin-hadoop2.6/lib/spark-assembly-1.5.2-hadoop2.6.0.jar
:/opt/spark-1.5.2-bin-hadoop2.6/lib/datanucleus-api-jdo-3.2.6.jar
:/opt/spark-1.5.2-bin-hadoop2.6/lib/datanucleus-core-3.2.10.jar
:/opt/spark-1.5.2-bin-hadoop2.6/lib/datanucleus-rdbms-3.2.9.jar 
-Xms1g 
-Xmx1g 
org.apache.spark.deploy.master.Master 
--ip gdusbabek.local 
--port 7077 
--webui-port 8080

Here's what I'm using:
java -cp ./conf/:/opt/spark-1.5.2-bin-hadoop2.6/lib/spark-assembly-1.5.2-hadoop2.6.0.jar -Xmx1G -Xms1G org.apache.spark.deploy.master.Master --ip gdusbabek.local --port 7077 --webui-port 8080

## Worker

/Library/Java/JavaVirtualMachines/jdk1.8.0_25.jdk/Contents/Home/bin/java 
-cp /opt/spark-1.5.2-bin-hadoop2.6/sbin/../conf/
:/opt/spark-1.5.2-bin-hadoop2.6/lib/spark-assembly-1.5.2-hadoop2.6.0.jar
:/opt/spark-1.5.2-bin-hadoop2.6/lib/datanucleus-api-jdo-3.2.6.jar
:/opt/spark-1.5.2-bin-hadoop2.6/lib/datanucleus-core-3.2.10.jar
:/opt/spark-1.5.2-bin-hadoop2.6/lib/datanucleus-rdbms-3.2.9.jar 
-Xms1g 
-Xmx1g 
org.apache.spark.deploy.worker.Worker 
--webui-port 8081 
spark://localhost:7077

Here's what I'm using:
-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=5005
java -agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=5005 -cp ./conf/:/opt/spark-1.5.2-bin-hadoop2.6/lib/spark-assembly-1.5.2-hadoop2.6.0.jar -Xmx1G -Xms1G org.apache.spark.deploy.worker.Worker --webui-port 8081 spark://gdusbabek.local:7077

