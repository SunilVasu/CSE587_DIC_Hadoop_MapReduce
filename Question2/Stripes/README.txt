Steps for running:
1) The Stripes.java is the java program for finding the pairs. 
2) The following commands are run to load into the hdfs
	hadoop com.sun.tools.javac.Main Stripes.java
	jar cf Stripes.jar Stripes*.class MyMap*.class
	hadoop jar Stripes.jar Stripes ~/input/Q2_2 ~/output1
	hdfs dfs -cat ~/output1/*

3) The output from the hadoop is saved into the InputOutput folder. 
