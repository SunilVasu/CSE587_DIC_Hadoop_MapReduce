Steps for running:
1) The PairsMapper.java is the java program for finding the pairs. 
2) The following commands are run to load into the hdfs
	hadoop com.sun.tools.javac.Main PairsMapper.java
	jar cf PairsMapper.jar PairsMapper*.class
	hadoop jar PairsMapper.jar PairsMapper ~/input/Q2_1 ~/output1
	hdfs dfs -cat ~/output1/*

3) The output from the hadoop is saved into the InputOutput folder. 
