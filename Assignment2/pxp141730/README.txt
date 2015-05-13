#################################################
#	Author - PRASHANT PRAKASH		#
# 						#	
#	Course -> Big Data Cs6350.001		#
#						#	
#	Purpose -> Assignment 2			#
# 						#
#	No of Programs -> 2			#
#						#	
#################################################


1) command to run 1st program:

hadoop jar PR4.jar PR4 /pxp141730/input/users.dat /pxp141730/input/ratings.dat /pxp141730/input/movies.dat /pxp141730/output/



2) Command to run 2nd program:

hadoop jar PR5.jar PR5 /pxp141730/input/users.dat /pxp141730/input/ratings.dat /pxp141730/output1/ 527




Explanation:

hadoop --> command to invoke hadoop
jar    --> specify hadoop to execute a jar file.
*.jar  --> the jar file to be executed
PR*    --> the name of the class 


/pxp141730/input/ -- > HDFS input location where all files users.dat , movies.dat and ratings.dat are stored

create directories /pxp141730/input/ in HDFS and copy all *.dat files to this location 

command to create input directory in HDFS 

hdfs dfs -mkdir -p /pxp141730/input/

hadoop fs -put input/* /pxp141730/input/ 

where input is the a folder in your current location and contains all the input files (users.dat,movies.dat,ratings.dat)



/pxp141730/output/output -- > output location for first program

In second program the argument "527"  is the movie ID provided by user. 


commands to see output
-----------------------
Check a file in the folder /pxp141730/output1/  part-* 
then give the command as below: 

hdfs dfs -cat /pxp141730/output1/part-*

where part-* is the name fo file which got created in the folder /pxp141730/output1/

Similarly do for other.
