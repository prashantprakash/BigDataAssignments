#################################################
#	Author - PRASHANT PRAKASH		#
# 						#	
#	Course -> Big Data Cs6350.001		#
#						#	
#	Purpose -> Assignment 1			#
# 						#
#	No of Programs -> 3			#
#						#	
#################################################


1) command to run 1st program:

hadoop jar PR1.jar PR1 /pxp141730/input/users.dat /user/pxp141730/output1


2) Command to run 2nd program:

hadoop jar PR2.jar PR2 /pxp141730/input/users.dat /user/pxp141730/output2


3) command to 3rd program:

hadoop jar PR3.jar PR3 /pxp141730/input/movies.dat /user/pxp141730/output3 Fantasy




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



/user/pxp141730/output1 -- > output location for first program

In third program the argument "Fantasy"  is the movie genre provided by user. 


commands to see output
-----------------------
Check a file in the folder /user/pxp141730/output1/  part-* 
then give the command as below: 

hdfs dfs -cat /user/pxp141730/output1/part-*

where part-* is the name fo file which got created in the folder /user/pxp141730/output1/

Similarly do for other two.
