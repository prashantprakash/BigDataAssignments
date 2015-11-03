/*
 * @Author: Prashant Prakash
 * CS6350, homework 3
 *
 *
 */


I. Purpose
----------

Learn learn how to how to use Pig Latin and Hive.


II. instructions of running programs
--------------------------------------------------

Q1:
pig -x mapreduce Q1.pig

Q2:
pig -x local -param "X=1" Q2.pig;

Q3:
pig -x local -param "X=1" Q3.pig;



hive -f Q8.hive;
hive -f Q7.hive;
hive -f Q6.hive;



Q4:
pig -x local -param "NetId=pxp141730" Q4.pig

Q5:
hive -f  -hiveconf X=1 Q5.hive

Q6:
hive -f Q6.hive

Q7:
hive -f Q7.hive

Q8:
hive -f Q8.hive

Q9:
 hive -hiveconf NetId='pxp141730' -f Q9.hive;