this is a java project.
using hadoop and mapreduce to computation distributedly to show the number of Money transfer person and Sectional statistics of the number of the bank per 10,000

using hadoop jar mapreduce.jar com.mrtest.hadoop.WordCountDriver /data/input/goal.txt /data/out to run it
using hadoop fs -cat /data/out/part-r-00000 to view the result

result:
x1    x2,x3  mean
the trade money between x1 ten thousand and x1+1 ten thousand of time is  x2, and the distanic customers is x3   
