export MR_OUTPUT=/user/root/output-data

hadoop fs -rm -r $MR_OUTPUT

hadoop jar "$HADOOP_MAPRED_HOME"/hadoop-streaming.jar \
-Dmapred.job.name='Simple streaming job reduce' \
-Dmapred.reduce.tasks=1 \
-file /tmp/mapreduce/mapper.py -mapper /tmp/mapreduce/mapper.py \
-file /tmp/mapreduce/reducer.py -reducer /tmp/mapreduce/reducer.py \
-input /user/root/input-data -output $MR_OUTPUT


#-Dmapreduce.input.lineinputformat.linespermap=1000 \
#-inputformat org.apache.hadoop.mapred.lib.NLineInputFormat \


#-Dmapred.job.name='Simple streaming job' \
#-Dmapred.reduce.tasks=1 -Dmapreduce.input.lineinputformat.linespermap=1000 \
#-inputformat org.apache.hadoop.mapred.lib.NLineInputFormat \


# -inputformat org.apache.hadoop.mapred.lib.NLineInputFormat -Dmapreduce.input.lineinputformat.linespermap=1000 \

## s3
# -Dfs.s3a.endpoint=s3.amazonaws.com -Dfs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider

#hadoop jar "$HADOOP_MAPRED_HOME"/hadoop-streaming.jar \
#-Dmapred.job.name='Taxi streaming job' \
#-Dmapred.reduce.tasks=1 \
#-Dfs.s3a.endpoint=s3.amazonaws.com -Dfs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider \
#-inputformat org.apache.hadoop.mapred.lib.NLineInputFormat \
#-file /tmp/mapreduce/mapper.py -mapper /tmp/mapreduce/mapper.py \
#-file /tmp/mapreduce/reducer.py -reducer /tmp/mapreduce/reducer.py \
#-input  s3a://nyc-tlc/trip\ data/yellow_tripdata_2019-1*  -output taxi-output
