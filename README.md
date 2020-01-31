# s3dupdo
S3 data duplicator

# Using it has two steps 

Plan (collects a src path + dst path) & writes an SQLite file with all the files to be copied

Run goes over the sqlitefile (in as many threads as you wish) and copies the files one by one, committing them to SQLite

So you can even begin a run on one machine, ^C and start over on another machine.

# Examples

You could also use `hadoop jar` instead of java command in the examples given below.
## Usage
```java

java -cp ./target/*:./target/lib/*:.: org.notmysock.repl.S3Dupdo

usage: S3Dupdo [-d <arg>] [-i <arg>] [-n <arg>] [-op <arg>] [-p <arg>] [-s
       <arg>] [-v <arg>]
Missing operation
 -awsKey <accesskey:secretkey> pass in aws keys
 -d,--dst <arg>          destination data
 -i,--nodeId <arg>       nodeId during copy
 -n,--nodes <arg>        parallelize n-nodes
 -op,--operation <arg>   operation (plan, run, verify)
 -p,--parallel <arg>     parallelize n-way
 -s,--src <arg>          source data
 -v,--verbose <arg>      verbose
```

## Plan

```java
-- defaults
java -cp ./target/*:./target/lib/*:.: org.notmysock.repl.S3Dupdo -s s3a://bucket1/src -d s3a://bucket1/dst -op plan test_s3_1.sqlite

-- Plan with 3 nodes. Node id is "0" indexed.
java -cp ./target/*:./target/lib/*:.: org.notmysock.repl.S3Dupdo -s s3a://bucket1/src/ -d s3a://bucket1/dst/ -op plan -n 3 test_s3_3_nodes.sqlite
```

## Run

```java
-- defaults. In this mode, all files are copied via single client node with specified parallelism of 10.
java -cp ./target/*:./target/lib/*:.: org.notmysock.repl.S3Dupdo -op run -p 10 test_s3_3_nodes.sqlite

-- Run in multi node fashion. Here, files specific to node "0" are copied over with specified parallelism of 10.
java -cp ./target/*:./target/lib/*:.: org.notmysock.repl.S3Dupdo -op run -p 10 -i 0 test_s3_3_nodes.sqlite
```

## Info
To know details about the persisted data, you can run `info` command to get
 the details on the number of files copied, number of nodes etc.
```java
java -cp ./target/*:./target/lib/*:.: org.notmysock.repl.S3Dupdo -op info test_s3_3_nodes.sqlite

Copied files stats:
	Total files : 591
	Total size : 84290
	Distinct Nodes : 3

Yet to be copied files stats:
	Total files : 217
	Total size : 29438
	Distinct Nodes : 3
```

# Debugging

- When running `plan`, `UNIQUE constraint failed` exception is thrown.
e.g
```java
Exception in thread "main" org.sqlite.SQLiteException: [SQLITE_CONSTRAINT]  Abort due to constraint violation (UNIQUE constraint failed: FILES.SRC)
	at org.sqlite.core.DB.newSQLException(DB.java:941)
	at org.sqlite.core.DB.newSQLException(DB.java:953)
	at org.sqlite.core.DB.execute(DB.java:854)
	at org.sqlite.core.DB.executeUpdate(DB.java:895)
	at org.sqlite.jdbc3.JDBC3PreparedStatement.executeUpdate(JDBC3PreparedStatement.java:102)
```

This happens when the same `sqlite` file is being used for persisting
 the data. Use different file to run `plan`. Otherwise, delete the old `sqlite`.


- Throughput: It depends on the system where you are running this
 program. Program emits throughput details after completion. Try to copy over smaller
 dataset and determine the appropriate parallelism for your workload. In case
  single client is not enough, generate `plan` with multiple nodes and run
   from multiple nodes.

 ```java
processedFiles
             count = 174
         mean rate = 8.08 events/second
     1-minute rate = 8.28 events/second
     5-minute rate = 8.37 events/second
    15-minute rate = 8.39 events/second
```
