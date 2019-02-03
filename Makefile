TBL=customer
SRC:=$(shell find . -name *.java )

all: target/s3dupdo-1.0-SNAPSHOT.jar
	-- rm $(TBL).sqlite
	-- hadoop fs -rm -r hdfs://ctr-e138-1518143905142-564127-01-000002.hwx.site:8020/tmp/$(TBL)_test 
	time env HADOOP_USER_NAME=hive java -jar target/s3dupdo-1.0-SNAPSHOT.jar \
	   -op plan -s hdfs://ctr-e138-1518143905142-564127-01-000002.hwx.site:8020/warehouse/tablespace/managed/hive/tpcds_bin_partitioned_orc_10000.db/$(TBL) \
	   -d hdfs://ctr-e138-1518143905142-564127-01-000002.hwx.site:8020/tmp/$(TBL)_test \
	   $(TBL)
	time env HADOOP_USER_NAME=hive java -jar target/s3dupdo-1.0-SNAPSHOT.jar \
	   -op run --parallel 5 $(TBL)

target/s3dupdo-1.0-SNAPSHOT.jar: $(SRC)
	mvn clean package
