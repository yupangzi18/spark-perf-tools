# spark-perf-tools

This is a performance test tools for Apache Spark, which is based on TPC-H and TPC-DS.

You can generate data in both local and HDFS:

    # You need to set `SPARK_HOME` to your Spark path before running a command below
    $ ./bin/hdgen --output-location hdfs://9.135.73.45:9000/tmp/spark/tpch --table-filter "lineitem"
    
    $ ./bin/dsdgen --output-location /tmp/saprk/tpcds --table-filter "lineitem" --num-partitions 128

After generating data, you can run queries:

    $ ./bin/run-tpch-benchmark --data-location hdfs://9.135.73.45:9000/tmp/spark/tpch --query-filter "q6, q8"
    
    $ ./bin/run-tpcds-benchmark --data-location /tmp/saprk/tpcds --query-filter "q2"

Options for the generator:

    $ ./bin/dsdgen or ./bin/hdgen --help
    Usage: spark-submit --class <this class> --conf key=value <spark tpcds/tpch datagen jar> [Options]
    Options:
      --output-location [STR]                Path to an output location
      --scale-factor [NUM]                   Scale factor (default: 1)
      --format [STR]                         Output format (default: parquet)
      --overwrite                            Whether it overwrites existing data (default: false)
      --partition-tables                     Whether it partitions output data (default: false)
      --use-double-for-decimal               Whether it prefers double types instead of decimal types (default: false)
      --use-string-for-char                  Whether it prefers string types instead of char/varchar types (default: false)
      --cluster-by-partition-columns         Whether it cluster output data by partition columns (default: false)
      --filter-out-null-partition-values     Whether it filters out NULL partitions (default: false)
      --table-filter [STR]                   Queries to filter, e.g., catalog_sales,store_sales
      --num-partitions [NUM]                 # of partitions (default: 100)

