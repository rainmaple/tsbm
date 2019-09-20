# Tsbm

TimeSerials BenchMark(TSBM) is a benchmark specially developed for time series databases. It is compatible with many popular time series databases such as InfluxDB, IOTDB, TaosDB and establishes a unified and standardized test benchmark.

## Prerequisites

1. OS (centos 7)
2. Java version>= 1.8
3. Maven >= 3.0 (If you want to compile and install IoTDB from source code)
4. if you want to test TDengine , the extra dependency need to be added

## Quick Start Guide

1. ###  Configure

   ```vim conf/db.properties``` 
   to config
   ```DB_TYPE=dbname in lowcase``` 

   ```DB_IP=the database ip address``` 

   ```DB_PORT=the database port```  

   ```DB_USER=the database login username ``` 

   ```DB_PASSWD=the database login password ``` 

2. ### * Start import (this procedure is neccessary)

   ```./import.sh``` 

3. ### Start write_test

   ```./write_test.sh``` 

4. ### Start read_test

   ```./read_test.sh``` 

5. ### Start write_test_mix.sh

   ```./write_test_mix.sh``` 

6. ### Start read_test_mix.sh

   ```./read_test_mix.sh```    

## Test results collection

the test result in document ```result/result.csv ```

## Add a New Test

if you want to test new database,  you can see [this md](/doc/development.md)
timeseries db benchmark 