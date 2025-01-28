# Code Repository

Project Group: 29
Course: Managing Big Data

prerequisites to run code

- access to the the spark eemcs.utwente.nl
- ~3 GB RAM as total size of datasets is 1.7 GB (no expode or flatMap is called)

## RQ1.1

```shell
spark-submit NYC_per_borough.py --conf spark.dynamicAllocation.maxExecutors=5
```

## RQ1.2

```shell
spark-submit NYC_per_zipcode.py --conf spark.dynamicAllocation.maxExecutors=5
```
