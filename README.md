# SparkHomework

## PySpark
```bash
conda env list
conda env remove --name hello-spark.yml # if it already exists hello-spark environment
conda env create -f hello-spark.yml
conda activate hello-spark
conda deactivate # if we exit this environment
start-master.sh
```
Run `HelloPySpark.ipynb` for solution by Python (Pyspark)
`Spark Main UI` run at [https://localhost.com:8080/](http://localhost:8080/)
`Spark Web UI` run at [https://localhost.com:4040/](http://localhost:4040/)

## Scala
```bash
start-master.sh
```
Reimport the package install in sbt file. Run 2 main `Object` **P1.scala** and **P2.scala** in folder **src/main/scala**  for **Problem 1**' s solution and **Problem 2**'s solution respectively.