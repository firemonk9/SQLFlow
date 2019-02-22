# Project SQLFlow

SQLFlow is a work flow based tool for Data Operations. The input is a flow(JSON & CSV) composed of jobs represented as a tree. The following six job types are supported.

* Filter
* Transformation
* Join
* Column cast
* Supports following Input/Output source
* Data Masking

The Input/Output can be any of the following five types of data sources.
1. CSV
2. Json
3. Hive
4. JDBC
5. Parquet


## Getting Started

These instructions will get you a copy of the project up and running on your local machine for development and testing. See deployment for notes on how to deploy the project on a live system.

Clone the Repo from URL:

```git clone https://github.com/firemonk9/SQLFlow```

### Prerequisites for development


* Java8
* Java editor (Intellij preferred)

```
Give examples
```

### Building

The following command will create the jar.
```
mvn package
```

## Running the tests

The following command executes tests.
```
mvn test
```

## Deployment

Copy the Masking-1.0-SNAPSHOT.jar jar to the server. Below is a an example to run the sample using spark-submit command.

replace the <CODE_PATH> with location where the project is downloaded/cloned in below command and also <CODE_PATH>/Basic_SQL_Flow/src/test/resources/flow1.json

```
spark-submit --master yarn --deploy-mode client  --class com.metlife.wf.SqlWorkFlowMain /home/METNET/at1146906/Masking-1.0-SNAPSHOT.jar INPUT_FILE=/tmp/flow1.csv
```

## Built With

* [Maven](https://search.maven.org/) - Dependency Management

## Authors

* **Dhiraj Peechara** - *Initial work*


## License

This project is licensed under the MIT License - see the [LICENSE.md](LICENSE.md) file for details

## Acknowledgments

* Hat tip to anyone whose code was used
* Inspiration
* etc
