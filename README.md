# Benchmarking Database Search Systems: a DB-NL Perspective

The following repo contains the database search systems used for the analysis provided in our paper "Benchmarking Database Search Systems: a DB-NL Perspective".


## Contents

* [discover/](discover/) -> `Discover: Keyword Search in Relational Databases`
* [discoverIR/](discoverIR/) -> `EfficientIR-style Keyword Search over Relational Databases`
* [expressq2/](expressq2/) ->`Constructing an Interactive Natural Language Interface for Relational Databases`
* [spark/](spark/) -> `Spark: Top-k Keyword Query in Relational Databases.`
* [NaLIR/](NaLIR/) -> `Answering Keyword Queries involving Aggregates and GROUPBY on Relational Databases.`
* [sharedlib/](sharedlib/) -> A library shared by all systems (except `NaLIR`) used for database management and utility functions. This folder also contains the [query generator](sharedlib/src/main/java/shared/benchmark/Generator.java)

## Prerequisites

The systems run on any MySQL database without any preprocessing steps, except `NaLIR`. For the latter, the procedure for setting it up is described by their authors in a readme file inside the folder. Take into consideration that the MySQL database must have some `FULLTEXT` indexes to enable the systems to search terms in the database. We have a java class [IndexCreator](sharedlib/src/main/java/shared/database/connectivity/DatabaseIndexCreator.java) that can create indexes with configurations provided by the user. Or you can visit the [mysql FULLTEXT documentation](https://dev.mysql.com/doc/refman/5.6/en/innodb-fulltext-index.html) for more information.

## Use

Each of the above folders have a gradle wrapper so by using `./gradlew run` the systems will start their command prompt interface.
Then each system will be waiting for an input consisting of 3 (or 4) things:
* The query
* The database to search
* The number of results

Insert each one of the above with followed by an `enter`. You will see that the systems request input when printing an `<input>` line (which is also printed when input was received successfully). The systems run in a loop so to stop them use `CTRL^C`.
