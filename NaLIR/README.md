## Running NaLIR

_NOTE:_ All steps that can be skipped will be marked with a `[SKIP]`.
This is because we tried to make the
whole setting up process a bit easier with minimum alterations to the code

### Downloads

`[SKIP]` First, download necessary jars from:

https://s3.amazonaws.com/umdb-users/cjbaik/nalir_jars.tar.gz

`[SKIP]`and unzip it into `NaLIR/lib`.

Second, download the SQL files needed from:

https://s3.amazonaws.com/umdb-users/cjbaik/mas.sql

and load it into a running MySQL database on your machine.

Also, load `auxFiles\setup_mas.sql` in the root project folder into MySQL as well, which adds some additional features to the database that are needed to execute it.

### Configuration

There are some hard-coded paths (to schema information and the like) in the original code that need to be modified. Executing it will give you the errors that will point you in the right direction, but at the very least, the following should be modified for your local machine:

* `[SKIP]` `architecture/CommandInterface.java`
    *  Line 70: path to the corresponding file on your machine
* `rdbms/RDBMS.java`
    * Lines 22-24: your MySQL configuration info
* `[SKIP]` `rdbms/SchemaGraph.java`
    * Line 33: path to corresponding file on your machine
    * Line 81: path to corresponding file on your machine

### Execute

You can either:
* Spin up an Apache Tomcat Server (the configuration should be setup for IntelliJ IDEA Ultimate Edition currently) and head to `/nalir.jsp` in your browser at the right port
* Execute `CommandInterface.java` using some of the following example commands to use it interactively
    * `#useDB mas` - initial setup, loads the MAS database
    * `#query return me the homepage of PVLDB` - run a query
    * [`NOTE`] The above steps are replaced, so to use NaLIR follow the steps in the root `README.md`
* Execute `Experiments.java` with some modification to run your tests

## Questions?

Contact cjbaik at umich dot edu
