mssqldestroyer
==============

Java tool to delete a mssql database, used to bring down a database after a pull request is merged.

### How to build and run.

Clean and make a Eclipse project.
`$ mvn clean eclipse:eclipse -U`

Make a single jar with embedded dependencies.
`$ mvn clean compile assembly:single`

Run.
`$ java -jar target/mssqldestroyer-0.1-jar-with-dependencies -c creds.properties -s localhost -d "TestDB"

See the sample.creds.properties file for an example of how the credential file should like.