# Akka.Persistence.Couchbase

.NET Library to use Akka.NET Persistence with Couchbase as the backend store.

| Tool                                             | Latest Version     |
|--------------------------------------------------|--------------------|
| Akka.Persistence.Couchbase                       | (v0.1.0.0)|

## Getting Started

This section provides an overview on how to configure your Akka.NET applications to use Couchbase as the backend for Persistence.

### Setting up Couchbase

To install and set up Couchbase, follow the steps https://docs.couchbase.com/server/current/install/install-intro.html 
Once Couchbase is installed and running, create the buckets you will use to store Snapshot data, Journal data and Journal Metadata. For each of these buckets create primary indexes by following the steps here https://docs.couchbase.com/server/current/n1ql/n1ql-language-reference/createprimaryindex.html 

###  Configuration

To use the library initialize the Actor System of your application with the following configuration

```
akka.persistence {
                    journal {
                        plugin = ""akka.persistence.journal.couchbase""
                        couchbase {
                            connection-string = ""couchbase://localhost""
                            username = ""administrator""
                            password = ""password""
                            journal-bucket = ""journalBucket""
                            metadata-bucket = ""metadataBucket""
                        }
                    }
                    snapshot-store {
                        plugin = ""akka.persistence.snapshot-store.couchbase""
                        couchbase {
                            connection-string = ""couchbase://localhost""
                            username = ""administrator""
                            password = ""password""
                            snapshot-bucket = ""snapshotBucket""
                        }
                    }
                  
                }
```    

# Running Tests
To execute the package tests, run the following command from the repository root folder.
```
dotnet test
```

