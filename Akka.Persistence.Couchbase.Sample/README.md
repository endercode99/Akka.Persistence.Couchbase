# Sample Application Using the Akka.Persistence.Couchbase Package

This is a simple console application that allows users to enter commands that will be sent to an Akka.net Actor.
In this app, the actor creates the required Journal and Snapshot entries and echos back the same to the user.


# Prerequisites
 Setup up a [Couchbase Server cluster.](https://docs.couchbase.com/server/current/getting-started/start-here.html)   
 Create buckets for the Journal, Snapshot and Metadata stores.  
 Create primary indexs on each of the buckets.  
 
 # Usage
Clone the repo.    
Update the configuration in the ExampleActorSystem.cs file with your own Couchbase configuration.

```
akka.persistence {
                    journal {
                        plugin = ""akka.persistence.journal.couchbase""
                        couchbase {
                            connection-string = ""couchbase://localhost""
                            username = ""admin""
                            password = ""password""
                            journal-bucket = ""journalBucket""
                            metadata-bucket = ""metadataBucket""
                        }
                    }
                    snapshot-store {
                        plugin = ""akka.persistence.snapshot-store.couchbase""
                        couchbase {
                            connection-string = ""couchbase://localhost""
                            username = ""admin""
                            password = ""password""
                            snapshot-bucket = ""snapshotBucket""
                        }
                    }
                  
                }
```
