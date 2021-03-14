#region [ License information          ]

/* ************************************************************
 *
 *    @author Couchbase <info@couchbase.com>
 *    @copyright 2021 Couchbase, Inc.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 *
 * ************************************************************/

#endregion
using System;
using Akka.Actor;
using Akka.Configuration;
using Akka.Persistence.Couchbase;

namespace TestCore
{
    public class ExampleActorSystem
    {
        public static ActorSystem ExampleSystem { get; private set; }

        public static IActorRef ExampleActor { get; private set; }

        public static void Start()
        {
            var config = ConfigurationFactory.ParseString(@"
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
                  
                }");

            ExampleSystem = ActorSystem.Create("ExampleSystem", config);

            CouchbasePersistence.Get(ExampleSystem);

            ExampleActor = ExampleSystem.ActorOf<ExampleActor>();
        }

        public static void Stop()
        {
            ExampleSystem.Terminate().Wait();
        }
    }
}
