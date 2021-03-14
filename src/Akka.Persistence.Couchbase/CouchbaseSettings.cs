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
using Akka.Configuration;


namespace Akka.Persistence.Couchbase
{
    public abstract class CouchbaseSettings
    {
        public string ConnectionString { get; private set; }

        public string Username { get; private set; }

        public string Password { get; private set; }

        protected CouchbaseSettings(Config config)
        {
            ConnectionString = config.GetString("connection-string");
            Username = config.GetString("username");
            Password = config.GetString("password");
        }
    }


    public class CouchbaseJournalSettings : CouchbaseSettings
    {
        public string JournalBucket { get; private set; }
        public string MetadataBucket { get; private set; }

        public CouchbaseJournalSettings(Config config) : base(config)
        {
            if (config == null)
                throw new ArgumentNullException(nameof(config),
                    "Couchbase journal settings HOCON section not found");
            JournalBucket = config.GetString("journal-bucket");
            MetadataBucket = config.GetString("metadata-bucket");
        }
    }

    public class CouchbaseSnapshotSettings : CouchbaseSettings
    {
        public string SnapshotBucket { get; private set; }
        public CouchbaseSnapshotSettings(Config config) : base(config)
        {
            if (config == null)
                throw new ArgumentNullException(nameof(config),
                    "Couchbase snapshot settings HOCON section not found");
            SnapshotBucket = config.GetString("snapshot-bucket");
        }
    }
}
