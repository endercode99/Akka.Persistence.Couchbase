using System;
using Akka.Configuration;


namespace Akka.Persistence.Couchbase
{
    public abstract class CouchbaseSettings
    {
        public string ConnectionString { get; private set; }

        public string Username { get; private set; }

        public string Password { get; private set; }

        public string Bucket { get; private set; }

        public string Collection { get; private set; }

        protected CouchbaseSettings(Config config)
        {
            ConnectionString = config.GetString("connection-string");
            Username = config.GetString("username");
            Password = config.GetString("password");
            Collection = config.GetString("collection");
            Bucket = config.GetString("bucket");
        }
    }


    public class CouchbaseJournalSettings : CouchbaseSettings
    {
        public string MetadataCollection { get; private set; }

        public CouchbaseJournalSettings(Config config) : base(config)
        {
            if (config == null)
                throw new ArgumentNullException(nameof(config),
                    "Couchbase journal settings cannot be initialized, because required HOCON section couldn't been found");

            MetadataCollection = config.GetString("metadata-collection");
        }
    }

    public class CouchbaseSnapshotSettings : CouchbaseSettings
    {
        public CouchbaseSnapshotSettings(Config config) : base(config)
        {
            if (config == null)
                throw new ArgumentNullException(nameof(config),
                    "Couchbase snapshot settings cannot be initialized, because required HOCON section couldn't been found");
        }
    }
}
