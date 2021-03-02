using System;
using Akka.Actor;
using Akka.Configuration;

namespace Akka.Persistence.Couchbase
{
    public class CouchbasePersistence : IExtension
    {
        public CouchbasePersistence() { }

        public static Config DefaultConfiguration()
        {
            return ConfigurationFactory.FromResource<CouchbasePersistence>("Akka.Persistence.Couchbase.reference.conf");
        }

        public static CouchbasePersistence Get(ActorSystem system)
        {
            return system.WithExtension<CouchbasePersistence, CouchbasePersistenceProvider>();
        }

        public CouchbaseJournalSettings JournalSettings { get; }

        public CouchbaseSnapshotSettings SnapshotStoreSettings { get; }

        public CouchbasePersistence(ExtendedActorSystem system)
        {
            if (system == null)
                throw new ArgumentNullException(nameof(system));

            system.Settings.InjectTopLevelFallback(DefaultConfiguration());

            var journalConfig = system.Settings.Config.GetConfig("akka.persistence.journal.couchbase");
            JournalSettings = new CouchbaseJournalSettings(journalConfig);

            var snapshotConfig = system.Settings.Config.GetConfig("akka.persistence.snapshot-store.couchbase");
            SnapshotStoreSettings = new CouchbaseSnapshotSettings(snapshotConfig);
        }
    }

}
