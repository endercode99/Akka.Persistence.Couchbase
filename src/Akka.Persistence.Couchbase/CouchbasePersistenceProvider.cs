using System;
using Akka.Actor;

namespace Akka.Persistence.Couchbase
{
    public class CouchbasePersistenceProvider: ExtensionIdProvider<CouchbasePersistence>
    {
        public override CouchbasePersistence CreateExtension(ExtendedActorSystem system)
        {
            return new CouchbasePersistence(system);
        }
    }
}
