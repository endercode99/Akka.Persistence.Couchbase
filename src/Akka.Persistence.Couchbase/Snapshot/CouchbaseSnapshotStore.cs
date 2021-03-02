using System;
using System.Threading.Tasks;
using Akka.Persistence.Snapshot;
using Couchbase;
using Couchbase.KeyValue;
using Couchbase.Query;

namespace Akka.Persistence.Couchbase.Snapshot
{
    public class CouchbaseSnapshotStore: SnapshotStore
    {
        private readonly CouchbaseSnapshotSettings _settings;

        private readonly Akka.Serialization.Serialization _serialization;

        private ICluster _cluster;
        private IBucket _bucket;
        private ICouchbaseCollection _snapshotCollection;

        public CouchbaseSnapshotStore()
        {
            _settings = CouchbasePersistence.Get(Context.System).SnapshotStoreSettings;

            _serialization = Context.System.Serialization;

        }

    
        protected override async void PreStart()
        {
            base.PreStart();

            _cluster = await Cluster.ConnectAsync(_settings.ConnectionString,
                    _settings.Username, _settings.Password);
            _bucket = await _cluster.BucketAsync(_settings.Bucket);
            _snapshotCollection = await _bucket.CollectionAsync(_settings.Collection);

        }

        protected override async Task DeleteAsync(SnapshotMetadata metadata)
        {
            string Id = metadata.PersistenceId + "_" + metadata.SequenceNr;
            await _snapshotCollection.RemoveAsync(Id);
        }

        protected override Task DeleteAsync(string persistenceId, SnapshotSelectionCriteria criteria)
        {
            string query = "DELETE FROM `$bucket` s WHERE s.DocumentType =$documentType AND s.PersistenceId = $persistenceID ";

            if (criteria.MaxSequenceNr > 0 && criteria.MaxSequenceNr < long.MaxValue)
            {
                query += "AND SequenceNr <= $limit ";
            }

            if (criteria.MaxTimeStamp != DateTime.MinValue && criteria.MaxTimeStamp != DateTime.MaxValue)
            {
                query += " AND Timestamp <= $timelimit ";
            }

            return TaskQueryAsync(query, persistenceId, criteria);
        }

        protected override  Task<SelectedSnapshot> LoadAsync(string persistenceId, SnapshotSelectionCriteria criteria)
        {
            
            string query = "SELECT s.* FROM `$bucket` s WHERE s.DocumentType =$documentType AND s.PersistenceId = $persistenceID ";

            if (criteria.MaxSequenceNr > 0 && criteria.MaxSequenceNr < long.MaxValue)
            {
               query += "AND SequenceNr <= $limit ";
            }

            if (criteria.MaxTimeStamp != DateTime.MinValue && criteria.MaxTimeStamp != DateTime.MaxValue)
            {
                query += " AND Timestamp <= $timelimit ";
            }

            query += "ORDER BY SequenceNr DESC Limit 1";


            return TaskQueryAsync(query, persistenceId, criteria);

        }

        private async Task<SelectedSnapshot> TaskQueryAsync(string query, string persistenceId, SnapshotSelectionCriteria criteria)
        {
            var result = await _cluster.QueryAsync<SnapshotEntry>(
                query,
                options => options.Parameter("bucket", _settings.Bucket)
                .Parameter("documentType", "SnapshotEntry")
                .Parameter("persistenceID", persistenceId)
                .Parameter("limit", criteria.MaxSequenceNr)
                .Parameter("timelimit", criteria.MaxTimeStamp.Ticks)
            );



            SnapshotEntry firstRow = null;

            if (result.MetaData.Status == QueryStatus.Success)
            {
                await foreach (var row in result)
                {
                    firstRow = row;
                    break;
                }
            }

            return ToSelectedSnapshot(firstRow);
         
        }


        private SelectedSnapshot ToSelectedSnapshot(SnapshotEntry entry)
        {
            return
                new SelectedSnapshot(
                    new SnapshotMetadata(entry.PersistenceId, entry.SequenceNr, new DateTime(entry.Timestamp)),
                    entry.Snapshot);
        }




        protected override async Task SaveAsync(SnapshotMetadata metadata, object snapshot)
        {
            SnapshotEntry snapshotEntry = new SnapshotEntry
                {
                    Id = metadata.PersistenceId + "_" + metadata.SequenceNr,
                    PersistenceId = metadata.PersistenceId,
                    SequenceNr = metadata.SequenceNr,
                    Snapshot = snapshot,
                    Timestamp = metadata.Timestamp.Ticks,
   
                };

            await _snapshotCollection.UpsertAsync(snapshotEntry.Id, snapshotEntry);
        }
    }
}
