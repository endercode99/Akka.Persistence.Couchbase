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

        private ICluster _cluster;
        private IBucket _snapshotBucket;
        private ICouchbaseCollection _snapshotCollection;

        public CouchbaseSnapshotStore()
        {
            _settings = CouchbasePersistence.Get(Context.System).SnapshotStoreSettings;
        }

    
        protected override void PreStart()
        {
            base.PreStart();

            _cluster = Cluster.ConnectAsync(_settings.ConnectionString,
                    _settings.Username, _settings.Password).Result;
            _snapshotBucket = _cluster.BucketAsync(_settings.SnapshotBucket).Result;
            _snapshotCollection = _snapshotBucket.DefaultCollectionAsync().Result;

        }

        protected override async Task DeleteAsync(SnapshotMetadata metadata)
        {
            string query = String.Format("DELETE FROM {0} WHERE persistenceId=$persistenceID AND documentType=$documentType", _settings.SnapshotBucket);

            if (metadata.SequenceNr > 0 && metadata.SequenceNr < long.MaxValue)
            {
                query += " AND sequenceNr=$SequenceNr";
            }

            if (metadata.Timestamp != DateTime.MinValue && metadata.Timestamp != DateTime.MaxValue)
            {
                query += " AND timestamp=$Timestamp";
            }

            await _cluster.QueryAsync<SnapshotEntry>(
                query,
                options => options
                .Parameter("documentType", "SnapshotEntry")
                .Parameter("persistenceID", metadata.PersistenceId)
                .Parameter("limit", metadata.SequenceNr)
                .Parameter("timelimit", metadata.Timestamp)
            );

        }

        protected override Task DeleteAsync(string persistenceId, SnapshotSelectionCriteria criteria)
        {
           return TaskDeleteQueryAsync(persistenceId, criteria);
        }

        protected override  Task<SelectedSnapshot> LoadAsync(string persistenceId, SnapshotSelectionCriteria criteria)
        {
            return TaskLoadQueryAsync(persistenceId, criteria);

        }

        private async Task<SelectedSnapshot> TaskLoadQueryAsync(string persistenceId, SnapshotSelectionCriteria criteria)
        {    
            string query = String.Format("SELECT {0}.* FROM {0} WHERE documentType=$documentType AND persistenceId=$persistenceID",
                _settings.SnapshotBucket);

            if (criteria.MaxSequenceNr > 0 && criteria.MaxSequenceNr < long.MaxValue)
            {
                query += String.Format(" AND sequenceNr<={0} ", criteria.MaxSequenceNr);
            }

            if (criteria.MaxTimeStamp != DateTime.MinValue && criteria.MaxTimeStamp != DateTime.MaxValue)
            {
                query += String.Format(" AND timestamp<={0} ", criteria.MaxTimeStamp.Ticks);
            }

            query += " ORDER BY sequenceNr DESC Limit 1;";

         
            var result = await _cluster.QueryAsync<SnapshotEntry>(query,
            options => options
                .Parameter("persistenceID", "ExampleActor")
                .Parameter("documentType", "SnapshotEntry")
            );

            if (result.MetaData.Status == QueryStatus.Success)
            {
              
                await foreach (var row in result.Rows)
                {
                
                    SnapshotEntry firstRow = row;
                    return ToSelectedSnapshot(firstRow);
                }
            }

            return null;
        }

        private async Task TaskDeleteQueryAsync(string persistenceId, SnapshotSelectionCriteria criteria)
        {

            string query = String.Format("DELETE FROM {0} WHERE persistenceId=$persistenceID documentType=$documentType ", _settings.SnapshotBucket);

            if (criteria.MaxSequenceNr > 0 && criteria.MaxSequenceNr < long.MaxValue)
            {
                query += "AND sequenceNr<=$limit ";
            }

            if (criteria.MaxTimeStamp != DateTime.MinValue && criteria.MaxTimeStamp != DateTime.MaxValue)
            {
                query += "AND timestamp<=$timelimit ";
            }


            var result = await _cluster.QueryAsync<SnapshotEntry>(
                query,
                options => options
                .Parameter("documentType", "SnapshotEntry")
                .Parameter("persistenceID", persistenceId)
                .Parameter("limit", criteria.MaxSequenceNr)
                .Parameter("timelimit", criteria.MaxTimeStamp.Ticks)
            );

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
                    Id = metadata.PersistenceId + "_" + metadata.SequenceNr.ToString(),
                    PersistenceId = metadata.PersistenceId,
                    SequenceNr = metadata.SequenceNr,
                    Snapshot = snapshot,
                    Timestamp = metadata.Timestamp.Ticks,
   
                };

            await _snapshotCollection.UpsertAsync(snapshotEntry.Id, snapshotEntry);
        }
    }
}
