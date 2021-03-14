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
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Threading.Tasks;
using Akka.Actor;
using Akka.Event;
using Akka.Persistence.Journal;
using Couchbase;
using Couchbase.KeyValue;

namespace Akka.Persistence.Couchbase.Journal
{
    public class CouchbaseJournal : AsyncWriteJournal
    {

        private readonly CouchbaseJournalSettings _settings;

        private ICluster _cluster;
        private IBucket _journalBucket;
        private ICouchbaseCollection _journalCollection;
        private IBucket _metadataBucket;
        private ICouchbaseCollection _metadataCollection;

        private ImmutableDictionary<string, IImmutableSet<IActorRef>> _persistenceIdSubscribers = ImmutableDictionary.Create<string, IImmutableSet<IActorRef>>();
        private ImmutableDictionary<string, IImmutableSet<IActorRef>> _tagSubscribers = ImmutableDictionary.Create<string, IImmutableSet<IActorRef>>();
        private readonly HashSet<IActorRef> _newEventsSubscriber = new HashSet<IActorRef>();

        public CouchbaseJournal()
        {
            _settings = CouchbasePersistence.Get(Context.System).JournalSettings;

        }


        protected override void PreStart()
        {
            base.PreStart();
            _cluster = Cluster.ConnectAsync(_settings.ConnectionString,
                      _settings.Username, _settings.Password).Result;
            _journalBucket =  _cluster.BucketAsync(_settings.JournalBucket).Result;
            _journalCollection = _journalBucket.DefaultCollectionAsync().Result;
            _metadataBucket = _cluster.BucketAsync(_settings.MetadataBucket).Result;
            _metadataCollection = _metadataBucket.DefaultCollectionAsync().Result;
        }

        public override Task<long> ReadHighestSequenceNrAsync(string persistenceId, long fromSequenceNr)
        {
            return ReadHighestSequenceNrTaskAsync(persistenceId, fromSequenceNr);
        }

        private async Task<long> ReadHighestSequenceNrTaskAsync(string persistenceId, long fromSequenceNr)
        {
            string query = String.Format("SELECT {0}.* FROM {0} WHERE documentType=$documentType AND persistenceId=$persistenceID ORDER BY sequenceNr DESC LIMIT 1;",
           _settings.JournalBucket);

            var result = await _cluster.QueryAsync<JournalEntry>(
                query,
                options => options
                .Parameter("persistenceID", persistenceId)
                .Parameter("documentType", "JournalEntry")
            );

            long highestSeqNum = 0;
            await foreach (var row in result.Rows)
            {
               long.TryParse(row.SequenceNr.ToString(), out highestSeqNum);
            }
            return highestSeqNum;
        }


        public override Task ReplayMessagesAsync(IActorContext context, string persistenceId, long fromSequenceNr, long toSequenceNr, long max, Action<IPersistentRepresentation> recoveryCallback)
        {
            var limitValue = max >= int.MaxValue ? int.MaxValue : (int)max;

            string query = String.Format("SELECT {0}.* FROM {0} WHERE persistenceId=$persistenceID AND sequenceNr>=$FromSequenceNr AND sequenceNr<=$ToSequenceNr ORDER BY SequenceNr ASC LIMIT $limitValue", _settings.JournalBucket);
            return ReplayMessagesQueryAsync(context, query, persistenceId, fromSequenceNr, toSequenceNr, limitValue, recoveryCallback);
        }

        private async Task ReplayMessagesQueryAsync(IActorContext context, string query, string persistenceId, long fromSequenceNr, long toSequenceNr, int limitValue, Action<IPersistentRepresentation> recoveryCallback)
        {
            if (limitValue == 0)
                return;

            var result = await _cluster.QueryAsync<JournalEntry>(
                query,
                options => options
                .Parameter("persistenceID", persistenceId)
                .Parameter("FromSequenceNr", fromSequenceNr)
                .Parameter("ToSequenceNr", toSequenceNr)
                .Parameter("limitValue", limitValue)
            );

            await foreach (var row in result.Rows)
            {
                recoveryCallback(ToPersistenceRepresentation(row, context.Sender));
            }

        }

        private Persistent ToPersistenceRepresentation(JournalEntry row, IActorRef sender)
        {
          return new Persistent(row.Payload, row.SequenceNr, row.Manifest, row.PersistenceId, row.IsDeleted, sender);
        }

        protected override Task DeleteMessagesToAsync(string persistenceId, long toSequenceNr)
        {
            return DeleteMessagesToTaskAsync(persistenceId, toSequenceNr);
        }

        private async Task DeleteMessagesToTaskAsync(string persistenceId, long toSequenceNr)
        {
            string query = String.Format("DELETE FROM {0} WHERE persistenceId=$persistenceID", _settings.JournalBucket);

            if (toSequenceNr != long.MaxValue)
            {
                query += " AND sequenceNr<=$ToSequenceNr;";
            }

            await _cluster.QueryAsync<dynamic>(
                query,
                options => options
                .Parameter("persistenceID", persistenceId)
                .Parameter("$ToSequenceNr", toSequenceNr)
            );
        }

        protected override async Task<IImmutableList<Exception>> WriteMessagesAsync(IEnumerable<AtomicWrite> messages)
        {
            var allTags = ImmutableHashSet<string>.Empty;
            var persistentIds = new HashSet<string>();
            var messageList = messages.ToList();

            var writeTasks = messageList.Select(async message =>
            {
                var persistentMessages = ((IImmutableList<IPersistentRepresentation>)message.Payload);

                if (HasTagSubscribers)
                {
                    foreach (var p in persistentMessages)
                    {
                        if (p.Payload is Tagged t)
                        {
                            allTags = allTags.Union(t.Tags);
                        }
                    }
                }

                foreach (IPersistentRepresentation persistentMessage in persistentMessages)
                {
                    var journalEntry = ToJournalEntry(persistentMessage);
                    await _journalCollection.UpsertAsync<JournalEntry>(journalEntry.Id, journalEntry);
                }

                if (HasPersistenceIdSubscribers)
                    persistentIds.Add(message.PersistenceId);
            });

            await SetHighSequenceId(messageList);

            var result = await Task<IImmutableList<Exception>>
                .Factory
                .ContinueWhenAll(writeTasks.ToArray(),
                    tasks => tasks.Select(t => t.IsFaulted ? TryUnwrapException(t.Exception) : null).ToImmutableList());

            if (HasPersistenceIdSubscribers)
            {
                foreach (var id in persistentIds)
                {
                    NotifyPersistenceIdChange(id);
                }
            }

            if (HasTagSubscribers && allTags.Count != 0)
            {
                foreach (var tag in allTags)
                {
                    NotifyTagChange(tag);
                }
            }
            if (HasNewEventSubscribers)
                NotifyNewEventAppended();
            return result;
        }

  

        private async Task SetHighSequenceId(IList<AtomicWrite> messages)
        {
            var persistenceId = messages.Select(c => c.PersistenceId).First();
            var highSequenceId = messages.Max(c => c.HighestSequenceNr);

            var metadataEntry = new MetadataEntry
            {
                Id = persistenceId,
                PersistenceId = persistenceId,
                SequenceNr = highSequenceId
            };

            await _metadataCollection.UpsertAsync<MetadataEntry>(metadataEntry.Id, metadataEntry);
        }

        private JournalEntry ToJournalEntry(IPersistentRepresentation message)
        {
            object payload = message.Payload;
            if (message.Payload is Tagged tagged)
            {
                payload = tagged.Payload;
                message = message.WithPayload(payload);
            }

            return new JournalEntry
            {
                Id = message.PersistenceId + "_" + message.SequenceNr,
                Payload = payload,
                PersistenceId = message.PersistenceId,
                SequenceNr = message.SequenceNr,
                Manifest = string.Empty,
                IsDeleted = message.IsDeleted,
            };
        }

        protected bool HasTagSubscribers => _tagSubscribers.Count != 0;
        protected bool HasPersistenceIdSubscribers => _persistenceIdSubscribers.Count != 0;
        protected bool HasNewEventSubscribers => _newEventsSubscriber.Count != 0;


        private void NotifyPersistenceIdChange(string persistenceId)
        {
            if (_persistenceIdSubscribers.TryGetValue(persistenceId, out var subscribers))
            {
                var changed = new EventAppended(persistenceId);
                foreach (var subscriber in subscribers)
                    subscriber.Tell(changed);
            }
        }

        private void NotifyTagChange(string tag)
        {
            if (_tagSubscribers.TryGetValue(tag, out var subscribers))
            {
                var changed = new TaggedEventAppended(tag);
                foreach (var subscriber in subscribers)
                    subscriber.Tell(changed);
            }
        }

        private void NotifyNewEventAppended()
        {
            if (HasNewEventSubscribers)
            {
                foreach (var subscriber in _newEventsSubscriber)
                {
                    subscriber.Tell(NewEventAppended.Instance);
                }
            }
        }
    }

    public sealed class EventAppended : IDeadLetterSuppression
    {
       
        public readonly string PersistenceId;
        public EventAppended(string persistenceId)
        {
            PersistenceId = persistenceId;
        }
    }

    public sealed class TaggedEventAppended : IDeadLetterSuppression
    {
        public readonly string Tag;
        public TaggedEventAppended(string tag)
        {
            Tag = tag;
        }
    }

    public sealed class NewEventAppended : IDeadLetterSuppression
    {
        public static NewEventAppended Instance = new NewEventAppended();

        private NewEventAppended() { }
    }
}
