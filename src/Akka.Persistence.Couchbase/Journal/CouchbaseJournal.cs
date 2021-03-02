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

        private readonly Akka.Serialization.Serialization _serialization;

        private ICluster _cluster;
        private IBucket _bucket;
        private ICouchbaseCollection _journalCollection;
        private ICouchbaseCollection _metadataCollection;

        private ImmutableDictionary<string, IImmutableSet<IActorRef>> _persistenceIdSubscribers = ImmutableDictionary.Create<string, IImmutableSet<IActorRef>>();
        private ImmutableDictionary<string, IImmutableSet<IActorRef>> _tagSubscribers = ImmutableDictionary.Create<string, IImmutableSet<IActorRef>>();
        private readonly HashSet<IActorRef> _newEventsSubscriber = new HashSet<IActorRef>();

        public CouchbaseJournal()
        {
            _settings = CouchbasePersistence.Get(Context.System).JournalSettings;

            _serialization = Context.System.Serialization;

        }


        protected override async void PreStart()
        {
            base.PreStart();

            _cluster = await Cluster.ConnectAsync(_settings.ConnectionString,
                    _settings.Username, _settings.Password);
            _bucket = await _cluster.BucketAsync(_settings.Bucket);
            _journalCollection = await _bucket.CollectionAsync(_settings.Collection);
            _metadataCollection = await _bucket.CollectionAsync(_settings.MetadataCollection);

        }

        public override Task<long> ReadHighestSequenceNrAsync(string persistenceId, long fromSequenceNr)
        {
            return ReadHighestSequenceNrTaskAsync(persistenceId, fromSequenceNr);
        }

        private async Task<long> ReadHighestSequenceNrTaskAsync(string persistenceId, long fromSequenceNr)
        {
            string query = "SELECT MAX(s.SequenceNr) FROM `$bucket` s AS $resultField WHERE s.DocumentType =$documentType AND s.PersistenceId = $persistenceID";

            var result = await _cluster.QueryAsync<dynamic>(
                query,
                options => options.Parameter("bucket", _settings.Bucket)
                .Parameter("persistenceID", persistenceId)
                .Parameter("resultField", "MaxSequenceNr")
                .Parameter("documentType", "JournalEntry")
            );

            long highestSeqNum = 0;

            await foreach (var row in result)
            {
                highestSeqNum = long.TryParse(row.SequenceNr, out highestSeqNum);
            }

            return highestSeqNum;
        }


        public override Task ReplayMessagesAsync(IActorContext context, string persistenceId, long fromSequenceNr, long toSequenceNr, long max, Action<IPersistentRepresentation> recoveryCallback)
        {
            var limitValue = max >= int.MaxValue ? int.MaxValue : (int)max;

            string query = "SELECT s.* FROM `$bucket` s WHERE AND s.PersistenceId = $persistenceID AND s.SequenceNr >= $FromSequenceNr  AND s.SequenceNr <= $ToSequenceNr ORDER BY s.SequenceNr ASC LIMIT $limitValue";
            return ReplayMessagesQueryAsync(context, query, persistenceId, fromSequenceNr, toSequenceNr, limitValue, recoveryCallback);
        }

        private async Task ReplayMessagesQueryAsync(IActorContext context, string query, string persistenceId, long fromSequenceNr, long toSequenceNr, int limitValue, Action<IPersistentRepresentation> recoveryCallback)
        {
            var result = await _cluster.QueryAsync<JournalEntry>(
                query,
                options => options.Parameter("bucket", _settings.Bucket)
                .Parameter("persistenceID", persistenceId)
                .Parameter("FromSequenceNr", fromSequenceNr)
                .Parameter("ToSequenceNr", toSequenceNr)
                .Parameter("limitValue", limitValue)
            );

            await foreach (var row in result)
            {
                recoveryCallback(ToPersistenceRepresentation(row, context.Sender));
            }

        }

        private Persistent ToPersistenceRepresentation(JournalEntry row, IActorRef sender)
        {
            return new Persistent(row.Payload, row.SequenceNr, row.Manifest, row.PersistenceId, row.isDeleted, sender);
        }

        protected override Task DeleteMessagesToAsync(string persistenceId, long toSequenceNr)
        {
            return DeleteMessagesToTaskAsync(persistenceId, toSequenceNr);
        }

        private async Task DeleteMessagesToTaskAsync(string persistenceId, long toSequenceNr)
        {
            string query = "DELETE FROM `$bucket` s WHERE s.PersistenceId = $persistenceID";

            if(toSequenceNr != long.MaxValue)
            {
                query += " AND s.SequenceNr <= $ToSequenceNr";
            }

            await _cluster.QueryAsync<dynamic>(
                query,
                options => options.Parameter("bucket", _settings.Bucket)
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
                    await _journalCollection.InsertAsync<JournalEntry>(journalEntry.Id, journalEntry);
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


            var serializer = _serialization.FindSerializerFor(message);
            var binary = serializer.ToBinary(message);


            return new JournalEntry
            {
                Id = message.PersistenceId + "_" + message.SequenceNr,
                Payload = binary,
                PersistenceId = message.PersistenceId,
                SequenceNr = message.SequenceNr,
                Manifest = string.Empty
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
