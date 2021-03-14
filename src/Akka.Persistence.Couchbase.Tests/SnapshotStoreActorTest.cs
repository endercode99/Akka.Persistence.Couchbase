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

namespace Akka.Persistence.Couchbase.Tests
{
    public class SnapshotStoreActorTest : UntypedPersistentActor
    {
        public static Props Props(string persistenceId) => Actor.Props.Create(() => new JournalActorTest(persistenceId));

        public override string PersistenceId { get; }

        public SnapshotStoreActorTest(string persistenceId)
        {
            PersistenceId = persistenceId;
        }

        public sealed class DeleteCommand
        {
            public DeleteCommand(long toSequenceNr)
            {
                ToSequenceNr = toSequenceNr;
            }

            public long ToSequenceNr { get; }
        }

        protected override void OnCommand(object message)
        {
        }

        protected override void OnRecover(object message)
        {
            switch (message)
            {
                case DeleteCommand delete:
                    DeleteMessages(delete.ToSequenceNr);
                    Sender.Tell($"{delete.ToSequenceNr}-deleted");
                    break;
                case string cmd:
                    var sender = Sender;
                    SaveSnapshot(cmd);
                    Persist(cmd, e => sender.Tell($"{e}-done"));
                    break;
            }
        }
    }
}
