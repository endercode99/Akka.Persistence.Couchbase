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
using FluentAssertions;
using Xunit;


namespace Akka.Persistence.Couchbase.Tests
{
    public class CouchbaseSettings: Akka.TestKit.Xunit2.TestKit
    {
        [Fact]
        public void default_journal_settings()
        {
            var couchbasePersistence = CouchbasePersistence.Get(Sys);
            couchbasePersistence.JournalSettings.ConnectionString.Should().Be(string.Empty);
            couchbasePersistence.JournalSettings.Username.Should().Be(string.Empty);
            couchbasePersistence.JournalSettings.Password.Should().Be(string.Empty);
            couchbasePersistence.JournalSettings.JournalBucket.Should().Be(string.Empty);
            couchbasePersistence.JournalSettings.MetadataBucket.Should().Be(string.Empty);
        }


        [Fact]
        public void default_snapshot_settings()
        {
            var couchbasePersistence = CouchbasePersistence.Get(Sys);
            couchbasePersistence.SnapshotStoreSettings.ConnectionString.Should().Be(string.Empty);
            couchbasePersistence.SnapshotStoreSettings.Username.Should().Be(string.Empty);
            couchbasePersistence.SnapshotStoreSettings.Password.Should().Be(string.Empty);
            couchbasePersistence.SnapshotStoreSettings.SnapshotBucket.Should().Be(string.Empty);
        }
    }
}
