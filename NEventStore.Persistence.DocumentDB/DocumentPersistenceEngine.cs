using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;
using NEventStore.Logging;
using NEventStore.Serialization;

namespace NEventStore.Persistence.DocumentDB
{
    class DocumentPersistenceEngine : IPersistStreams
    {
        private static readonly ILog Logger = LogFactory.BuildLogger(typeof(DocumentPersistenceEngine));

        public DocumentPersistenceEngine(DocumentClient client, IDocumentSerializer serializer, DocumentPersistenceOptions options)
        {
            this.Client = client;
            this.Serializer = serializer;
            this.Options = options;
        }

        public DocumentClient Client { get; private set; }
        public IDocumentSerializer Serializer { get; private set; }
        public DocumentPersistenceOptions Options { get; private set; }

        private Database Database { get; set; }

        public void DeleteStream(string bucketId, string streamId)
        {
            throw new NotImplementedException();
        }

        public void Drop()
        {
            this.Purge();
        }

        public ICheckpoint GetCheckpoint(string checkpointToken = null)
        {
            throw new NotImplementedException();
        }

        public IEnumerable<ICommit> GetFrom(string checkpointToken = null)
        {
            var collection = EnsureCollection(Options.CommitCollectionName).Result;
            var documents = Client.CreateDocumentQuery<DocumentCommit>(
                collection.DocumentsLink,
                $"SELECT * FROM {Options.CommitCollectionName}" +
                "WHERE " +
                $"{Options.CommitCollectionName}.CheckpointToken = '{checkpointToken}'");

            return documents.ToArray().Select(p => p.ToCommit(Serializer));
        }

        public IEnumerable<ICommit> GetFrom(string bucketId, DateTime start)
        {
            var collection = EnsureCollection(Options.CommitCollectionName).Result;
            var documents = Client.CreateDocumentQuery<DocumentCommit>(
                collection.DocumentsLink,
                $"SELECT * FROM {Options.CommitCollectionName}" +
                "WHERE " +
                $"{Options.CommitCollectionName}.BucketId = '{bucketId}' AND " +
                $"{Options.CommitCollectionName}.CommitStamp >= {start}");

            return documents.ToArray().Select(p => p.ToCommit(Serializer));
        }

        public IEnumerable<ICommit> GetFrom(string bucketId, string checkpointToken)
        {
            var collection = EnsureCollection(Options.CommitCollectionName).Result;
            var documents = Client.CreateDocumentQuery<DocumentCommit>(
                collection.DocumentsLink,
                $"SELECT * FROM {Options.CommitCollectionName}" +
                "WHERE " +
                $"{Options.CommitCollectionName}.BucketId = '{bucketId}' AND " +
                $"{Options.CommitCollectionName}.CheckpointToken = '{checkpointToken}'");

            return documents.ToArray().Select(p => p.ToCommit(Serializer));
        }

        public IEnumerable<ICommit> GetFromTo(string bucketId, DateTime start, DateTime end)
        {
            var collection = EnsureCollection(Options.CommitCollectionName).Result;
            var documents = Client.CreateDocumentQuery<DocumentCommit>(
                collection.DocumentsLink,
                $"SELECT * FROM {Options.CommitCollectionName}" +
                "WHERE " +
                $"{Options.CommitCollectionName}.BucketId = '{bucketId}' AND " +
                $"({Options.CommitCollectionName}.CommitStamp BETWEEN {start} AND {end})");

            return documents.ToArray().Select(p => p.ToCommit(Serializer));
        }

        public IEnumerable<ICommit> GetUndispatchedCommits()
        {
            throw new NotImplementedException();
        }

        public void Initialize()
        {
            var databases = Client.ReadDatabaseFeedAsync().Result;
            Database = databases.FirstOrDefault(d => d.Id == Options.DatabaseName);

            if (Database == null)
            {
                Database = Client.CreateDatabaseAsync(new Database {Id = Options.DatabaseName}).Result;
            }

            if (Database == null)
            {
                throw new NullReferenceException("Database cannot be null");
            }
        }

        public bool IsDisposed
        {
            get { throw new NotImplementedException(); }
        }

        public void MarkCommitAsDispatched(ICommit commit)
        {
            throw new NotImplementedException();
        }

        public void Purge(string bucketId)
        {
            throw new NotImplementedException();
        }

        public void Purge()
        {
            throw new NotImplementedException();
        }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        protected virtual void Dispose(bool disposing)
        {
            if (!disposing || IsDisposed)
                return;
            Logger.Debug(Messages.ShuttingDownPersistence);
            this.Client.Dispose();
        }

        public ICommit Commit(CommitAttempt attempt)
        {
            Logger.Debug(Messages.AttemptingToCommit, attempt.Events.Count, attempt.StreamId, attempt.CommitSequence, attempt.BucketId);
            try
            {
                return TryExecute(() =>
                {
                    var doc = attempt.ToDocumentCommit(this.Serializer);

                    var collection = this.EnsureCollection(this.Options.CommitCollectionName).Result;
                    var document = this.Client.CreateDocumentAsync(collection.SelfLink, doc).Result;

                    Logger.Debug(Messages.CommitPersisted, attempt.CommitId, attempt.BucketId);
                    SaveStreamHead(attempt.ToDocumentStreamHead());
                    
                    return doc.ToCommit(this.Serializer);
                });
            }
            catch (Microsoft.Azure.Documents.DocumentClientException) // TODO: verify actual exception
            {
                DocumentCommit savedCommit = LoadSavedCommit(attempt);
                
                if (savedCommit.CommitId == attempt.CommitId)
                    throw new DuplicateCommitException();
                
                Logger.Debug(Messages.ConcurrentWriteDetected);
                throw new ConcurrencyException();
            }

        }

        public IEnumerable<ICommit> GetFrom(string bucketId, string streamId, int minRevision, int maxRevision)
        {
            var collection = EnsureCollection(Options.CommitCollectionName).Result;
            var documents = Client.CreateDocumentQuery<DocumentCommit>(
                collection.DocumentsLink,
                $"SELECT * FROM {Options.CommitCollectionName} " +
                "WHERE " +
                $"{Options.CommitCollectionName}.BucketId = '{bucketId}' AND " +
                $"{Options.CommitCollectionName}.StreamId = '{streamId}' AND " +
                $"({Options.CommitCollectionName}.StreamRevision BETWEEN {minRevision} AND {maxRevision})");

            return documents.ToArray().Select(p => p.ToCommit(Serializer));
        }

        public bool AddSnapshot(ISnapshot snapshot)
        {
            return false;
        }

        public ISnapshot GetSnapshot(string bucketId, string streamId, int maxRevision)
        {
            return null;
        }

        public IEnumerable<IStreamHead> GetStreamsToSnapshot(string bucketId, int maxThreshold)
        {
            throw new NotImplementedException();
        }

        protected virtual T TryExecute<T>(Func<T> callback)
        {
            try
            {
                return callback();
            }
            //catch (WebException e)
            //{
            //    Logger.Warn(Messages.StorageUnavailable);
            //    throw new StorageUnavailableException(e.Message, e);
            //}
            //catch (NonUniqueObjectException e)
            //{
            //    Logger.Warn(Messages.DuplicateCommitDetected);
            //    throw new DuplicateCommitException(e.Message, e);
            //}
            //catch (Document.Abstractions.Exceptions.ConcurrencyException)
            //{
            //    Logger.Warn(Messages.ConcurrentWriteDetected);
            //    throw;
            //}
            catch (ObjectDisposedException)
            {
                Logger.Warn(Messages.StorageAlreadyDisposed);
                throw;
            }
            catch (Exception e)
            {
                Logger.Error(Messages.StorageThrewException, e.GetType());
                throw new StorageException(e.Message, e);
            }
        }

        private async Task<DocumentCollection> EnsureCollection(string collectionId)
        {
            if (Database == null)
            {
                Initialize();
            }

            var collections = await Client.ReadDocumentCollectionFeedAsync(Database.CollectionsLink);

            return collections.FirstOrDefault(c => c.Id == collectionId)
                ?? await Client.CreateDocumentCollectionAsync(Database.SelfLink, new DocumentCollection { Id = collectionId });
        }

        private DocumentCommit LoadSavedCommit(CommitAttempt attempt)
        {
            Logger.Debug(Messages.DetectingConcurrency);
            
            return TryExecute(() =>
            {
                var collection = this.EnsureCollection(this.Options.CommitCollectionName).Result;
                var documents = this.Client.ReadDocumentFeedAsync(collection.DocumentsLink).Result;
                var documentId = attempt.ToDocumentCommitId();

                return documents.Where(d => d.Id == documentId).AsEnumerable().FirstOrDefault();
            });
        }

        private void SaveStreamHead(DocumentStreamHead updated)
        {
            TryExecute(() =>
            {
                var collection = EnsureCollection(Options.StreamHeadCollectionName).Result;
                var documents = Client.ReadDocumentFeedAsync(collection.DocumentsLink).Result;
                var documentId = DocumentStreamHead.GetStreamHeadId(updated.BucketId, updated.StreamId);

                var streamHead = documents.FirstOrDefault(d => d.Id == documentId) ?? updated;

                streamHead.HeadRevision = updated.HeadRevision;

                if (updated.SnapshotRevision > 0)
                    streamHead.SnapshotRevision = updated.SnapshotRevision;

                var documentToUpdate = Client.CreateDocumentAsync(collection.SelfLink, streamHead).Result;
                return Client.ReplaceDocumentAsync(documentToUpdate).Result;
           });
        }
    }
}