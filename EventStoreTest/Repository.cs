using EventStore.Client;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace EventStoreTest
{
    public interface IRepository
    {
        Task<TAggregate> GetByIdAsync<TAggregate>(object id) where TAggregate : IAggregate, new();
        Task<long> SaveAsync(AggregateBase aggregate, params KeyValuePair<string, string>[] extraHeaders);
    }

    public class Repository : IRepository, IDisposable
    {
        private const int WritePageSize = 500;
        private const int ReadPageSize = 500;
        private static readonly JsonSerializerSettings SerializerSettings = new JsonSerializerSettings { TypeNameHandling = TypeNameHandling.None };
        private readonly EventStoreClient _eventStoreConnection;
        private readonly EventTypeResolver _eventTypeResolver;

        public Repository(EventStoreClient eventStoreConnection, EventTypeResolver eventTypeResolver)
        {
            _eventStoreConnection = eventStoreConnection;
            _eventTypeResolver = eventTypeResolver;
        }

        public void Dispose()
        {
        }

        public async Task<TAggregate> GetByIdAsync<TAggregate>(object id) where TAggregate : IAggregate, new()
        {
            var aggregate = new TAggregate();

            var streamName = $"{aggregate.Identifier}-{id}";

            var resp = _eventStoreConnection.ReadStreamAsync(Direction.Forwards, streamName, StreamPosition.Start);
            await foreach (var evt in resp)
            {
                var payload = DeserializeEvent(evt);
                aggregate.ApplyEvent(payload);
            }

            //long eventNumber = 0;
            //StreamEventsSlice currentSlice;
            //do
            //{
            //    currentSlice = await _eventStoreConnection.ReadStreamEventsForwardAsync(streamName, eventNumber, ReadPageSize, false);

            //    if (currentSlice.Status == SliceReadStatus.StreamNotFound)
            //    {
            //        throw new AggregateNotFoundException(id, typeof(TAggregate));
            //    }

            //    if (currentSlice.Status == SliceReadStatus.StreamDeleted)
            //    {
            //        throw new AggregateDeletedException(id, typeof(TAggregate));
            //    }

            //    eventNumber = currentSlice.NextEventNumber;

            //    foreach (var resolvedEvent in currentSlice.Events)
            //    {
            //        var payload = DeserializeEvent(resolvedEvent.Event);
            //        aggregate.ApplyEvent(payload);
            //    }
            //} while (!currentSlice.IsEndOfStream);

            return aggregate;
        }

        private object DeserializeEvent(ResolvedEvent evt)
        {
            var targetType = _eventTypeResolver.GetTypeForEventName(evt.Event.EventType);
            var json = Encoding.UTF8.GetString(evt.Event.Data.ToArray());
            return JsonConvert.DeserializeObject(json, targetType);
        }

        public async Task<long> SaveAsync(AggregateBase aggregate, params KeyValuePair<string, string>[] extraHeaders)
        {
            var streamName = $"{aggregate.Identifier}-{aggregate.Id}";

            var pendingEvents = aggregate.GetPendingEvents();
            var originalVersion = aggregate.Version - pendingEvents.Count;

            //TODO: konkurenciakezelés, expectedversion izé
            //TODO: hamárvanilyenstream-akkorszálljonel-kezelés, StreamState enum
            //TODO: tranzakciókezelés több batch-nél

            try
            {
                IWriteResult result;

                var commitHeaders = CreateCommitHeaders(aggregate, extraHeaders);
                var eventsToSave = pendingEvents.Select(x => ToEventData(Guid.NewGuid(), x, commitHeaders));

                var eventBatches = GetEventBatches(eventsToSave);

                //if (eventBatches.Count == 1)
                //{
                    // If just one batch write them straight to the Event Store
                    result = await _eventStoreConnection.AppendToStreamAsync(streamName, StreamState.Any, eventBatches[0]);
                //}
                //else
                //{
                //    // If we have more events to save than can be done in one batch according to the WritePageSize, then we need to save them in a transaction to ensure atomicity
                //    using var transaction = await _eventStoreConnection.StartTransactionAsync(streamName, originalVersion);
                //    foreach (var batch in eventBatches)
                //    {
                //        await transaction.WriteAsync(batch);
                //    }

                //    result = await transaction.CommitAsync();
                //}

                aggregate.ClearPendingEvents();

                return result.NextExpectedVersion;
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
            }

            return originalVersion + 1;
        }


        private IList<IList<EventData>> GetEventBatches(IEnumerable<EventData> events)
        {
            return events.Batch(WritePageSize).Select(x => (IList<EventData>)x.ToList()).ToList();
        }

        private static IDictionary<string, string> CreateCommitHeaders(AggregateBase aggregate, KeyValuePair<string, string>[] extraHeaders)
        {
            var commitId = Guid.NewGuid();

            var commitHeaders = new Dictionary<string, string>
            {
                {MetadataKeys.CommitIdHeader, commitId.ToString()},
                {MetadataKeys.AggregateClrTypeHeader, aggregate.GetType().AssemblyQualifiedName},
                {MetadataKeys.UserIdentityHeader, Thread.CurrentThread.Name}, // TODO - was Thread.CurrentPrincipal?.Identity?.Name
                {MetadataKeys.ServerNameHeader, "DefaultServerNameHEader"}, // TODO - was Environment.MachineName
                {MetadataKeys.ServerClockHeader, DateTime.UtcNow.ToString("o")}
            };

            foreach (var extraHeader in extraHeaders)
            {
                commitHeaders[extraHeader.Key] = extraHeader.Value;
            }

            return commitHeaders;
        }

        private static EventData ToEventData(Guid eventId, object evnt, IDictionary<string, string> headers)
        {
            var data = Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(evnt, SerializerSettings));

            var eventHeaders = new Dictionary<string, string>(headers)
            {
                {MetadataKeys.EventClrTypeHeader, evnt.GetType().AssemblyQualifiedName}
            };
            var metadata = Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(eventHeaders, SerializerSettings));
            var typeName = evnt.GetType().Name;

            return new EventData(Uuid.FromGuid(eventId), typeName, data, metadata);
        }
    }
}
