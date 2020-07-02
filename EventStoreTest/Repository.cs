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

            var events = _eventStoreConnection.ReadStreamAsync(Direction.Forwards, streamName, StreamPosition.Start);
            
            await foreach (var evt in events)
            {
                var payload = DeserializeEvent(evt);
                aggregate.ApplyEvent(payload);
            }

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
            var originalVersion = (uint)aggregate.Version - (uint)pendingEvents.Count;

            //TODO: hamárvanilyenstream-akkorszálljonel-kezelés, StreamState enum ?

            try
            {
                IWriteResult result;

                var commitHeaders = CreateCommitHeaders(aggregate, extraHeaders);
                var eventsToSave = pendingEvents.Select(x => ToEventData(Guid.NewGuid(), x, commitHeaders));

                result = await _eventStoreConnection.AppendToStreamAsync(streamName, originalVersion, eventsToSave);

                aggregate.ClearPendingEvents();

                return result.NextExpectedVersion;
            }
            catch (WrongExpectedVersionException ex)
            {
                throw new /*Concurrency*/Exception("concurrency exception", ex);
            }
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
