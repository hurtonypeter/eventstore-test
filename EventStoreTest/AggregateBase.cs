using System;
using System.Collections.Generic;
using System.Text;

namespace EventStoreTest
{
    public interface IAggregate
    {
        int Version { get; }
        string Identifier { get; }
        void ApplyEvent(object @event);
        ICollection<object> GetPendingEvents();
        void ClearPendingEvents();
    }

    public abstract class AggregateBase : IAggregate
    {
        private readonly List<object> _pendingEvents = new List<object>();

        public Guid Id { get; set; }
        public string Identifier => $"{GetType().Name}";
        public int Version { get; private set; } = -1;

        void IAggregate.ApplyEvent(object @event)
        {
            ((dynamic)this).Apply((dynamic)@event);
            Version++;
        }

        public ICollection<object> GetPendingEvents()
        {
            return _pendingEvents;
        }

        public void ClearPendingEvents()
        {
            _pendingEvents.Clear();
        }

        protected void RaiseEvent(object @event)
        {
            ((IAggregate)this).ApplyEvent(@event);
            _pendingEvents.Add(@event);
        }
    }
}
