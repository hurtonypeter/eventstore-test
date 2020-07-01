using EventStore.Client;
using Microsoft.Extensions.DependencyInjection;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Net.Http;
using System.Net.Security;
using System.Text;
using System.Threading.Tasks;

namespace EventStoreTest
{
    public class OrderCreatedEvent
    {
        public Guid OrderId { get; set; }
        public Guid CustomerId { get; set; }
    }

    public class OrderItemAddedEvent
    {
        public Guid ItemId { get; set; }

        public int Quantity { get; set; }
    }

    public class OrderItemRemovedEvent
    {
        public Guid ItemId { get; set; }
    }

    public class Order : AggregateBase
    {

        public Guid CustomerId { get; set; }

        private readonly List<OrderItem> _items = new List<OrderItem>();
        public IReadOnlyList<OrderItem> Items => _items.AsReadOnly();

        public Order()
        {

        }

        public Order(Guid orderId, Guid customerId)
        {
            RaiseEvent(new OrderCreatedEvent
            {
                OrderId = orderId,
                CustomerId = customerId
            });
        }

        public void Apply(OrderCreatedEvent evt)
        {
            Id = evt.OrderId;
            CustomerId = evt.CustomerId;
        }

        public void Apply(OrderItemAddedEvent evt)
        {
            _items.Add(new OrderItem
            {
                ItemId = evt.ItemId,
                Quantity = evt.Quantity
            });
        }

        public void Apply(OrderItemRemovedEvent evt)
        {
            _items.RemoveAll(x => x.ItemId == evt.ItemId);
        }

        public void AddItem(Guid itemId, int quantity)
        {
            if (Items.Any(x => x.ItemId == itemId))
            {
                throw new /*Domain*/Exception("Item already exists in the order!");
            }

            RaiseEvent(new OrderItemAddedEvent
            {
                ItemId = itemId,
                Quantity = quantity
            });
        }

        public void RemoveItem(Guid itemId)
        {
            if (!Items.Any(x => x.ItemId == itemId))
            {
                throw new /*Domain*/Exception("Item does not exist in the order!");
            }

            RaiseEvent(new OrderItemRemovedEvent
            {
                ItemId = itemId
            });
        }
    }

    public class OrderItem
    {
        public Guid ItemId { get; set; }
        public int Quantity { get; set; }
    }

    class Program
    {
        static readonly Guid order1Id = new Guid("2badff42-20bd-4cf3-ac6d-ec070b30643a");
        static readonly Guid order2Id = new Guid("77d1535b-ffe0-4370-8bc4-3679823898b3");

        static readonly Guid customer1Id = new Guid("94653d38-e09a-44e7-a6bb-49459ac2fb99");
        static readonly Guid customer2Id = new Guid("942c7f81-fd46-4962-b557-0aa21427622b");

        static readonly Guid item1Id = new Guid("5fcaf602-4380-4980-a4af-97f90d51b918");
        static readonly Guid item2Id = new Guid("c1d51516-d980-4060-a0b2-9e9656b9f127");
        static readonly Guid item3Id = new Guid("c7ee4682-204b-4d41-a875-63a178791afb");
        static readonly Guid item4Id = new Guid("56a127f6-4aa1-4407-962f-a9485a17ab7b");

        static async Task Main(string[] args)
        {
            var client = new EventStoreClient(new EventStoreClientSettings
            {
                OperationOptions = {
                    TimeoutAfter = Debugger.IsAttached
                        ? new TimeSpan?()
                        : TimeSpan.FromSeconds(30)
                },
                CreateHttpMessageHandler = () => new SocketsHttpHandler
                {
                    SslOptions = new SslClientAuthenticationOptions
                    {
                        RemoteCertificateValidationCallback = delegate { return true; }
                    }
                }
            });

            var services = new ServiceCollection();
            services.AddTransient<IRepository, Repository>();
            services.AddSingleton<EventTypeResolver>(fact => new EventTypeResolver(typeof(Program).Assembly));
            services.AddSingleton<EventStoreClient>(fact => client);

            var provider = services.BuildServiceProvider();

            //// trans 1
            using (var scope = provider.CreateScope())
            {
                //var order1 = new Order(order1Id, customer1Id);
                //var order2 = new Order(order2Id, customer2Id);

                //var repo = scope.ServiceProvider.GetRequiredService<IRepository>();

                //await repo.SaveAsync(order1);
                //await repo.SaveAsync(order2);
            }

            //// trans 2
            using (var scope = provider.CreateScope())
            {
                var repo = scope.ServiceProvider.GetRequiredService<IRepository>();

                var order = await repo.GetByIdAsync<Order>(order1Id);

                order.AddItem(item1Id, 2);
                order.AddItem(item2Id, 3);

                await repo.SaveAsync(order);
            }

            //// trans 3
            using (var scope = provider.CreateScope())
            {
                var repo = scope.ServiceProvider.GetRequiredService<IRepository>();

                var order = await repo.GetByIdAsync<Order>(order1Id);

                order.RemoveItem(item1Id);
                order.AddItem(item3Id, 4);

                await repo.SaveAsync(order);
            }

            //// trans 4
            using (var scope = provider.CreateScope())
            {
                var repo = scope.ServiceProvider.GetRequiredService<IRepository>();

                var order = await repo.GetByIdAsync<Order>(order1Id);
            }
        }
    }
}
