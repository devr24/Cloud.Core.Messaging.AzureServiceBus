namespace Microsoft.Extensions.DependencyInjection
{
    using System;
    using Cloud.Core;
    using Cloud.Core.Messaging.AzureServiceBus;
    using Cloud.Core.Messaging.AzureServiceBus.Config;
    using Cloud.Core.Messaging.AzureServiceBus.Models;

    /// <summary>
    /// Class Service Collection extensions.
    /// </summary>
    public static class ServiceCollectionExtensions
    {
        /// <summary>
        /// Add service bus singleton of type T, using named properties (as opposed to passing MsiConfig/ServicePrincipleConfig etc).
        /// Will automatically use MsiConfiguration.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="services">Service collection to extend</param>
        /// <param name="instanceName">Instance name of service bus.</param>
        /// <param name="tenantId">Tenant Id where service bus exists.</param>
        /// <param name="subscriptionId">Subscription within the tenancy to use for the service bus instance.</param>
        /// <param name="receiver">Receiver configuration (if any).</param>
        /// <param name="sender">Sender configuration (if any).</param>
        /// <param name="enableAutoBackoff">Backoff mechanism enabled (only works when both sender and receiver is configured).</param>
        /// <returns>Modified service collection with the IReactiveMessenger, IMessenger and NamedInstanceFactory{T} configured.</returns>
        public static IServiceCollection AddServiceBusSingleton<T>(this IServiceCollection services, string instanceName, string tenantId, string subscriptionId, ReceiverSetup receiver = null, SenderSetup sender = null, bool enableAutoBackoff = false)
            where T : IMessageOperations
        {
            return services.AddServiceBusSingletonNamed<T>(null, instanceName, tenantId, subscriptionId, receiver, sender, enableAutoBackoff);
        }

        /// <summary>
        /// Add service bus singleton of type T, using named properties (as opposed to passing MsiConfig/ServicePrincipleConfig etc).
        /// Will automatically use MsiConfiguration.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="services">Service collection to extend</param>
        /// <param name="key">Key to identify the named instance of the service bus singleton.</param>
        /// <param name="instanceName">Instance name of service bus.</param>
        /// <param name="tenantId">Tenant Id where service bus exists.</param>
        /// <param name="subscriptionId">Subscription within the tenancy to use for the service bus instance.</param>
        /// <param name="receiver">Receiver configuration (if any).</param>
        /// <param name="sender">Sender configuration (if any).</param>
        /// <param name="enableAutoBackoff">Backoff mechanism enabled (only works when both sender and receiver is configured).</param>
        /// <returns>Modified service collection with the IReactiveMessenger, IMessenger and NamedInstanceFactory{T} configured.</returns>
        public static IServiceCollection AddServiceBusSingletonNamed<T>(this IServiceCollection services, string key, string instanceName, string tenantId, string subscriptionId, ReceiverSetup receiver = null, SenderSetup sender = null, bool enableAutoBackoff = false)
            where T : IMessageOperations
        {
            var serviceBusInstance = new ServiceBusMessenger(new MsiConfig
            {
                InstanceName = instanceName,
                TenantId = tenantId,
                SubscriptionId = subscriptionId,
                Receiver = receiver,
                Sender = sender,
                EnableAutobackOff = enableAutoBackoff
            });

            if (!key.IsNullOrEmpty())
            {
                serviceBusInstance.Name = key;
            }

            services.AddSingleton(typeof(T), serviceBusInstance);
            services.AddFactoryIfNotAdded<IMessenger>();
            services.AddFactoryIfNotAdded<IReactiveMessenger>();
            return services;
        }

        /// <summary>
        /// Add service bus singleton of type T, using connection string configuration.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="services">Service collection to extend</param>
        /// <param name="key">Key to identify the named instance of the service bus singleton.</param>
        /// <param name="config">The connection string configuration</param>
        /// <returns>Modified service collection with the IReactiveMessenger, IMessenger and NamedInstanceFactory{T} configured.</returns>
        public static IServiceCollection AddServiceBusSingletonNamed<T>(this IServiceCollection services, string key, ConnectionConfig config) where T : IMessageOperations
        {
            var serviceBusInstance = new ServiceBusMessenger(config);

            if (!key.IsNullOrEmpty())
            {
                serviceBusInstance.Name = key;
            }

            services.AddSingleton(typeof(T), serviceBusInstance);
            services.AddFactoryIfNotAdded<IMessenger>();
            services.AddFactoryIfNotAdded<IReactiveMessenger>();
            return services;
        }

        /// <summary>
        /// Adds the service bus singleton.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="services">The services.</param>
        /// <param name="config">The configuration.</param>
        /// <returns>ServiceCollection.</returns>
        /// <exception cref="InvalidOperationException">Problem occurred while configuring Service Bus Manager Identify config</exception>
        public static IServiceCollection AddServiceBusSingleton<T>(this IServiceCollection services, MsiConfig config)
            where T : IMessageOperations
        {
            var serviceBusInstance = new ServiceBusMessenger(config);
            services.AddSingleton(typeof(T), serviceBusInstance);
            services.AddFactoryIfNotAdded<IMessenger>();
            services.AddFactoryIfNotAdded<IReactiveMessenger>();
            return services;
        }

        /// <summary>
        /// Adds the service bus singleton.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="services">The services.</param>
        /// <param name="config">The configuration.</param>
        /// <returns>IServiceCollection.</returns>
        public static IServiceCollection AddServiceBusSingleton<T>(this IServiceCollection services, ConnectionConfig config)
            where T : IMessageOperations
        {
            var serviceBusInstance = new ServiceBusMessenger(config);
            services.AddSingleton(typeof(T), serviceBusInstance);
            services.AddFactoryIfNotAdded<IMessenger>();
            services.AddFactoryIfNotAdded<IReactiveMessenger>();
            return services;
        }

        /// <summary>
        /// Adds the service bus singleton.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="services">The services.</param>
        /// <param name="config">The configuration.</param>
        /// <returns>IServiceCollection.</returns>
        public static IServiceCollection AddServiceBusSingleton<T>(this IServiceCollection services, ServicePrincipleConfig config)
            where T : IMessageOperations
        {
            var serviceBusInstance = new ServiceBusMessenger(config);
            services.AddSingleton(typeof(T), serviceBusInstance);
            services.AddFactoryIfNotAdded<IMessenger>();
            services.AddFactoryIfNotAdded<IReactiveMessenger>();
            return services;
        }
    }
}
