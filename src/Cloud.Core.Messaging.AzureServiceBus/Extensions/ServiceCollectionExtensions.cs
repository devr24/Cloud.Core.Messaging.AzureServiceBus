namespace Microsoft.Extensions.DependencyInjection
{
    using System;
    using Cloud.Core;
    using Cloud.Core.Extensions;
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
        /// <param name="key">Key to identify the named instance of the service bus singleton.</param>
        /// <param name="instanceName">Instance name of service bus.</param>
        /// <param name="tenantId">Tenant Id where service bus exists.</param>
        /// <param name="subscriptionId">Subscription within the tenancy to use for the service bus instance.</param>
        /// <param name="receiver">Receiver configuration (if any).</param>
        /// <param name="sender">Sender configuration (if any).</param>
        /// <param name="enableAutoBackoff">Back-off mechanism enabled (only works when both sender and receiver is configured).</param>
        /// <returns>Modified service collection with the IReactiveMessenger or IMessenger instance and NamedInstanceFactory{T} configured.</returns>
        public static IServiceCollection AddServiceBusSingletonNamed<T>(this IServiceCollection services, string key, string instanceName, string tenantId, string subscriptionId, ReceiverSetup receiver = null, SenderSetup sender = null, bool enableAutoBackoff = false)
            where T : IMessageOperations
        {
            return AddNamedInstance<T>(services, key, new ServiceBusMessenger(new MsiConfig
            {
                InstanceName = instanceName,
                TenantId = tenantId,
                SubscriptionId = subscriptionId,
                Receiver = receiver,
                Sender = sender,
                EnableAutobackOff = enableAutoBackoff
            }));
        }

        /// <summary>
        /// Add service bus singleton of type ServiceBusMessenger, using named properties (as opposed to passing MsiConfig/ServicePrincipleConfig etc).
        /// Will automatically use MsiConfiguration.
        /// </summary>
        /// <param name="services">Service collection to extend</param>
        /// <param name="key">Key to identify the named instance of the service bus singleton.</param>
        /// <param name="instanceName">Instance name of service bus.</param>
        /// <param name="tenantId">Tenant Id where service bus exists.</param>
        /// <param name="subscriptionId">Subscription within the tenancy to use for the service bus instance.</param>
        /// <param name="receiver">Receiver configuration (if any).</param>
        /// <param name="sender">Sender configuration (if any).</param>
        /// <param name="enableAutoBackoff">Back-off mechanism enabled (only works when both sender and receiver is configured).</param>
        /// <returns>Modified service collection with the ServiceBusMessenger and NamedInstanceFactory{ServiceBusMessenger} configured.</returns>
        public static IServiceCollection AddServiceBusSingletonNamed(this IServiceCollection services, string key, string instanceName, string tenantId, string subscriptionId, ReceiverSetup receiver = null, SenderSetup sender = null, bool enableAutoBackoff = false)
        {
            return AddNamedInstance<ServiceBusMessenger>(services, key, new ServiceBusMessenger(new MsiConfig
            {
                InstanceName = instanceName,
                TenantId = tenantId,
                SubscriptionId = subscriptionId,
                Receiver = receiver,
                Sender = sender,
                EnableAutobackOff = enableAutoBackoff
            }));
        }
        
        /// <summary>
        /// Add service bus singleton of type T, using connection string configuration.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="services">Service collection to extend</param>
        /// <param name="key">Key to identify the named instance of the service bus singleton.</param>
        /// <param name="config">The connection string configuration</param>
        /// <returns>Modified service collection with the IReactiveMessenger or IMessenger instance and NamedInstanceFactory{T} configured.</returns>
        public static IServiceCollection AddServiceBusSingletonNamed<T>(this IServiceCollection services, string key, ConnectionConfig config) where T : IMessageOperations
        {
            return AddNamedInstance<T>(services, key, new ServiceBusMessenger(config));
        }

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
        /// <param name="enableAutoBackoff">Back-off mechanism enabled (only works when both sender and receiver is configured).</param>
        /// <returns>Modified service collection with the IReactiveMessenger or IMessenger instance and NamedInstanceFactory{T} configured.</returns>
        public static IServiceCollection AddServiceBusSingleton<T>(this IServiceCollection services, string instanceName, string tenantId, string subscriptionId, ReceiverSetup receiver = null, SenderSetup sender = null, bool enableAutoBackoff = false)
            where T : IMessageOperations
        {
            return services.AddServiceBusSingletonNamed<T>(null, instanceName, tenantId, subscriptionId, receiver, sender, enableAutoBackoff);
        }

        /// <summary>
        /// Adds the service bus singleton instance and NamedInstanceFactory{ServiceBusInstance} configured.
        /// Uses MsiConfig to setup configuration.
        /// </summary>
        /// <param name="services">Service collection to extend.</param>
        /// <param name="config">The configuration.</param>
        /// <returns>Modified service collection with the IReactiveMessenger or IMessenger and NamedInstanceFactory{T} configured.</returns>
        public static IServiceCollection AddServiceBusSingleton<T>(this IServiceCollection services, MsiConfig config)
            where T : IMessageOperations
        {
            return AddNamedInstance<T>(services, null, new ServiceBusMessenger(config));
        }

        /// <summary>
        /// Adds the service bus singleton instance and NamedInstanceFactory{ServiceBusInstance} configured.
        /// Uses ConnectionConfig to setup configuration.
        /// </summary>
        /// <param name="services">Service collection to extend.</param>
        /// <param name="config">The configuration.</param>
        /// <returns>Modified service collection with the IReactiveMessenger or IMessenger and NamedInstanceFactory{T} configured.</returns>
        public static IServiceCollection AddServiceBusSingleton<T>(this IServiceCollection services, ConnectionConfig config)
            where T : IMessageOperations
        {
            return AddNamedInstance<T>(services, null, new ServiceBusMessenger(config));
        }

        /// <summary>
        /// Adds the service bus singleton instance and NamedInstanceFactory{ServiceBusInstance} configured.
        /// Uses ServicePrincipleConfig to setup configuration.
        /// </summary>
        /// <param name="services">Service collection to extend.</param>
        /// <param name="config">The configuration.</param>
        /// <returns>Modified service collection with the IReactiveMessenger or IMessenger and NamedInstanceFactory{T} configured.</returns>
        public static IServiceCollection AddServiceBusSingleton<T>(this IServiceCollection services, ServicePrincipleConfig config)
            where T : IMessageOperations
        {
            return AddNamedInstance<T>(services, null, new ServiceBusMessenger(config));
        }

        /// <summary>
        /// Add service bus singleton of type ServiceBusMessenger, using named properties (as opposed to passing MsiConfig/ServicePrincipleConfig etc).
        /// Will automatically use MsiConfiguration.
        /// </summary>
        /// <param name="services">Service collection to extend.</param>
        /// <param name="instanceName">Instance name of service bus.</param>
        /// <param name="tenantId">Tenant Id where service bus exists.</param>
        /// <param name="subscriptionId">Subscription within the tenancy to use for the service bus instance.</param>
        /// <param name="receiver">Receiver configuration (if any).</param>
        /// <param name="sender">Sender configuration (if any).</param>
        /// <param name="enableAutoBackoff">Back-off mechanism enabled (only works when both sender and receiver is configured).</param>
        /// <returns>Modified service collection with the ServiceBusMessenger and NamedInstanceFactory{ServiceBusMessenger} configured.</returns>
        public static IServiceCollection AddServiceBusSingleton(this IServiceCollection services, string instanceName, string tenantId, string subscriptionId, ReceiverSetup receiver = null, SenderSetup sender = null, bool enableAutoBackoff = false)
        {
            return services.AddServiceBusSingletonNamed<ServiceBusMessenger>(null, instanceName, tenantId, subscriptionId, receiver, sender, enableAutoBackoff);
        }

        /// <summary>
        /// Adds the service bus singleton instance and NamedInstanceFactory{ServiceBusInstance} configured.
        /// Uses MsiConfig to setup configuration.
        /// </summary>
        /// <param name="services">Service collection to extend.</param>
        /// <param name="config">The configuration.</param>
        /// <returns>Modified service collection with the ServiceBusMessenger and NamedInstanceFactory{ServiceBusMessenger} configured.</returns>
        public static IServiceCollection AddServiceBusSingleton(this IServiceCollection services, MsiConfig config)
        {
            return AddNamedInstance<ServiceBusMessenger>(services, null, new ServiceBusMessenger(config));
        }

        /// <summary>
        /// Adds the service bus singleton instance and NamedInstanceFactory{ServiceBusInstance} configured.
        /// Uses ConnectionConfig to setup configuration.
        /// </summary>
        /// <param name="services">Service collection to extend.</param>
        /// <param name="config">The configuration.</param>
        /// <returns>Modified service collection with the ServiceBusMessenger and NamedInstanceFactory{ServiceBusMessenger} configured.</returns>
        public static IServiceCollection AddServiceBusSingleton(this IServiceCollection services, ConnectionConfig config)
        {
            return AddNamedInstance<ServiceBusMessenger>(services, null, new ServiceBusMessenger(config));
        }

        /// <summary>
        /// Adds the service bus singleton instance and NamedInstanceFactory{ServiceBusInstance} configured.
        /// Uses ServicePrincipleConfig to setup configuration.
        /// </summary>
        /// <param name="services">Service collection to extend.</param>
        /// <param name="config">The configuration.</param>
        /// <returns>Modified service collection with the ServiceBusMessenger and NamedInstanceFactory{ServiceBusMessenger} configured.</returns>
        public static IServiceCollection AddServiceBusSingleton(this IServiceCollection services, ServicePrincipleConfig config)
        {
            return AddNamedInstance<ServiceBusMessenger>(services, null, new ServiceBusMessenger(config));
        }

        private static IServiceCollection AddNamedInstance<T>(IServiceCollection services, string key, ServiceBusMessenger instance)
            where T : INamedInstance
        {
            if (!key.IsNullOrEmpty())
            {
                instance.Name = key;
            }

            services.AddSingleton(typeof(T), instance);

            // Ensure there's a NamedInstance factory to allow named collections of the messenger.
            services.AddFactoryIfNotAdded<T>();

            return services;
        }
    }
}
