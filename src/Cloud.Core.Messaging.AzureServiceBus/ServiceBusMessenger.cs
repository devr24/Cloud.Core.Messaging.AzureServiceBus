namespace Cloud.Core.Messaging.AzureServiceBus
{
    using Core;
    using JetBrains.Annotations;
    using System;
    using System.Collections.Generic;
    using System.Reactive;
    using System.Reactive.Linq;
    using System.Reactive.Subjects;
    using System.Threading.Tasks;
    using System.Collections.Concurrent;
    using System.Linq;
    using Microsoft.Azure.Management.Fluent;
    using Microsoft.Azure.Management.ResourceManager.Fluent;
    using Microsoft.Azure.Management.ResourceManager.Fluent.Authentication;
    using Microsoft.Azure.Management.ResourceManager.Fluent.Core;
    using Microsoft.Extensions.Logging;
    using Microsoft.IdentityModel.Clients.ActiveDirectory;
    using Microsoft.Rest;
    using Microsoft.Rest.TransientFaultHandling;
    using Microsoft.Azure.Services.AppAuthentication;
    using Config;
    using Microsoft.Azure.Management.ServiceBus.Fluent.Models;

    /// <summary>
    /// ServiceBus specific implementation of IMessenger and IReactiveMessenger.
    /// </summary>
    /// <seealso cref="IMessenger" />
    /// <seealso cref="IReactiveMessenger" />
    public class ServiceBusMessenger : IMessenger, IReactiveMessenger
    {
        internal readonly object CancelGate = new object();
        internal readonly object SetupGate = new object();
        internal readonly object ReceiveGate = new object();
        
        internal readonly ILogger Logger;

        internal readonly ISubject<object> MessagesIn = new Subject<object>();
        internal ConcurrentDictionary<Type, IDisposable> MessageSubs = new ConcurrentDictionary<Type, IDisposable>();
        internal ConcurrentDictionary<Type, ServiceBusConnector> QueueConnectors = new ConcurrentDictionary<Type, ServiceBusConnector>();
        
        private readonly MsiConfig _msiConfig;
        private readonly ServicePrincipleConfig _spConfig;
        private ServiceBusConfig _config;

        public ServiceBusConfig ConnectionConfig
        {
            get {
                if (_config == null)
                {
                    BuildSbConnection().GetAwaiter().GetResult();
                }

                return _config;
            }
        }

        /// <summary>Initializes a new instance of ServiceBusMessenger with Managed Service Identity (MSI) authentication.</summary>
        /// <param name="config">The Msi ServiceBus configuration.</param>
        /// <param name="logger">The logger.</param>
        public ServiceBusMessenger([NotNull]MsiConfig config, ILogger logger = null)
        {
            // Ensure all required configuration is as expected.
            config.Validate();

            Logger = logger;
            _msiConfig = config;
        }

        /// <summary>Initializes a new instance of ServiceBusMessenger with Service Principle authentication.</summary>
        /// <param name="config">The Service Principle configuration.</param>
        /// <param name="logger">The logger.</param>
        public ServiceBusMessenger([NotNull]ServicePrincipleConfig config, ILogger logger = null)
        {
            // Ensure all required configuration is as expected.
            config.Validate();

            Logger = logger;
            _spConfig = config;
        }

        /// <summary>Initializes a new instance of the ServiceBusMessenger using a connection string.</summary>
        /// <param name="config">The connection string configuration.</param>
        /// <param name="logger">The logger.</param>
        public ServiceBusMessenger([NotNull]ConnectionConfig config, ILogger<IReactiveMessenger> logger = null)
        {
            // Ensure all required configuration is as expected.
            config.Validate();

            Logger = logger;
            _config = config;
        }

        /// <inheritdoc />
        public async Task Send<T>(T message) where T : class
        {
            await SendBatch(new List<T> {message});
        }

        /// <summary>Send a batch of messages to Service Bus.</summary>
        /// <param name="messages">List of messages to send.</param>
        /// <param name="batchSize">Size of message batches to send in a single call. If set to zero, uses the default batch size from config.</param>
        /// <inheritdoc />
        public async Task SendBatch<T>(IList<T> messages, int batchSize = 0) where T : class
        {
            // Setup the queue adapter if it doesn't exist.
            if (!QueueConnectors.ContainsKey(typeof(T)))
            {
                SetupConnectorType<T>();
            }

            var queue = (ServiceBusConnector<T>)QueueConnectors[typeof(T)];

            if (batchSize != 0)
                queue.Config.BatchSize = batchSize;

            // Send messages as a batch.
            await queue.SendBatch(messages);
        }

        /// <param name="successCallback">Callback to execute after a message batch has been received.</param>
        /// <param name="errorCallback">Callback to execute after an error occurs.</param>
        /// <param name="batchSize">Size of message batches to receive in a single call. If set to zero, uses the default batch size from config.</param>
        /// <inheritdoc />
        public void Receive<T>(Action<T> successCallback, Action<Exception> errorCallback, int batchSize = 0) where T : class
        {
            System.Threading.Monitor.Enter(ReceiveGate);

            try
            {
                // If the subscription (callback) has already been setup for this type, then throw an error.
                if (MessageSubs.ContainsKey(typeof(T)))
                    throw new InvalidOperationException("Callback for this message type already configured. Only one callback per type is supported.");
                
                var queue = SetupConnectorType<T>();

                if (batchSize != 0)
                    queue.Config.BatchSize = batchSize;

                // Start ready for this type.
                queue.StartReading();

                // Add to callback subscription list.
                MessageSubs.TryAdd(typeof(T), MessagesIn.OfType<T>().Subscribe(successCallback, errorCallback));
            }
            finally
            {
                System.Threading.Monitor.Exit(ReceiveGate);
            }
        }

        /// <summary>
        /// Read a single message.
        /// </summary>
        /// <typeparam name="T">Type of object on the entity.</typeparam>
        public IMessageItem<T> ReceiveOne<T>() where T : class
        {
            // If no subscriptions exist for this message, setup the new read.
            if (!MessageSubs.TryGetValue(typeof(T), out var queue))
            {
                queue = SetupConnectorType<T>();
            }

            // Start ready for this type.
            return ((ServiceBusConnector<T>)queue).ReadOne().ConfigureAwait(false).GetAwaiter().GetResult();

        }

        /// <summary>
        /// Get the total number of messages that exist on the receiver entity.
        /// </summary>
        /// <typeparam name="T">Type of object on the entity.</typeparam>
        /// <returns>Awaitable task of total number of messages on entity.</returns>
        public async Task<long> GetReceiverMessageCount<T>() where T : class
        {
            // If no subscriptions exist for this message, setup the new read.
            if (!MessageSubs.TryGetValue(typeof(T), out var queue))
            {
                queue = SetupConnectorType<T>();
            }

            return await ((ServiceBusConnector<T>) queue).GetReceiverMessageCount().ConfigureAwait(false);
        }

        /// <summary>
        /// Starts the receive.
        /// </summary>
        /// <typeparam name="T">Type of object to receive</typeparam>
        /// <param name="batchSize">Size of message batches to receive in a single call. If set to zero, uses the default batch size from config.</param>
        /// <returns>IObservable{T}.</returns>
        public IObservable<T> StartReceive<T>(int batchSize = 0) where T : class
        {
            // If no subscriptions exist for this message, setup the new read.
            if (!MessageSubs.ContainsKey(typeof(T)))
            {
                var queue = SetupConnectorType<T>();

                if (batchSize != 0)
                    queue.Config.BatchSize = batchSize;

                queue.StartReading();
            }

            // Return the observable to be subscribed to.
            return MessagesIn.OfType<T>().AsObservable();
        }

        /// <summary>
        /// Gets the queue adapter for the type T if it exists.
        /// </summary>
        /// <typeparam name="T">Type of adapter.</typeparam>
        /// <returns>Message queue for the type T.</returns>
        /// <exception cref="InvalidOperationException"></exception>
        internal ServiceBusConnector<T> GetQueueAdapterIfExists<T>() where T : class
        {
            return QueueConnectors.TryGetValue(typeof(T), out var result)
                    ? (ServiceBusConnector<T>)result
                    : throw new InvalidOperationException($"Messages of type {typeof(T).FullName} have not been setup or have been cancelled");
        }

        /// <summary>
        /// Cancels the receive of messages.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        public void CancelReceive<T>() where T : class
        {

            System.Threading.Monitor.Enter(CancelGate);

            try
            {
                // If an adapter exists for the type T, stop reading.
                GetQueueAdapterIfExists<T>()?.StopReading();

                // Remove specific existing subscriptions
                MessageSubs.TryRemove(typeof(T), out var msgSub);
                msgSub?.Dispose();

                // Remove specific existing adapters.
                QueueConnectors.TryRemove(typeof(T), out var connector);
                connector?.Dispose();
            }
            finally
            {
                System.Threading.Monitor.Exit(CancelGate);
            }
        }
        
        /// <inheritdoc />
        public async Task Complete<T>(T message) where T : class
        {
            await GetQueueAdapterIfExists<T>().Complete(message).ConfigureAwait(false);
        }

        /// <inheritdoc />
        public async Task Abandon<T>(T message) where T : class
        {
            await GetQueueAdapterIfExists<T>().Abandon(message).ConfigureAwait(false);
        }

        /// <inheritdoc />
        public async Task Error<T>(T message) where T : class
        {
            await GetQueueAdapterIfExists<T>().Error(message).ConfigureAwait(false);
        }

        /// <summary>
        /// Sets the messenger up for sending / receiving a specific message of type <typeparamref name="T"/>.
        /// </summary>
        /// <typeparam name="T">The type of the message we are setting up.</typeparam>
        /// <returns>The message queue adapter.</returns>
        internal ServiceBusConnector<T> SetupConnectorType<T>() where T : class
        {
            System.Threading.Monitor.Enter(SetupGate);

            try
            {
                // If no adapter for this type already exists, then create an instance.
                if (!QueueConnectors.ContainsKey(typeof(T)))
                {
                    var queue = new ServiceBusConnector<T>(ConnectionConfig, MessagesIn.AsObserver(), Logger);
                    QueueConnectors.TryAdd(typeof(T), queue);
                }

                return (ServiceBusConnector<T>) QueueConnectors[typeof(T)];
            }
            finally
            {
                System.Threading.Monitor.Exit(SetupGate);
            }
        }

        /// <inheritdoc />
        public void Dispose()
        {
            // Clear all message subscriptions.
            MessageSubs.Release();
            MessageSubs = null;

            // Clear all adapters.
            QueueConnectors.Release();
            QueueConnectors = null;
        }

        /// <summary>
        /// Builds the sb connection.
        /// </summary>
        /// <returns><see cref="ConnectionConfig"/> generated.</returns>
        internal async Task BuildSbConnection()
        {
            try
            {
                string token, subscriptionName, instanceName, sharedAccessPolicy;
                bool isServiceLevelSap, isTopic;

                const string azureManagementAuthority = "https://management.azure.com/";
                const string windowsLoginAuthority = "https://login.windows.net/";

                // Generate authentication token using Msi Config if it's been specified, otherwise, use Service principle.
                if (_msiConfig != null)
                {
                    // Managed Service Identity (MSI) authentication.
                    var provider = new AzureServiceTokenProvider();
                    token = provider.GetAccessTokenAsync(azureManagementAuthority, _msiConfig.TenantId).GetAwaiter().GetResult();

                    if (string.IsNullOrEmpty(token))
                        throw new InvalidOperationException("Could not authenticate using Managed Service Identity, ensure the application is running in a secure context");
                    
                    _config = _msiConfig;
                    subscriptionName = _msiConfig.SubscriptionId;
                    instanceName = _msiConfig.InstanceName;
                    sharedAccessPolicy = _msiConfig.SharedAccessPolicy;
                    isServiceLevelSap = _msiConfig.IsServiceLevelSharedAccessPolicy;
                    isTopic = _msiConfig.IsTopic;
                }
                else
                { 
                    // Grab an authentication token from Azure.
                    var context = new AuthenticationContext(windowsLoginAuthority + _spConfig.TenantId);

                    var credential = new ClientCredential(_spConfig.AppId, _spConfig.AppSecret);
                    var tokenResult = await context.AcquireTokenAsync(azureManagementAuthority, credential);
                    
                    if (tokenResult == null)
                        throw new InvalidOperationException($"Could not authenticate to {azureManagementAuthority} using supplied AppId: {_spConfig.AppId}");

                    token = tokenResult.AccessToken;

                    _config = _spConfig;
                    subscriptionName = _spConfig.SubscriptionId;
                    instanceName = _spConfig.InstanceName;
                    sharedAccessPolicy = _spConfig.SharedAccessPolicy;
                    isServiceLevelSap = _spConfig.IsServiceLevelSharedAccessPolicy;
                    isTopic = _spConfig.IsTopic;
                }

                // Set credentials and grab the authenticated REST client.
                var tokenCredentials = new TokenCredentials(token);
                
                var client = RestClient.Configure()
                    .WithEnvironment(AzureEnvironment.AzureGlobalCloud)
                    .WithLogLevel(HttpLoggingDelegatingHandler.Level.None)
                    .WithCredentials(new AzureCredentials(tokenCredentials, tokenCredentials, string.Empty, AzureEnvironment.AzureGlobalCloud))
                    .WithRetryPolicy(new RetryPolicy(new HttpStatusCodeErrorDetectionStrategy(), new FixedIntervalRetryStrategy(3, TimeSpan.FromMilliseconds(500))))
                    .Build();

                // Authenticate against the management layer.
                var azureManagement = Azure.Authenticate(client, string.Empty).WithSubscription(subscriptionName);
                var sbNamespace = (await azureManagement.ServiceBusNamespaces.ListAsync()).FirstOrDefault(s => s.Name == instanceName);

                // If the namespace is not found, throw an exception.
                if (sbNamespace == null)
                    throw new InvalidOperationException($"Could not find the a service bus instance in the subscription with ID {subscriptionName}");

                // If this is premium tier service bus namespace, limit message send size.
                if (sbNamespace.Sku.Tier == SkuTier.Premium) 
                    _config.IsPremiumTier = true;

                // Build dynamic lock time.
                _config.LockInSeconds = (isTopic ? 
                    sbNamespace.Topics.GetByName(_config.ReceiverEntity).Subscriptions.GetByName(_config.ReceiverSubscriptionName).LockDurationInSeconds
                    : sbNamespace.Queues.GetByName(_config.ReceiverEntity).LockDurationInSeconds);

                // Connection string built up in different ways depending on the shared access policy level. Uses "global" (top service level) when isServiceLevelSAP [true].
                if (isServiceLevelSap)
                    _config.Connection = sbNamespace.AuthorizationRules.GetByName(sharedAccessPolicy).GetKeys().PrimaryConnectionString;
                else
                {
                    // Build dynamic connection string for Topic or queue.
                    _config.Connection = (isTopic ? 
                        sbNamespace.Topics.GetByName(_config.ReceiverEntity).AuthorizationRules.GetByName(sharedAccessPolicy).GetKeys().PrimaryConnectionString 
                        : sbNamespace.Queues.GetByName(_config.ReceiverEntity).AuthorizationRules.GetByName(sharedAccessPolicy).GetKeys().PrimaryConnectionString);
                    
                    // Remove the addition "EntityPath" text.
                    _config.Connection = _config.Connection.Replace($";EntityPath={_config.ReceiverEntity}", string.Empty);
                }
                
                Logger?.LogInformation($"Message lock duration:{_config.LockInSeconds} seconds, message lock renewed at: {_config.LockTimeThreshold} seconds");
                Logger?.LogInformation($"ServiceBus namespace is premium tier: {_config.IsPremiumTier}");
                Logger?.LogInformation($"Max send message size: {_config.MaxMessageSizeKb}");
            }
            catch (Exception e)
            {
                Logger?.LogError(e, "An exception occurred during service bus connection");
                throw new Exception("An exception occurred during service connection, see inner exception for more detail", e);
            }
        }
    }
}
