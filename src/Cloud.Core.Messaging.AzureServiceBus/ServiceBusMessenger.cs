namespace Cloud.Core.Messaging.AzureServiceBus
{
    using System;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Diagnostics.CodeAnalysis;
    using System.Linq;
    using System.Reactive;
    using System.Reactive.Linq;
    using System.Reactive.Subjects;
    using System.Threading;
    using System.Threading.Tasks;
    using Config;
    using Core;
    using Microsoft.Azure.Management.Fluent;
    using Microsoft.Azure.Management.ResourceManager.Fluent;
    using Microsoft.Azure.Management.ResourceManager.Fluent.Authentication;
    using Microsoft.Azure.Management.ResourceManager.Fluent.Core;
    using Microsoft.Azure.Services.AppAuthentication;
    using Microsoft.Extensions.Logging;
    using Microsoft.IdentityModel.Clients.ActiveDirectory;
    using Microsoft.Rest;
    using Microsoft.Rest.TransientFaultHandling;

    /// <summary>
    /// ServiceBus specific implementation of IMessenger and IReactiveMessenger.
    /// Implements the <see cref="IMessenger" />
    /// Implements the <see cref="IReactiveMessenger" />
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
        internal static readonly ConcurrentDictionary<string, string> ConnectionStrings = new ConcurrentDictionary<string, string>();

        private readonly MsiConfig _msiConfig;
        private readonly ServicePrincipleConfig _spConfig;
        private readonly ConnectionConfig _connConfig;

        private ServiceBusManager _sbInfo;
        internal bool Disposed;

        /// <summary>
        /// Gets or sets the configuration.
        /// </summary>
        /// <value>The configuration.</value>
        public object Config { get; }

        /// <summary>Name of the object instance.</summary>
        public string Name { get; set; }

        /// <summary>
        /// Gets the connection configuration.
        /// </summary>
        /// <value>The connection configuration.</value>
        internal ServiceBusManager ConnectionManager
        {
            get
            {
                // Initialise once.
                if (_sbInfo == null)
                {
                    string connectionString;
                    ConfigBase config;

                    // Get the service bus connection string (either directly from Connection Config OR build using Msi/Service Principle Config).
                    if (_connConfig != null)
                    {
                        // Connection string and config comes direction from ConnConfig object.
                        config = _connConfig;
                        connectionString = _connConfig.ConnectionString;
                        ConnectionStrings.TryAdd(_connConfig.InstanceName, connectionString);
                    }
                    else
                    {
                        // Otherwise config comes from either Msi/Mui OR Service Principle configs.  Connection string will be built on the fly.  
                        config = (ConfigBase)_msiConfig ?? _spConfig;
                        connectionString = BuildSbConnectionString().GetAwaiter().GetResult();
                    }

                    // Class to represent the built configurations.
                    _sbInfo = new ServiceBusManager(connectionString, config);

                    // Needs told to initialise to go get additional info.
                    _sbInfo.Initialise().GetAwaiter().GetResult();
                }

                return _sbInfo;
            }
        }

        /// <summary>
        /// Property representing IManager interface method.
        /// </summary>
        public IMessageEntityManager EntityManager => ConnectionManager;

        /// <summary>
        /// Initializes a new instance of ServiceBusMessenger with Managed Service Identity (MSI) authentication.
        /// </summary>
        /// <param name="config">The Msi ServiceBus configuration.</param>
        /// <param name="logger">The logger.</param>
        public ServiceBusMessenger([NotNull]MsiConfig config, ILogger logger = null)
        {
            // Ensure all required configuration is as expected.
            config.Validate();
            Name = config.InstanceName;

            Logger = logger;
            _msiConfig = config;
            Config = config;
        }

        /// <summary>
        /// Initializes a new instance of ServiceBusMessenger with Service Principle authentication.
        /// </summary>
        /// <param name="config">The Service Principle configuration.</param>
        /// <param name="logger">The logger.</param>
        public ServiceBusMessenger([NotNull]ServicePrincipleConfig config, ILogger logger = null)
        {
            // Ensure all required configuration is as expected.
            config.Validate();
            Name = config.InstanceName;

            Logger = logger;
            _spConfig = config;
            Config = config;
        }

        /// <summary>
        /// Initializes a new instance of the ServiceBusMessenger using a connection string.
        /// </summary>
        /// <param name="config">The connection string configuration.</param>
        /// <param name="logger">The logger.</param>
        public ServiceBusMessenger([NotNull]ConnectionConfig config, ILogger logger = null)
        {
            // Ensure all required configuration is as expected.
            config.Validate();
            Name = config.InstanceName;

            Logger = logger;
            _connConfig = config;
            Config = config;
        }

        /// <summary>
        /// Sends message to the service bus
        /// </summary>
        /// <typeparam name="T">Type of object on the entity.</typeparam>
        /// <param name="message">The message body to be sent.</param>
        /// <returns>Task.</returns>
        public async Task Send<T>(T message) where T : class
        {
            await Send(message, null);
        }

        /// <summary>
        /// Sends a message to Service Bus with properties.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="message">The message body to be sent.</param>
        /// <param name="properties">The properties of the message.</param>
        /// <returns>Task.</returns>
        public async Task Send<T>(T message, KeyValuePair<string, object>[] properties) where T : class
        {
            if (!QueueConnectors.ContainsKey((typeof(T))))
            {
                SetupConnectorType<T>();
            }

            var queue = (ServiceBusConnector<T>)QueueConnectors[typeof(T)];
            await queue.SendMessage(message, properties);
        }

        /// <summary>
        /// Send a batch of messages to Service Bus.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="messages">List of messages to send.</param>
        /// <param name="batchSize">Size of message batches to send in a single call. If set to zero, uses the default batch size from config.</param>
        /// <returns>Task.</returns>
        public async Task SendBatch<T>(IEnumerable<T> messages, int batchSize = 100) where T : class
        {
            KeyValuePair<string, object>[] placeholder = null;

            // Send the batch of messages with NO properties specified.
            // ReSharper disable once ExpressionIsAlwaysNull
            await SendBatch(messages, placeholder, batchSize);
        }

        /// <summary>
        /// Sends a message to Service Bus with properties.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="messages">List of messages to send</param>
        /// <param name="properties">The properties applied to all messages</param>
        /// <param name="batchSize">Size of message batches to send in a single call. If set to zero, uses the default batch size from config.</param>
        /// <returns>Task.</returns>
        public async Task SendBatch<T>(IEnumerable<T> messages, KeyValuePair<string, object>[] properties, int batchSize = 100) where T : class
        {
            // Setup the queue adapter if it doesn't exist.
            if (!QueueConnectors.ContainsKey(typeof(T)))
            {
                SetupConnectorType<T>();
            }

            var queue = (ServiceBusConnector<T>)QueueConnectors[typeof(T)];

            // Send messages as a batch with defined list of properties for all messages.
            await queue.SendBatch(messages, batchSize, properties);
        }

        /// <summary>
        /// Send a batch of messages all with a set list of properties.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="messages">List of messages to send</param>
        /// <param name="setProps">Function for setting properties for each message.</param>
        /// <param name="batchSize">Size of message batches to send in a single call. If set to zero, uses the default batch size from config.</param>
        /// <returns>Task.</returns>
        public async Task SendBatch<T>(IEnumerable<T> messages, Func<T, KeyValuePair<string, object>[]> setProps, int batchSize = 100) where T : class
        {
            // Setup the queue adapter if it doesn't exist.
            if (!QueueConnectors.ContainsKey(typeof(T)))
            {
                SetupConnectorType<T>();
            }

            var queue = (ServiceBusConnector<T>)QueueConnectors[typeof(T)];

            // Send messages as a batch, with a function to set the message properties.
            await queue.SendBatch(messages, batchSize, null, setProps);
        }

        /// <summary>
        /// Receives the specified success callback.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="successCallback">Callback to execute after a message batch has been received.</param>
        /// <param name="errorCallback">Callback to execute after an error occurs.</param>
        /// <param name="batchSize">Size of message batches to receive in a single call. If set to zero, uses the default batch size from config.</param>
        /// <exception cref="InvalidOperationException">Callback for this message type already configured. Only one callback per type is supported.</exception>
        public void Receive<T>(Action<T> successCallback, Action<Exception> errorCallback, int batchSize = 10) where T : class
        {
            System.Threading.Monitor.Enter(ReceiveGate);

            try
            {
                // If the subscription (callback) has already been setup for this type, then throw an error.
                if (MessageSubs.ContainsKey(typeof(T)))
                {
                    throw new InvalidOperationException("Callback for this message type already configured. Only one callback per type is supported.");
                }

                var queue = SetupConnectorType<T>();

                // Start ready for this type.
                queue.StartReading(batchSize);

                // Add to callback subscription list.
                MessageSubs.TryAdd(typeof(T), MessagesIn.OfType<T>().Subscribe(successCallback, errorCallback));
            }
            finally
            {
                System.Threading.Monitor.Exit(ReceiveGate);
            }
        }

        /// <summary>
        /// Starts to receive messages as an observable.
        /// </summary>
        /// <typeparam name="T">Type of object to receive</typeparam>
        /// <param name="batchSize">Size of message batches to receive in a single call. If set to zero, uses the default batch size from config.</param>
        /// <returns>IObservable{T}.</returns>
        public IObservable<T> StartReceive<T>(int batchSize = 10) where T : class
        {
            // If no subscriptions exist for this message, setup the new read.
            if (!MessageSubs.ContainsKey(typeof(T)))
            {
                var queue = SetupConnectorType<T>();
                queue.StartReading(batchSize);
            }

            // Return the observable to be subscribed to.
            return MessagesIn.OfType<T>();
        }

        /// <summary>
        /// Read a single message.
        /// </summary>
        /// <typeparam name="T">Type of object on the entity.</typeparam>
        /// <returns>IMessageItem&lt;T&gt;.</returns>
        public T ReceiveOne<T>() where T : class
        {
            return ReceiveOneEntity<T>()?.Body;
        }

        /// <summary>
        /// Receives the one entity.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <returns>IMessageEntity&lt;T&gt;.</returns>
        public IMessageEntity<T> ReceiveOneEntity<T>() where T : class
        {
            // Setup the queue adapter if it doesn't exist.
            if (!QueueConnectors.ContainsKey(typeof(T)))
            {
                SetupConnectorType<T>();
            }

            var queue = (ServiceBusConnector<T>)QueueConnectors[typeof(T)];

            // Start ready for this type.
            return queue.ReadOne().ConfigureAwait(false).GetAwaiter().GetResult();
        }

        /// <summary>
        /// Read a batch of typed messages in a synchronous manner.
        /// </summary>
        /// <typeparam name="T">Type of object on the entity.</typeparam>
        /// <param name="batchSize">Size of the batch.</param>
        /// <returns>IMessageItem&lt;T&gt;.</returns>
        public async Task<List<T>> ReceiveBatch<T>(int batchSize) where T : class
        {
            var messages = await ReceiveBatchEntity<T>(batchSize);
            return messages?.Select(m => m.Body).ToList();
        }

        /// <summary>
        /// Receives a batch of message in a synchronous manner of type IMessageEntity types.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="batchSize">Size of the batch.</param>
        /// <returns>IMessageEntity&lt;T&gt;.</returns>
        public async Task<List<IMessageEntity<T>>> ReceiveBatchEntity<T>(int batchSize) where T : class
        {
            // Setup the queue adapter if it doesn't exist.
            if (!QueueConnectors.ContainsKey(typeof(T)))
            {
                SetupConnectorType<T>();
            }

            var queue = (ServiceBusConnector<T>)QueueConnectors[typeof(T)];

            // Start ready for this type.
            return await queue.ReadBatch(batchSize);
        }

        /// <summary>
        /// Completes all messages on the receiver.
        /// </summary>
        public async Task CompleteAllMessages(CancellationTokenSource tokenSource = null) 
        {
            // Setup the queue adapter if it doesn't exist.
            if (!QueueConnectors.ContainsKey(typeof(object)))
            {
                SetupConnectorType<object>();
            }

            var queue = (ServiceBusConnector<object>)QueueConnectors[typeof(object)];

            // Start ready for this type.
            await queue.CompleteAll(tokenSource);
        }

        /// <summary>
        /// Read message properties for the passed message.
        /// </summary>
        /// <typeparam name="T">Type of message body.</typeparam>
        /// <param name="msg">Message body, used to identify actual Service Bus message.</param>
        /// <returns>IDictionary&lt;System.String, System.Object&gt;.</returns>
        public IDictionary<string, object> ReadProperties<T>(T msg) where T : class
        {
            if (!QueueConnectors.ContainsKey(typeof(T)))
            {
                SetupConnectorType<T>();
            }

            var queue = (ServiceBusConnector<T>)QueueConnectors[typeof(T)];

            // Read properties.
            return queue.ReadProperties(msg);
        }

        /// <summary>
        /// Gets the queue adapter for the type T if it exists.
        /// </summary>
        /// <typeparam name="T">Type of adapter.</typeparam>
        /// <returns>Message queue for the type T.</returns>
        /// <exception cref="InvalidOperationException">Messages of type {typeof(T).FullName}</exception>
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
            Monitor.Enter(CancelGate);

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
            catch
            {
                // do nothing here...
            }
            finally
            {
                Monitor.Exit(CancelGate);
            }
        }

        /// <summary>
        /// Completes the specified message.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="message">The message.</param>
        /// <returns>Task.</returns>
        public async Task Complete<T>(T message) where T : class
        {
            await GetQueueAdapterIfExists<T>().Complete(message).ConfigureAwait(false);
        }

        /// <summary>
        /// Completes multiple messages message.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="messages">The list of messages.</param>
        /// <returns>Task.</returns>
        public async Task CompleteAll<T>(IEnumerable<T> messages) where T : class
        {
            await GetQueueAdapterIfExists<T>().Complete(messages).ConfigureAwait(false);
        }

        /// <summary>
        /// Abandons the specified message.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="message">The message.</param>
        /// <returns>Task.</returns>
        public async Task Abandon<T>(T message) where T : class
        {
            await GetQueueAdapterIfExists<T>().Abandon(message).ConfigureAwait(false);
        }

        /// <summary>
        /// Abandons the specified message.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="message">The message.</param>
        /// <param name="propertiesToModify"></param>
        /// <returns>Task.</returns>
        public async Task Abandon<T>(T message, KeyValuePair<string, object>[] propertiesToModify) where T : class
        {
            await GetQueueAdapterIfExists<T>().Abandon(message, propertiesToModify).ConfigureAwait(false);
        }

        /// <summary>
        /// Defers the specified message.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="message">The message.</param>
        /// <returns>Task.</returns>
        public async Task Defer<T>(T message) where T : class
        {
            await GetQueueAdapterIfExists<T>().Defer(message).ConfigureAwait(false);
        }

        /// <summary>
        /// Defers the specified message.
        /// </summary>
        /// <typeparam name="T"></typeparam>    
        /// <param name="message">The message.</param>
        /// <param name="propertiesToModify"></param>
        /// <returns>Task.</returns>
        public async Task Defer<T>(T message, KeyValuePair<string, object>[] propertiesToModify) where T : class
        {
            await GetQueueAdapterIfExists<T>().Defer(message, propertiesToModify).ConfigureAwait(false);
        }

        /// <summary>
        /// Receive an existing batch of items that were previously deferred, using their sequence numbers.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="sequenceNumbers">List of message identities to lookup.</param>
        /// <returns>List of deferred messages.</returns>
        public async Task<List<T>> ReceiveDeferredBatch<T>(IEnumerable<long> sequenceNumbers) where T : class
        {
            // Setup the queue adapter if it doesn't exist.
            if (!QueueConnectors.ContainsKey(typeof(T)))
            {
                SetupConnectorType<T>();
            }

            var queue = (ServiceBusConnector<T>)QueueConnectors[typeof(T)];

            // Start ready for this type.
            var deferrals = await queue.ReceiveDeferred(sequenceNumbers);

            return deferrals?.Select(m => m.Body).ToList();
        }

        /// <summary>
        /// Receive an existing batch of items that were previously deferred, using their sequence numbers.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="sequenceNumbers">List of message identities to lookup.</param>
        /// <returns>List of deferred messages.</returns>
        public async Task<List<IMessageEntity<T>>> ReceiveDeferredBatchEntity<T>(IEnumerable<long> sequenceNumbers) where T : class
        {
            // Setup the queue adapter if it doesn't exist.
            if (!QueueConnectors.ContainsKey(typeof(T)))
            {
                SetupConnectorType<T>();
            }

            var queue = (ServiceBusConnector<T>)QueueConnectors[typeof(T)];

            // Start ready for this type.
            return await queue.ReceiveDeferred(sequenceNumbers);
        }

        /// <summary>
        /// Errors the specified message.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="message">The message.</param>
        /// <param name="reason">The reason.</param>
        /// <returns>Task.</returns>
        public async Task Error<T>(T message, string reason = null) where T : class
        {
            await GetQueueAdapterIfExists<T>().Error(message, reason).ConfigureAwait(false);
        }

        /// <summary>
        /// Interface method to allow retrieval of SignedAccessUrls for supported Message Providers.
        /// ServiceBus is not currently a supported Message Provider so it will error with "Not Implemented" if it is used.
        /// </summary>
        /// <param name="accessConfig"></param>
        /// <returns></returns>
        public string GetSignedAccessUrl(ISignedAccessConfig accessConfig)
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Sets the messenger up for sending / receiving a specific message of type <typeparamref name="T" />.
        /// </summary>
        /// <typeparam name="T">The type of the message we are setting up.</typeparam>
        /// <returns>The message queue adapter.</returns>
        internal ServiceBusConnector<T> SetupConnectorType<T>() where T : class
        {
            Monitor.Enter(SetupGate);

            try
            {
                // If no adapter for this type already exists, then create an instance.
                if (!QueueConnectors.ContainsKey(typeof(T)))
                {
                    var queue = new ServiceBusConnector<T>(ConnectionManager, MessagesIn.AsObserver(), Logger);
                    QueueConnectors.TryAdd(typeof(T), queue);
                }

                return (ServiceBusConnector<T>)QueueConnectors[typeof(T)];
            }
            finally
            {
                Monitor.Exit(SetupGate);
            }
        }

        /// <summary>
        /// Performs application-defined tasks associated with freeing, releasing, or resetting unmanaged resources.
        /// </summary>
        public void Dispose()
        {
            // Dispose of unmanaged resources.
            Dispose(true);

            // Suppress finalization.
            GC.SuppressFinalize(this);
        }

        /// <summary>
        /// Releases unmanaged and - optionally - managed resources.
        /// </summary>
        /// <param name="disposing"><c>true</c> to release both managed and unmanaged resources; <c>false</c> to release only unmanaged resources.</param>
        protected virtual void Dispose(bool disposing)
        {
            if (Disposed)
                return;

            if (disposing)
            {
                // Clear all message subscriptions.
                MessageSubs.Release();
                MessageSubs = null;

                // Clear all adapters.
                QueueConnectors.Release();
                QueueConnectors = null;
            }

            Disposed = true;
        }

        /// <summary>
        /// Builds the sb connection.
        /// </summary>
        /// <returns><see cref="ConnectionManager" /> generated.</returns>
        /// <exception cref="InvalidOperationException">
        /// Could not authenticate using Managed Service Identity, ensure the application is running in a secure context
        /// or
        /// Could not authenticate to {azureManagementAuthority} using supplied AppId: {_spConfig.AppId}
        /// or
        /// Could not find the a service bus instance in the subscription with ID {subscriptionId}
        /// </exception>
        /// <exception cref="ArgumentException">An exception occurred during service connection, see inner exception for more detail</exception>
        [ExcludeFromCodeCoverage]
        internal async Task<string> BuildSbConnectionString()
        {
            try
            {
                var instanceName = (_msiConfig != null) ? _msiConfig.InstanceName : _spConfig.InstanceName;

                // If we already have the connection string for this instance - don't go get it again.
                if (ConnectionStrings.TryGetValue(instanceName, out var connStr))
                {
                    return connStr;
                }

                const string azureManagementAuthority = "https://management.azure.com/";
                const string windowsLoginAuthority = "https://login.windows.net/";

                string token, subscriptionId, sharedAccessPolicy;

                // Generate authentication token using Msi Config if it's been specified, otherwise, use Service principle.
                if (_msiConfig != null)
                {
                    // Managed Service Identity (MSI) authentication.
                    var provider = new AzureServiceTokenProvider();
                    token = provider.GetAccessTokenAsync(azureManagementAuthority, _msiConfig.TenantId).GetAwaiter().GetResult();

                    if (string.IsNullOrEmpty(token))
                        throw new InvalidOperationException("Could not authenticate using Managed Service Identity, ensure the application is running in a secure context");

                    subscriptionId = _msiConfig.SubscriptionId;
                    instanceName = _msiConfig.InstanceName;
                    sharedAccessPolicy = _msiConfig.SharedAccessPolicyName;
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

                    subscriptionId = _spConfig.SubscriptionId;
                    instanceName = _spConfig.InstanceName;
                    sharedAccessPolicy = _spConfig.SharedAccessPolicyName;
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
                var azureManagement = Azure.Authenticate(client, string.Empty).WithSubscription(subscriptionId);
                var sbNamespace = (await azureManagement.ServiceBusNamespaces.ListAsync()).FirstOrDefault(s => s.Name == instanceName);
                
                // If the namespace is not found, throw an exception.
                if (sbNamespace == null)
                    throw new InvalidOperationException($"Could not find the a service bus instance in the subscription with ID {subscriptionId}");

                // Get the built connection string.
                var name = await sbNamespace.AuthorizationRules.GetByNameAsync(sharedAccessPolicy);
                var keys = await name.GetKeysAsync();
                var connectionString = keys.PrimaryConnectionString;

                // Cache the connection string off so we don't have to re-authenticate.
                if (!ConnectionStrings.ContainsKey(instanceName))
                {
                    ConnectionStrings.TryAdd(instanceName, connectionString);
                }

                // Return the connection string.
                return connectionString;

                // NOTE: Re-implement the code in previous version of Master Branch when the intent is to use shared access policies that are topic or queue level.  
                // When this happens, a connection string may need to be added to both the sender and receiver as a connection string generated using a lower level 
                // access policy will mean it needs a connection per send/receive.  This is why I have left this out for now (I can see from our solution we do not use 
                // low level Shared Access Policies at the moment either.
            }
            catch (Exception e)
            {
                Logger?.LogError(e, "An exception occurred during service bus connection");
                throw new ArgumentException("An exception occurred during service connection, see inner exception for more detail", e);
            }
        }

        /// <summary>
        /// Update the receiver to listen to a topic/subscription
        /// </summary>
        /// <param name="entityName">The name of the updated Topic to Listen to.</param>
        /// <param name="entitySubscriptionName">The name of the updated Subscription on the Topic.</param>
        /// <param name="entityFilter">A filter that will be applied to the entity if created through this method</param>
        /// <returns></returns>
        public async Task UpdateReceiver(string entityName, string entitySubscriptionName = null, KeyValuePair<string, string>? entityFilter = null)
        {
            // Clear all message subscriptions.
            MessageSubs?.Release();

            // Clear all adapters.
            QueueConnectors?.Release();

            await ConnectionManager.UpdateReceiver(entityName, entitySubscriptionName, ((ConfigBase)Config).Receiver.CreateEntityIfNotExists, entityFilter);

            // Short sleep while the settings are applied.
            await Task.Delay(5000);
        }
    }
}
