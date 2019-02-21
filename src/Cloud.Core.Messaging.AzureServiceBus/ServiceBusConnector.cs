namespace Cloud.Core.Messaging.AzureServiceBus
{
    using System.Text.RegularExpressions;
    using System.Collections.Concurrent;
    using Microsoft.Extensions.Logging;
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Text;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.Azure.ServiceBus;
    using Microsoft.Azure.ServiceBus.Core;
    using Newtonsoft.Json;
    using JetBrains.Annotations;
    using Core;
    using System.Diagnostics;
    using Microsoft.Azure.ServiceBus.Management;
    using Config;
    using Models;

    /// <summary>
    /// Generics based message queue router from <see cref="IObservable{T}"/> through to the <see cref="QueueClient"/>.
    /// </summary>
    /// <typeparam name="T">The type of the message that we are routing.</typeparam>
    /// <inheritdoc />
    internal sealed class ServiceBusConnector<T> : ServiceBusConnector where T : class
    {
        internal readonly ConcurrentDictionary<T, Message> Messages = new ConcurrentDictionary<T, Message>(ObjectReferenceEqualityComparer<T>.Default);
        internal readonly ConcurrentDictionary<T, Timer> LockTimers = new ConcurrentDictionary<T, Timer>(ObjectReferenceEqualityComparer<T>.Default);
        internal readonly IObserver<T> MessageIn;
        internal Timer ReadTimer;

        private const string ContentType = "application/json";
        private bool _batchInProcess;

        /// <summary>
        /// Initializes a new instance of <see cref="ServiceBusConnector{T}" />.
        /// </summary>
        /// <param name="config">The configuration.</param>
        /// <param name="messagesIn">The <see cref="IObserver{IMessage}" /> used to push received messages into the pipeline.</param>
        /// <param name="logger">The logger.</param>
        /// <inheritdoc />
        public ServiceBusConnector([NotNull] ServiceBusConfig config, [NotNull]IObserver<T> messagesIn, ILogger logger)
            : base(config, logger)
        {
            MessageIn = messagesIn;
        }

        /// <summary>
        /// Starts pooling the queue in order to read messages in Peek Lock mode.
        /// </summary>
        internal void StartReading()
        {
            if (Receiver == null)
                throw new InvalidOperationException("Receiver Entity has not been configured");

			// Ensure prefetch count matches the requested batch count to avoid lock
			// exiry issues with the messages.
            Receiver.PrefetchCount = Config.BatchSize;

            if (ReadTimer != null) return;
            ReadTimer = new Timer(
                async _ =>
                {
                    // Only process a single message batch at any one time.
                    if (_batchInProcess)
                        return;
                    
                    await Read(_);
                },
                null,
                TimeSpan.FromSeconds(Config.PollFrequencyInSeconds),    // start receiving now
                TimeSpan.FromSeconds(Config.PollFrequencyInSeconds));   // default is every 0.05 seconds 
        }

        /// <summary>
        /// Stops pooling the queue for reading messages.
        /// </summary>
        internal void StopReading()
        {
            ReadTimer?.Dispose();
            ReadTimer = null;
        }

        /// <summary>
        /// Sends the batch.
        /// </summary>
        /// <param name="messages">The messages.</param>
        /// <returns>Task.</returns>
        /// <exception cref="InvalidOperationException">Sender Entity has not been configured</exception>
        /// <exception cref="ArgumentOutOfRangeException">Message Size (kb) - Message body of {msgSize} kb exceeds the allowed message limit of {Config.MaxMessageSizeKb} kb (namespace is Premium OR (Basic/Standard))</exception>
        internal async Task SendBatch([NotNull] IList<T> messages)
        {
            if (Sender == null)
                throw new InvalidOperationException("Sender Entity has not been configured");

            var batchOfMessages = new List<Message>();
            for (var i = 0; i < messages.Count;)
            {
                var msgBatch = messages.Skip(i).Take(Config.BatchSize).Select(m => new Message(Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(m)))
                {
                    ContentType = ContentType,
                    Label = m.GetType().FullName,
                    UserProperties =
                    {
                        { "Version", Config.MessageVersionString }
                    }
                }).ToList();

                // Ensure messages sent are within the allowed ServiceBus limit.
                var oversizedMsg = msgBatch.FirstOrDefault(m => m.Body.Length / 1000 > Config.MaxMessageSizeKb);
                if (oversizedMsg != null)
                {
                    var msgSize = (oversizedMsg.Body.Length / 1000).ToString("0.00");
                    throw new ArgumentOutOfRangeException("Message Size (kb)", msgSize,
                        $"Message body of {msgSize} kb exceeds the allowed message limit of {Config.MaxMessageSizeKb} kb " +
                        $"(namespace is {(Config.IsPremiumTier ? "Premium" : "Basic or Standard")} tier)");
                }
                
                batchOfMessages.AddRange(msgBatch);

                await Sender.SendAsync(batchOfMessages).ConfigureAwait(false);

                // Clear batch.
                batchOfMessages.Clear();

                i = i + Config.BatchSize;
            }
        }

        /// <summary>
        /// [BATCHED] Read message call back.
        /// </summary>
        /// <param name="_">[Ignored]</param>
        internal async Task Read([CanBeNull]object _)
        {
			// If already processing, return from read 
			// (shouldn't happen, but in for a safeguard).
            if (_batchInProcess)
                return;

            if (Receiver == null)
                throw new InvalidOperationException("Receiver Entity has not been configured");

            // Set batch "in processing" by setting flag and stopping read timer.
            _batchInProcess = true;
            StopReading();

            
            try
            {
                var messages = await Receiver.ReceiveAsync(Config.BatchSize).ConfigureAwait(false);

                if (messages == null)
                {
                    // FIX: If we are receiving "null" when really there are messages on the queue, we know that 
                    // the receiver is in an error state and we fix this by re-instantiating the receiver.  Only check count after the 5 seconds idle buffer.
                    // StackOverflow article: https://stackoverflow.com/questions/53853306/renewlock-error-using-servicebus-subscriptionclient?noredirect=1#comment94574818_53853306
                    if (ReceiverSleepTime.Elapsed.Seconds > 5)
                    {
                        var activeMessages = GetReceiverMessageCount().GetAwaiter().GetResult();
                        Logger?.LogInformation($"There are {activeMessages} active message(s) on the entity");

                        // When there are active messages, setup the receiver again.
                        if (activeMessages > 0)
                            SetupReader();
                    }

                    return; // no more processing.
                }

                foreach (var message in messages)
                {
                    // Get the message content from the body.
                    var messageBody = GetTypedMessageContent(message);
                    if (messageBody != null)
                    {
                        Messages[messageBody] = message;

                        // auto lock when message is received and set renewal timer.
                        await Lock(message, messageBody).ConfigureAwait(false);
                    }
                }


                /***********************/
                // NOTE: Re-add the task.run code to increase parallelism... for now its one-by-one processing.  Alternatively, a 
                // parallel.for could be used to parse the messages to introduce parallelism.
                // Raise the event after all is locked.
                foreach (var message in Messages)
                {
                    // After message locked, raise process message event.
                    MessageIn.OnNext(message.Key);
                }
            }

            catch (Exception ex) when (ex is MessageLockLostException || ex is MessageNotFoundException || ex is ObjectDisposedException)
            {
                Logger?.LogWarning(ex, "Error during message lock, lock was lost [THIS CAN BE IGNORED - already in use or already processed]");
            }
            catch (Exception e)
            {
                Logger?.LogError(e, "Error during read of service bus message");

                // Error on all other types of exception (http, timeout etc).
                MessageIn.OnError(e);
            }
            finally
            {
                // Set batch processing to finished and restart the read timer.
                _batchInProcess = false;
                StartReading();
            }
        }

        /// <summary>
        /// Read a single message.
        /// </summary>
        internal async Task<IMessageItem<T>> ReadOne()
        {
            try
            { 
                var messages = await Receiver.ReceiveAsync(1, TimeSpan.FromSeconds(1)).ConfigureAwait(false);

                if (messages == null)
                {
                    return null;
                }

                var message = messages[0];

                var messageBody = GetTypedMessageContent(message);
                if (messageBody != null)
                {
                    await Lock(message, messageBody).ConfigureAwait(false);

                    if (Messages.TryAdd(messageBody, message))
                    {
                        return new MessageItem<T>
                        {
                            Body = messageBody,
                            Error = null
                        };
                    }
                    return new MessageItem<T>
                    {
                        Body = null,
                        Error = new Exception("Received messaged but failed to add to collection")
                    };
                }
                return new MessageItem<T>()
                {
                    Body = null,
                    Error = new Exception("Could not deserialize message")
                };
            }
            catch (Exception ex) when (ex is MessageLockLostException || ex is MessageNotFoundException || ex is ObjectDisposedException)
            {
                Logger?.LogWarning(ex, "Error during message lock, lock was lost [THIS CAN BE IGNORED - already in use or already processed]");
                return new MessageItem<T>()
                {
                    Body = null,
                    Error = ex
                };
            }
            catch (Exception e)
            {
                Logger?.LogError(e, "Error during read of service bus message");
                return new MessageItem<T>()
                {
                    Body = null,
                    Error = e
                };
            }
        }

        /// <summary>
        /// Creates a perpetual lock on a message by continuously renewing it's lock.
        /// This is usually created at the start of a handler so that we guarantee that we still have a valid lock
        /// and we retain that lock until we finish handling the message.
        /// </summary>
        /// <param name="message">The message that we want to create the lock on.</param>
        /// <param name="messageBody">The message body.</param>
        /// <returns>Task.</returns>
        internal async Task Lock(Message message, T messageBody)
        {
            try
            {
                await Receiver.RenewLockAsync(message.SystemProperties.LockToken).ConfigureAwait(false);

                // Remove before adding if already exists.
                LockTimers.TryRemove(messageBody, out _);

                LockTimers.TryAdd(
                    messageBody,
                    new Timer(
                        async _ =>
                        {
                            if (Messages.TryGetValue(messageBody, out var msg))
                            {
                                try
                                {
                                    await Receiver.RenewLockAsync(msg.SystemProperties.LockToken).ConfigureAwait(false);
                                }
                                catch (Exception ex) when (ex is MessageLockLostException || ex is MessageNotFoundException)
                                {
                                    Logger?.LogWarning(ex, "Error during message lock, lock was lost [THIS CAN BE IGNORED - already in use or already processed]");
                                    Release(messageBody);
                                }
                                catch (Exception e)
                                {
                                    Logger?.LogError(e, "Error during message lock");
                                    Release(messageBody);
                                }
                            }
                        },
                        null,
                        TimeSpan.FromSeconds(Config.LockTimeThreshold),
                        TimeSpan.FromSeconds(Config.LockTimeThreshold)));
            }
            catch (Exception ex) when (ex is MessageLockLostException || ex is MessageNotFoundException)
            {
                Logger?.LogWarning(ex, "Error during message lock, lock was lost [THIS CAN BE IGNORED - already in use or already processed]");
            }
            catch (Exception e)
            {
                Logger?.LogError(e, "Error during message lock");

                // Catch all other types (timeout, http etc).
                MessageIn.OnError(e);
                Release(messageBody);
            }
        }
        
        /// <summary>
        /// Completes a message by doing the actual READ from the queue.
        /// </summary>
        /// <param name="message">The message we want to complete.</param>
        internal async Task Complete(T message)
        {
            if (Messages.TryGetValue(message, out var msg))
            {
                try
                {
                    if (msg.SystemProperties.LockedUntilUtc < DateTime.Now)
                        Receiver.RenewLockAsync(msg.SystemProperties.LockToken).GetAwaiter().GetResult();

                    await Receiver.CompleteAsync(msg.SystemProperties.LockToken).ConfigureAwait(false);
                }
                catch (Exception ex) when (ex is MessageLockLostException || ex is MessageNotFoundException)
                {
                    Logger?.LogWarning(ex, "Error during message Complete, lock was lost [THIS CAN BE IGNORED - already in use or already processed]");
                }
                catch (Exception e)
                {
                    Logger?.LogError(e, "Error during message Complete");

                    MessageIn.OnError(e);
                }
                finally
                {
                    Release(message);
                }
            }
        }

        /// <summary>
        /// Abandons a message by returning it to the queue.
        /// </summary>
        /// <param name="message">The message we want to abandon.</param>
        internal async Task Abandon(T message)
        {
            if (Messages.TryGetValue(message, out var msg))
            {
                try
                {
                    if (msg.SystemProperties.LockedUntilUtc < DateTime.Now)
                        Receiver.RenewLockAsync(msg.SystemProperties.LockToken).GetAwaiter().GetResult();

                    await Receiver.AbandonAsync(msg.SystemProperties.LockToken).ConfigureAwait(false);
                }
                catch (Exception ex) when (ex is MessageLockLostException || ex is MessageNotFoundException)
                {
                    Logger?.LogWarning(ex, "Error during message Abandon, lock was lost [THIS CAN BE IGNORED - already in use or already processed]");
                }
                catch (Exception e)
                {
                    Logger?.LogError(e, "Error during message Abandon");

                    MessageIn.OnError(e);
                }
                finally
                {
                    Release(message);
                }
            }
        }

        /// <summary>
        /// Errors a message by moving it specifically to the error (dead letter) queue.
        /// </summary>
        /// <param name="message">The message that we want to move to the error queue.</param>
        internal async Task Error(T message)
        {
            if (Messages.TryGetValue(message, out var msg))
            {
                try
                {
                    if (msg.SystemProperties.LockedUntilUtc < DateTime.Now)
                        Receiver.RenewLockAsync(msg.SystemProperties.LockToken).GetAwaiter().GetResult();

                    await Receiver.DeadLetterAsync(msg.SystemProperties.LockToken).ConfigureAwait(false);
                }
                catch (Exception ex) when (ex is MessageLockLostException || ex is MessageNotFoundException)
                {
                    Logger?.LogWarning(ex, "Error during message DeadLetter, lock was lost [THIS CAN BE IGNORED - already in use or already processed]");
                }
                catch (Exception e)
                {
                    Logger?.LogError(e, "Error during message DeadLetter");

                    MessageIn.OnError(e);
                }
                finally
                {
                    Release(message);
                }
            }
        }

        /// <summary>
        /// Releases a message from the Queue by releasing all the specific message resources like lock
        /// renewal timers.
        /// This is called by all the methods that terminate the life of a message like COMPLETE, ABANDON and ERROR.
        /// </summary>
        /// <param name="message">The message that we want to release.</param>
        internal void Release([NotNull]T message)
        {
            Messages.TryRemove(message, out _ );

            LockTimers.TryRemove(message, out var objTimer);
            objTimer?.Dispose();
        }

        /// <summary>
        /// Extracts and deserializes a given message
        /// </summary>
        /// <param name="message">The message that needs deserialized</param>
        internal T GetTypedMessageContent(Message message)
        {
            var content = Encoding.UTF8.GetString(message.Body);
            try
            {
                if (Config.EnableStringBodyTypeSupport)
                {
                    content = Regex.Replace(content, @"[^\u0020-\u007E]", string.Empty);
                    content = content.Replace("@string3http://schemas.microsoft.com/2003/10/Serialization/",
                        string.Empty);
                }

                return JsonConvert.DeserializeObject<T>(content);
            }
            catch (Exception ex) when (ex is JsonReaderException || ex is JsonSerializationException)
            {
                Logger?.LogWarning($"Could not map message to {typeof(T)}, dead-lettering this message");

                // Dead letter the message if it does not serialize otherwise it will keep being picked up.
                Receiver.DeadLetterAsync(message.SystemProperties.LockToken,
                    $"Message dead-lettered due to json serialization exception (type was not the expected type of {typeof(T)}). Content was: {content}",
                    ex.Message).GetAwaiter().GetResult();
                return null;
            }
        }

        /// <inheritdoc />
        public override void Dispose()
        {
            LockTimers.Release();
            Messages.Clear();
            ReadTimer?.Dispose();
            base.Dispose();
        }
    }


    /// <summary>
    /// Non generic message queue router from <see cref="IObservable{IMessage}"/> through to the <see cref="Receiver"/>.
    /// </summary>
    internal abstract class ServiceBusConnector : IDisposable
    {
        public readonly ServiceBusConfig Config;
        internal readonly ILogger Logger;
        internal MessageSender Sender;
        internal MessageReceiver Receiver;
        internal readonly Stopwatch ReceiverSleepTime = new Stopwatch();
        internal ManagementClient SubscriptionManager;
        
        internal ServiceBusConnector([NotNull]ServiceBusConfig config, ILogger logger = null)
        {
            Logger = logger;
            Config = config;

            SetupSender();
            SetupReader();
        }

        /// <summary>
        /// Setups the service bus sender.
        /// </summary>
        internal void SetupSender()
        {
            if (!Config.SenderEntity.IsNullOrEmpty())
            {
                Sender = new MessageSender(Config.Connection, Config.SenderEntity, new RetryExponential(TimeSpan.FromMilliseconds(100), TimeSpan.FromMilliseconds(500), 3));
            }
        }

        /// <summary>
        /// Setups the service bus reader.
        /// </summary>
        internal void SetupReader()
        {
            if (!Config.ReceiverPath.IsNullOrEmpty())
            {
                Logger?.LogDebug("Instantiating message receiver");

                ReceiverSleepTime.Reset();
                ReceiverSleepTime.Start();
                Receiver?.CloseAsync().GetAwaiter().GetResult();

                Receiver = new MessageReceiver(Config.Connection, Config.ReceiverPath, ReceiveMode.PeekLock, new RetryExponential(TimeSpan.FromMilliseconds(100), TimeSpan.FromMilliseconds(500), 3), Config.BatchSize);
            }
        }

        /// <summary>
        /// Gets the active message count.
        /// </summary>
        /// <returns>Task&lt;System.Int64&gt;.</returns>
        internal async Task<long> GetReceiverMessageCount()
        {
            // If the receiver has been setup then read the count.
            if (!Config.ReceiverPath.IsNullOrEmpty())
            {
                try
                {
                    // If the subscription manager has not been set - set it now.
                    if (SubscriptionManager == null)
                        SubscriptionManager = new ManagementClient(Config.Connection);

                    var runtimeInfo = await SubscriptionManager.GetSubscriptionRuntimeInfoAsync(Config.ReceiverEntity, Config.ReceiverSubscriptionName);
                    var messageCount = runtimeInfo.MessageCountDetails.ActiveMessageCount;

                    return messageCount;
                }
                catch (Exception ex)
                {
                    Logger?.LogWarning(ex, $"Error \"{ex.Message}\" during message count for {Config.ReceiverEntity}/subscriptions/{Config.ReceiverSubscriptionName} - [IGNORED]");
                }
            }

            return -1;
        }

        /// <inheritdoc />
        public virtual void Dispose()
        {
            Receiver?.CloseAsync().Wait();
            Sender?.CloseAsync().Wait();
        }
    }

}
