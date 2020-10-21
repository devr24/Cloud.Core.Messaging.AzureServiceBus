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
    using Core;
    using System.Diagnostics;
    using Microsoft.Azure.ServiceBus.InteropExtensions;
    using Exceptions;
    using Models;
    using Comparer;
    using System.Diagnostics.CodeAnalysis;
    using Extensions;

    /// <summary>
    /// Generics based message queue router from <see cref="IObservable{T}" /> through to the <see cref="QueueClient" />.
    /// Implements the <see cref="ServiceBusConnector" />
    /// </summary>
    /// <typeparam name="T">The type of the message that we are routing.</typeparam>
    /// <seealso cref="ServiceBusConnector" />
    /// <inheritdoc />
    [ExcludeFromCodeCoverage]
    internal sealed class ServiceBusConnector<T> : ServiceBusConnector where T : class
    {
        internal readonly ConcurrentDictionary<T, Message> Messages = new ConcurrentDictionary<T, Message>(ObjectReferenceEqualityComparer<T>.Default);
        internal readonly ConcurrentDictionary<T, Timer> LockTimers = new ConcurrentDictionary<T, Timer>(ObjectReferenceEqualityComparer<T>.Default);
        internal readonly IObserver<T> MessageIn;
        internal Timer ReadTimer;

        private bool _batchInProcess;
        internal const long HeaderSizeEstimate = 54; // start with the smallest header possible

        private readonly object _objLock = new object();

        /// <summary>
        /// Initializes a new instance of <see cref="ServiceBusConnector{T}" />.
        /// </summary>
        /// <param name="config">The configuration.</param>
        /// <param name="messagesIn">The <see cref="IObserver{IMessage}" /> used to push received messages into the pipeline.</param>
        /// <param name="logger">The logger.</param>
        /// <inheritdoc />
        public ServiceBusConnector([NotNull] ServiceBusManager config, [NotNull]IObserver<T> messagesIn, ILogger logger)
            : base(config, logger)
        {
            MessageIn = messagesIn;
        }

        /// <summary>
        /// Starts pooling the queue in order to read messages in Peek Lock mode.
        /// </summary>
        /// <param name="readBatchSize">Size of the read batch.</param>
        /// <exception cref="InvalidOperationException">Receiver Entity has not been configured</exception>
        internal void StartReading(int readBatchSize)
        {
            if (Config.ReceiverInfo == null)
                throw new InvalidOperationException("Receiver Entity has not been configured");

            if (Disposed || Receiver == null || Receiver.IsClosedOrClosing) return;

            // Ensure prefetch count matches the requested batch count to avoid lock
            // expiry issues with the messages.
            Receiver.PrefetchCount = readBatchSize;

            if (ReadTimer != null) return;
            ReadTimer = new Timer(
                _ =>
                {
                    // Only process a single message batch at any one time.  
                    // Also, don't process during back-off periods.
                    if (_batchInProcess || ShouldBackOff())
                        return;

                    Read(_, readBatchSize);
                },
                null,
                TimeSpan.FromSeconds(Config.ReceiverInfo.PollFrequencyInSeconds),    // start receiving now
                TimeSpan.FromSeconds(Config.ReceiverInfo.PollFrequencyInSeconds));   // default is every 0.05 seconds 
        }

        /// <summary>
        /// Stops pooling the queue for reading messages when subscribed.
        /// </summary>
        internal void StopReading()
        {
            ReadTimer?.Dispose();
            ReadTimer = null;
        }

        /// <summary>
        /// Sends the batch of messages to Service Bus.
        /// </summary>
        /// <param name="messages">The messages.</param>
        /// <param name="sendBatchSize">The number of messages sent in a batch.</param>
        /// <param name="properties">The message properties</param>
        /// <param name="setMessagesFunc">The set messages function.</param>
        /// <returns>Async Task.</returns>
        /// <exception cref="InvalidOperationException">Sender Entity has not been configured</exception>
        /// <exception cref="EntityDisabledException"></exception>
        /// <exception cref="EntityFullException"></exception>
        /// <exception cref="InvalidOperationException">Sender Entity has not been configured</exception>
        /// <exception cref="ArgumentOutOfRangeException">Sender Entity has not been configured</exception>
        [ExcludeFromCodeCoverage] // Skipped - too many edge cases that cant be tested.
        internal async Task SendBatch([NotNull] IEnumerable<T> messages, int sendBatchSize,
            KeyValuePair<string, object>[] properties = null, Func<T, KeyValuePair<string, object>[]> setMessagesFunc = null)
        {
            if (Disposed) return;

            if (Sender == null)
                throw new InvalidOperationException("Sender Entity has not been configured");

            var totalMsgsSent = 0;

            var enumerable = messages as T[] ?? messages.ToArray();
            try
            {
                var batchByteSize = 0L;
                var batchMsgCount = 0;
                var currentMsgBatch = new List<Message>();

                // Converts each message to a valid batch of messages and sends - will verify message batch size is allowed before sending.
                foreach (var msg in enumerable)
                {
                    // Setup message properties as caller wished.  This will either be with a set group of properties passed in (which applies to every message) OR
                    // a function will build the properties on a per message basis, allowing the called to set dynamic properties for each message.
                    var msgProps = properties ?? setMessagesFunc?.Invoke(msg);

                    // Get the converted message and its total bytes count.
                    // NOTE: the catch statement will catch when the total batch is larger than the allowed size.
                    var sbMessage = ConvertMessage(msg, msgProps);
                    var msgByteSize = sbMessage.Size + HeaderSizeEstimate;

                    // Size of total bytes after current message bytes are added.  Helps decide to go ahead and send now before the batch limit is reached.
                    var bytesAfterCurrent = batchByteSize + msgByteSize;

                    // If the total bytes will exceed the allowed batch limit OR batch size is that of the max allowed batch limit, then send now!
                    if (bytesAfterCurrent > Config.SenderInfo.MaxMessageBatchSizeBytes ||
                        batchMsgCount >= sendBatchSize)
                    {
                        await Sender.SendAsync(currentMsgBatch).ConfigureAwait(false);

                        // Increment total number of messages sent and then reset all other counters.
                        totalMsgsSent += batchMsgCount;
                        batchByteSize = 0;
                        batchMsgCount = 0;
                        currentMsgBatch.Clear();
                    }

                    currentMsgBatch.Add(sbMessage);
                    batchByteSize += msgByteSize;
                    batchMsgCount++;
                }

                // Catch any remaining messages.
                if (batchMsgCount > 0)
                    await Sender.SendAsync(currentMsgBatch);
            }
            catch (MessageSizeExceededException)
            {
                // If the batch size is exceeded during send, then resend any unsent items with a reduced batch size.
                var reducedBatchSize = Convert.ToInt32(Math.Floor(sendBatchSize * .55));
                await SendBatch(enumerable.Skip(totalMsgsSent).Take(enumerable.Count() - totalMsgsSent).ToList(), reducedBatchSize, properties);
            }
            catch (MessagingEntityDisabledException mex)
            {
                Logger?.LogWarning(mex, "Error occurred sending messages - entity is disabled");
                throw new EntityDisabledException(Config.SenderInfo.EntityName, mex.Message, mex);
            }
            catch (QuotaExceededException qex)
            {
                long maxSize = 0, currentSize = 0;

                // Build a meaningful entity full message.
                try
                {
                    long.TryParse(qex.Message.Substring("Size of entity in bytes:", ",").Trim(), out currentSize);
                    long.TryParse(qex.Message.Substring("Max entity size in bytes:", ".").Trim(), out maxSize);
                }
                catch (Exception)
                {
                    // Do nothing on error if there are any problems grabbing the size in bytes.
                }

                Logger?.LogWarning(qex, $"Error occurred sending messages - sender entity is full (max allowed: {maxSize}, current: {currentSize})");
                throw new EntityFullException(Config.SenderInfo.EntityName, qex.Message, currentSize, maxSize, qex);
            }
        }

        /// <summary>
        /// Sends the message.
        /// </summary>
        /// <param name="messageBody">The message body.</param>
        /// <param name="messageProperties">The message properties.</param>
        /// <exception cref="EntityDisabledException"></exception>
        /// <exception cref="EntityFullException"></exception>
        [ExcludeFromCodeCoverage] // Skipped - too many edge cases that cant be tested.
        internal async Task SendMessage(T messageBody, KeyValuePair<string, object>[] messageProperties = null)
        {
            if (Disposed) return;

            try
            {
                // Converts the message to a broker message and verifies its size is within the allowed size.
                var message = ConvertMessage(messageBody, messageProperties);
                await Sender.SendAsync(message).ConfigureAwait(false);
            }
            catch (MessagingEntityDisabledException mex)
            {
                Logger?.LogWarning(mex, "Error occurred sending message - entity is disabled");
                throw new EntityDisabledException(Config.SenderInfo.EntityName, mex.Message, mex);
            }
            catch (QuotaExceededException qex)
            {
                long maxSize = 0, currentSize = 0;

                // Build a meaningful entity full message.
                try
                {
                    long.TryParse(qex.Message.Substring("Size of entity in bytes:", ",").Trim(), out currentSize);
                    long.TryParse(qex.Message.Substring("Max entity size in bytes:", ".").Trim(), out maxSize);
                }
                catch (Exception)
                {
                    // Do nothing on error if there are any problems grabbing the size in bytes.
                }

                Logger?.LogWarning(qex, $"Error occurred sending messages - sender entity is full (max allowed: {maxSize}, current: {currentSize})");
                throw new EntityFullException(Config.SenderInfo.EntityName, qex.Message, currentSize, maxSize, qex);
            }
        }

        /// <summary>
        /// Convert from generic type to Azure specific message for sending.
        /// </summary>
        /// <param name="msg">Generic type message to serialize.</param>
        /// <param name="properties">Property collection to add to the message.</param>
        /// <returns>Azure message.</returns>
        private Message ConvertMessage(T msg, KeyValuePair<string, object>[] properties = null)
        {
            const string contentType = "application/json";

            var message = new Message(Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(msg)))
            {
                ContentType = contentType,
                Label = msg.GetType().FullName,
                UserProperties =
                {
                    { "Version", Config.SenderInfo.MessageVersionString }
                }
            };

            if (properties != null)
            {
                foreach (var prop in properties)
                {
                    message.UserProperties.AddOrUpdate(prop);

                    if (prop.Key.Equals("id", StringComparison.InvariantCultureIgnoreCase))
                    {
                        message.MessageId = prop.Value.ToString();
                    }
                }
            }

            // Ensure messages sent are within the allowed ServiceBus limit.  Header estimate added in for contingency (there are header properties in the message also). 
            ValidateMessageSize(message.Body.Length + HeaderSizeEstimate);

            return message;
        }

        /// <summary>
        /// Validates the size of the message is an allowed size before attempting to send.
        /// </summary>
        /// <param name="messageSizeBytes">The message size in bytes to check.</param>
        /// <exception cref="ArgumentOutOfRangeException">messageSizeBytes - Message body of {messageSizeBytes.ToSizeSuffix()} exceeds the allowed message limit of {Config.Config.SenderInfo.MaxMessageBatchSizeBytes} KB " +
        ///                     $"(namespace is {(Config.IsPremiumTier ? "Premium" : "Basic or Standard")}</exception>
        /// <exception cref="ArgumentOutOfRangeException">Message body of exceeds the allowed message limit.</exception>
        private void ValidateMessageSize(long messageSizeBytes)
        {
            if (messageSizeBytes > Config.SenderInfo.MaxMessageBatchSizeBytes)
            {
                throw new ArgumentOutOfRangeException("messageSizeBytes", messageSizeBytes,
                    $"Message body of {messageSizeBytes / 1000} KB exceeds the allowed message limit of {Config.SenderInfo.MaxMessageBatchSizeKb} KB " +
                    $"(namespace is {(Config.IsPremiumTier ? "Premium" : "Basic or Standard")} tier)");
            }
        }

        /// <summary>
        /// Check to see if the receiver says there are no messages BUT there are still messages on the topic/queue.
        /// If so, we renew the Receiver, which starts the problematic messages coming through.
        /// </summary>
        /// <param name="instantCheck">If set to <c>true</c> [instantly check] without worrying about the timer.</param>
        /// <returns>Boolean [true] if it was broken and [false] if it wasn't.</returns>
        private bool BrokenReceiverCheck(bool instantCheck = false)
        {
            // FIX: If we are receiving "null" when really there are messages on the queue, we know that 
            // the receiver is in an error state and we fix this by re-instantiating the receiver.  Only check count after the 5 seconds idle buffer.
            // StackOverflow article: https://stackoverflow.com/questions/53853306/renewlock-error-using-servicebus-subscriptionclient?noredirect=1#comment94574818_53853306
            if ((instantCheck && Messages.IsEmpty) ||
                // 60 seconds renewal backoff
                (ReceiverSleepTime.Elapsed.TotalSeconds > 60
                // Finally, only renew client IF we aren't actively processing any messages at the 
                // moment (renewal of new client will break current messages completing/erroring etc).
                && Messages.IsEmpty))
            {
                var messageCount = GetReceiverMessageCount().GetAwaiter().GetResult();
                Logger?.LogInformation($"There are {messageCount.ActiveEntityCount} active message(s) on the entity");

                // When there are active messages, setup the receiver again.
                if (messageCount.ActiveEntityCount > 0)
                {
                    SetupReader();
                    return true;
                }
            }

            return false;
        }

        /// <summary>
        /// [BATCHED] Read message call back.
        /// </summary>
        /// <param name="_">[Ignored]</param>
        /// <param name="readBatchSize">Size of the read batch.</param>
        /// <returns>Task.</returns>
        /// <exception cref="InvalidOperationException">Receiver Entity has not been configured</exception>
        [ExcludeFromCodeCoverage] // Skipped - too many edge cases that cant be tested.
        internal void Read([MaybeNull]object _, int readBatchSize)
        {
            lock (_objLock)
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
                    var messages = Receiver.ReceiveAsync(readBatchSize).GetAwaiter().GetResult();

                    if (messages == null)
                    {
                        // If we have null returned, double check we aren't getting null when there are
                        // actually items on the entity - BUG FIX FOR SB.
                        BrokenReceiverCheck();
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
                            Lock(message, messageBody).GetAwaiter().GetResult();
                        }
                    }

                    /***********************/
                    // NOTE: Re-add the task.run code to increase parallelism... for now its one-by-one processing.  
                    // Raise the event for processing.
                    foreach (var message in Messages)
                    {
                        // After message locked, raise process message event.
                        MessageIn?.OnNext(message.Key);
                    }
                }
                catch (ObjectDisposedException dex)
                {
                    Logger?.LogError(dex, "Trying to read when receiver is disposed - IGNORE");
                }
                catch (InvalidOperationException iex)
                {
                    Logger?.LogError(iex, "Error during read of service bus message");
                    MessageIn?.OnError(iex);
                }
                catch (Exception ex) when (ex is MessageLockLostException || ex is MessageNotFoundException || ex is ObjectDisposedException)
                {
                    Logger?.LogWarning(ex, "Error during message lock, lock was lost [THIS CAN BE IGNORED - already in use or already processed]");
                }
                catch (Exception e)
                {
                    Logger?.LogError(e, "Error during read of service bus message");

                    // Error on all other types of exception (http, timeout etc).
                    MessageIn?.OnError(e);
                }
                finally
                {
                    if (!Disposed)
                    {
                        // Set batch processing to finished and restart the read timer.
                        _batchInProcess = false;
                        StartReading(readBatchSize);
                    }
                }
            }
        }

        /// <summary>
        /// Completes all.
        /// </summary>
        /// <param name="tokenSource">The token source.</param>
        [ExcludeFromCodeCoverage] // Skipped - too many edge cases that cant be tested.
        internal async Task CompleteAll(CancellationTokenSource tokenSource = null)
        {
            do
            {
                // Check for operation cancellation.
                if (tokenSource != null && tokenSource.IsCancellationRequested)
                {
                    break;
                }

                try
                {
                    Receiver.PrefetchCount = 100;

                    // Get a single message from the receiver entity.
                    var messages = await Receiver.ReceiveAsync(1, TimeSpan.FromSeconds(5)).ConfigureAwait(false);

                    if (messages == null)
                    {
                        // If we have null returned, double check we aren't getting null when there are
                        // actually items on the entity - BUG FIX FOR SB.
                        if (BrokenReceiverCheck())
                        {
                            messages = await Receiver.ReceiveAsync(1, TimeSpan.FromSeconds(10)).ConfigureAwait(false);
                        }

                        // If messages are still null - do the count and potentially finish off.
                        if (messages == null)
                        {
                            var messageCount = await GetReceiverMessageCount();
                            if (messageCount.ActiveEntityCount == 0)
                            {
                                break;
                            }
                        }
                    }

                    if (messages != null)
                    {
                        foreach (var msg in messages)
                        {
                            await Receiver.CompleteAsync(msg.SystemProperties.LockToken);
                        }
                    }
                }
                catch (Exception e)
                {
                    Logger?.LogError(e, "Error during read of service bus message");
                }

            } while (true);
        }

        /// <summary>
        /// Read a batch of messages synchronously.
        /// </summary>
        /// <returns>Async task.</returns>
        internal async Task<List<IMessageEntity<T>>> ReadBatch(int batchSize)
        {
            var typedMessages = new List<IMessageEntity<T>>();
            try
            {

                Receiver = new MessageReceiver(Config.ConnectionString, Config.ReceiverInfo.ReceiverFullPath, ReceiveMode.PeekLock,
                    new RetryExponential(TimeSpan.FromMilliseconds(100), TimeSpan.FromMilliseconds(500), 3))
                {
                    PrefetchCount = 0
                };
                var maxTimeout = new Stopwatch();
                maxTimeout.Start();

                do
                {
                    // Get a single message from the receiver entity.
                    var messages = await Receiver.ReceiveAsync(batchSize, TimeSpan.FromSeconds(5));
                    if (messages != null)
                    {
                        foreach (var m in messages)
                        {
                            if (typedMessages.Count < batchSize)
                            {
                                // Convert the message body to generic type object.
                                var messageBody = GetTypedMessageContent(m);
                                if (messageBody != null)
                                {
                                    // If we do not already have this message in processing AND we can
                                    // successfully lock the message, then it can be returned from this wrapper.
                                    if (Messages.TryAdd(messageBody, m) &&
                                        await Lock(m, messageBody))
                                    {
                                        typedMessages.Add(new ServiceBusMessageEntity<T>
                                        {
                                            Body = messageBody, Properties = ReadProperties(messageBody)
                                        });
                                    }
                                }
                            }
                        }
                    }
                } while (typedMessages.Count < batchSize && maxTimeout.Elapsed.TotalSeconds < 60);
            }
            catch (InvalidOperationException iex)
            {
                Logger?.LogError(iex, "Error during read of service bus message");
                throw;
            }
            catch (Exception e)
            {
                Logger?.LogError(e, "Error during read of service bus message");
            }
            
            return typedMessages;
        }

        /// <summary>
        /// Read a single message.
        /// </summary>
        /// <returns>Async task.</returns>
        internal async Task<IMessageEntity<T>> ReadOne()
        {
            try
            {
                Receiver.PrefetchCount = 0;

                // Get a single message from the receiver entity.
                var messages = await Receiver.ReceiveAsync(1, TimeSpan.FromSeconds(5)).ConfigureAwait(false);

                if (messages == null)
                {
                    // If we have null returned, double check we aren't getting null when there are
                    // actually items on the entity - BUG FIX FOR SB.
                    if (BrokenReceiverCheck())
                        messages = await Receiver.ReceiveAsync(1, TimeSpan.FromSeconds(10)).ConfigureAwait(false);

                    if (messages == null)
                        return null;
                }

                var message = messages[0];

                // Convert the message body to generic type object.
                var messageBody = GetTypedMessageContent(message);
                if (messageBody != null)
                {
                    // If we do not already have this message in processing AND we can
                    // successfully lock the message, then it can be returned from this wrapper.
                    if (Messages.TryAdd(messageBody, message) &&
                        await Lock(message, messageBody).ConfigureAwait(false))
                    {
                        return new ServiceBusMessageEntity<T> { Body = messageBody, Properties = ReadProperties(messageBody) };
                    }
                }

                return null;
            }
            catch (InvalidOperationException iex)
            {
                Logger?.LogError(iex, "Error during read of service bus message");
                throw;
            }
            catch (Exception e)
            {
                Logger?.LogError(e, "Error during read of service bus message");
                return null;
            }
        }

        /// <summary>
        /// Receives the deferred messages.
        /// </summary>
        /// <param name="sequenceNumbers">The sequence numbers for deferred messages to lookup.</param>
        /// <returns>List&lt;IMessageEntity&lt;T&gt;&gt;.</returns>
        internal async Task<List<IMessageEntity<T>>> ReceiveDeferred(IEnumerable<long> sequenceNumbers)
        {
            try
            {
                Receiver.PrefetchCount = 0;

                var messages = await Receiver.ReceiveDeferredMessageAsync(sequenceNumbers);

                if (messages == null)
                    return null;

                var typedMessages = new List<IMessageEntity<T>>();

                foreach (var m in messages)
                {
                    // Convert the message body to generic type object.
                    var messageBody = GetTypedMessageContent(m);
                    if (messageBody != null)
                    {
                        // If we do not already have this message in processing AND we can
                        // successfully lock the message, then it can be added to the return list.
                        if (Messages.TryAdd(messageBody, m) && await Lock(m, messageBody).ConfigureAwait(false))
                        {
                            // If we do not already have this message in processing
                            // then it can be returned from this wrapper.
                            typedMessages.Add(new ServiceBusMessageEntity<T>
                            {
                                Body = messageBody, Properties = ReadProperties(messageBody)
                            });
                        }
                    }
                }

                return typedMessages;
            }
            catch (ServiceBusException e)
            {
                Logger?.LogError(e, "Error during read of service bus message");
                return null;
            }
            catch (InvalidOperationException iex)
            {
                Logger?.LogError(iex, "Error during read of service bus message");
                throw;
            }
            catch (Exception e)
            {
                Logger?.LogError(e, "Error during read of service bus message");
                return null;
            }
        }

        /// <summary>
        /// Reads the properties.
        /// </summary>
        /// <param name="message">The message to find properties for.</param>
        /// <returns>IDictionary&lt;System.String, System.Object&gt;.</returns>
        /// <exception cref="NotImplementedException"></exception>
        public IDictionary<string, object> ReadProperties(T message)
        {
            if (Messages.TryGetValue(message, out var msg))
            {
                var userProperties = msg.UserProperties;
                userProperties.AddOrUpdate("MessageId", msg.MessageId);
                userProperties.AddOrUpdate("ContentType", msg.ContentType);
                userProperties.AddOrUpdate("SequenceNumber", msg.SystemProperties.SequenceNumber);


                var properties = userProperties
                    .GroupBy(kv => kv.Key)
                    .ToDictionary(g => g.Key, g => g.First().Value);

                return properties;
            }

            return new Dictionary<string, object>();
        }

        /// <summary>
        /// Creates a perpetual lock on a message by continuously renewing it's lock.
        /// This is usually created at the start of a handler so that we guarantee that we still have a valid lock
        /// and we retain that lock until we finish handling the message.
        /// </summary>
        /// <param name="message">The message that we want to create the lock on.</param>
        /// <param name="messageBody">The message body.</param>
        /// <returns>Task.</returns>
        internal async Task<bool> Lock(Message message, T messageBody)
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
                            if (!Receiver.IsClosedOrClosing && Messages.TryGetValue(messageBody, out var msg))
                            {
                                try
                                {
                                    await Receiver.RenewLockAsync(msg.SystemProperties.LockToken).ConfigureAwait(false);
                                }
                                catch (ObjectDisposedException ex)
                                {
                                    Logger?.LogWarning(ex, "Error during message lock, service bus client disposed");
                                    Release(messageBody);
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
                            else
                            {
                                Release(messageBody);
                            }
                        },
                        null,
                        TimeSpan.FromSeconds(Config.ReceiverInfo.LockRenewalTimeThreshold),
                        TimeSpan.FromSeconds(Config.ReceiverInfo.LockRenewalTimeThreshold)));

                return true;
            }
            catch (Exception ex) when (ex is MessageLockLostException || ex is MessageNotFoundException)
            {
                BrokenReceiverCheck(); // Fallback check as we shouldn't have this error here.
                Logger?.LogWarning(ex, "Error during message lock, fallback initiated");
                Release(messageBody);
                return false;
            }
            catch (Exception e)
            {
                Logger?.LogError(e, "Error during message lock");

                // Catch all other types (timeout, http etc).
                MessageIn?.OnError(e);
                Release(messageBody);
                return false;
            }
        }

        /// <summary>
        /// Completes a message by doing the actual READ from the queue.
        /// </summary>
        /// <param name="messages">The message we want to complete.</param>
        /// <returns>Task.</returns>
        internal async Task Complete(IEnumerable<T> messages)
        {
            try
            {
                var messageLockTokens = new List<string>();
                foreach (var message in messages)
                {
                    if (!Messages.TryGetValue(message, out var msg))
                    {
                        if (message.GetPropertyValueByName("body") is T body)
                        {
                            Messages.TryGetValue(body, out msg);
                        }
                    }

                    if (msg == null) continue;

                    Release(message);

                    if (msg.SystemProperties.LockedUntilUtc < DateTime.Now)
                    {
                        Receiver.RenewLockAsync(msg.SystemProperties.LockToken).GetAwaiter().GetResult();
                    }

                    messageLockTokens.Add(msg.SystemProperties.LockToken);
                }
                if (messageLockTokens.Count > 0)
                {
                    await Receiver.CompleteAsync(messageLockTokens).ConfigureAwait(false);
                }
            }
            catch (Exception ex) when (ex is MessageLockLostException || ex is MessageNotFoundException)
            {
                Logger?.LogWarning(ex, "Error during message Complete, lock was lost [THIS CAN BE IGNORED - already in use or already processed]");
            }
            catch (Exception e)
            {
                Logger?.LogError(e, "Error during message Complete");

                MessageIn?.OnError(e);
            }
        }

        /// <summary>
        /// Completes a message by doing the actual READ from the queue.
        /// </summary>
        /// <param name="message">The message we want to complete.</param>
        /// <returns>Task.</returns>
        internal async Task Complete(T message)
        {
            await Complete(new List<T> { message });
        }

        /// <summary>
        /// Abandons a message by returning it to the queue.
        /// </summary>
        /// <param name="message">The message we want to abandon.</param>
        /// <param name="propertiesToModify">The message properties to modify</param>
        /// <param name="modifyMessagesFunc">The modify messages function</param>
        /// <returns>Task.</returns>
        internal async Task Abandon(T message, KeyValuePair<string, object>[] propertiesToModify = null,
            Func<T, KeyValuePair<string, object>[]> modifyMessagesFunc = null)
        {
            if (Messages.TryGetValue(message, out var msg))
            {
                try
                {
                    Release(message);
                    if (msg.SystemProperties.LockedUntilUtc < DateTime.Now)
                        Receiver.RenewLockAsync(msg.SystemProperties.LockToken).GetAwaiter().GetResult();

                    var msgProps = propertiesToModify ?? modifyMessagesFunc?.Invoke(message);

                    await Receiver
                        .AbandonAsync(msg.SystemProperties.LockToken, msgProps?.ToDictionary(x => x.Key, x => x.Value))
                        .ConfigureAwait(false);
                }
                catch (Exception ex) when (ex is MessageLockLostException || ex is MessageNotFoundException)
                {
                    Logger?.LogWarning(ex,
                        "Error during message Abandon, lock was lost [THIS CAN BE IGNORED - already in use or already processed]");
                }
                catch (Exception e)
                {
                    Logger?.LogError(e, "Error during message Abandon");

                    MessageIn?.OnError(e);
                }
            }
        }

        /// <summary>
        /// Sets a message to deferred state in the queue.
        /// </summary>
        /// <param name="message">The message we want to defer.</param>
        /// <param name="propertiesToModify">The message properties to modify</param>
        /// <param name="modifyMessagesFunc">The modify messages function</param>
        /// <returns>Task.</returns>
        internal async Task Defer(T message, KeyValuePair<string, object>[] propertiesToModify = null,
            Func<T, KeyValuePair<string, object>[]> modifyMessagesFunc = null)
        {
            if (Messages.TryGetValue(message, out var msg))
            {
                try
                {
                    Release(message);
                    var msgProps = propertiesToModify ?? modifyMessagesFunc?.Invoke(message);

                    await Receiver
                        .DeferAsync(msg.SystemProperties.LockToken, msgProps?.ToDictionary(x => x.Key, x => x.Value))
                        .ConfigureAwait(false);
                }
                catch (Exception ex) when (ex is MessageLockLostException || ex is MessageNotFoundException)
                {
                    Logger?.LogWarning(ex,
                        "Error during message Defer, lock was lost [THIS CAN BE IGNORED - already in use or already processed]");
                }
                catch (Exception e)
                {
                    Logger?.LogError(e, "Error during message Defer");

                    MessageIn?.OnError(e);
                }
            }
        }

        /// <summary>
        /// Errors a message by moving it specifically to the error (dead letter) queue.
        /// </summary>
        /// <param name="message">The message that we want to move to the error queue.</param>
        /// <param name="reason">(optional) The reason for "Erroring".</param>
        /// <returns>Task.</returns>
        internal async Task Error(T message, string reason = null)
        {
            if (Messages.TryGetValue(message, out var msg))
            {
                try
                {
                    Release(message);
                    if (reason != null)
                        msg.UserProperties.Add("Reason", reason);

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

                    MessageIn?.OnError(e);
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
            Messages.TryRemove(message, out _);

            LockTimers.TryRemove(message, out var objTimer);
            objTimer?.Dispose();
        }

        /// <summary>
        /// Extracts and deserializes a given message
        /// </summary>
        /// <param name="message">The message that needs deserialized</param>
        /// <returns>T.</returns>
        /// <exception cref="InvalidOperationException">Cannot access the message content for message {message.MessageId}</exception>
        internal T GetTypedMessageContent(Message message)
        {
            string content;

            // We need to support legacy service bus code - this code allows interop between old service bus SDK and new.
            if (message.Body == null)
                content = message.GetBody<string>();
            else
                content = Encoding.UTF8.GetString(message.Body);

            // Check for no content (we cannot process this).
            if (content.IsNullOrEmpty() && !Config.ReceiverInfo.SupportStringBodyType)
                throw new InvalidOperationException($"Cannot access the message content for message {message.MessageId}");

            if (Config.ReceiverInfo.SupportStringBodyType)
            {
                content = Regex.Replace(content, @"[^\u0020-\u007E]", string.Empty);
                content = content.Replace("@string3http://schemas.microsoft.com/2003/10/Serialization/", string.Empty);

                if (content.Length == 0 && typeof(T) == typeof(string))
                {
                    return string.Empty as T;
                }
            }

            try
            { 
                return JsonConvert.DeserializeObject<T>(content);
            }
            catch (Exception ex) when (ex is JsonReaderException || ex is JsonSerializationException)
            {
                // If we are actually expecting T to be a system type, just return without serialization.
                if (typeof(T).IsSystemType())
                {
                    return content as T;
                }

                Logger?.LogWarning($"Could not map message to {typeof(T)}, dead-lettering this message");

                // Dead letter the message if it does not serialize otherwise it will keep being picked up.
                Receiver.DeadLetterAsync(message.SystemProperties.LockToken,
                    $"Message dead-lettered due to json serialization exception (type was not the expected type of {typeof(T)}). Content was: {content}",
                    ex.Message).GetAwaiter().GetResult();
                return null;
            }
        }

        /// <summary>
        /// Releases unmanaged and - optionally - managed resources.
        /// </summary>
        /// <param name="disposing"><c>true</c> to release both managed and unmanaged resources; <c>false</c> to release only unmanaged resources.</param>
        /// <inheritdoc />
        protected override void Dispose(bool disposing)
        {
            if (Disposed)
                return;

            if (disposing)
            {
                LockTimers.Release();
                Messages.Clear();
                ReadTimer?.Dispose();

            }
            base.Dispose(disposing);
        }
    }

    /// <summary>
    /// Non generic message queue router from <see cref="IObservable{IMessage}" /> through to the <see cref="Receiver" />.
    /// Implements the <see cref="Cloud.Core.Messaging.AzureServiceBus.ServiceBusConnector" />
    /// </summary>
    /// <seealso cref="Cloud.Core.Messaging.AzureServiceBus.ServiceBusConnector" />
    internal abstract class ServiceBusConnector : IDisposable
    {
        private bool _shouldBackOffResult = true;
        private readonly object _backoffLock = new object();

        /// <summary>
        /// The configuration
        /// </summary>
        public readonly ServiceBusManager Config;
        /// <summary>
        /// The logger
        /// </summary>
        internal readonly ILogger Logger;
        /// <summary>
        /// The sender
        /// </summary>
        internal MessageSender Sender;
        /// <summary>
        /// The receiver
        /// </summary>
        internal MessageReceiver Receiver;
        /// <summary>
        /// The receiver sleep time
        /// </summary>
        internal readonly Stopwatch ReceiverSleepTime = new Stopwatch();
        /// <summary>
        /// The back off checker.
        /// </summary>
        internal Stopwatch BackOffChecker;
        /// <summary>
        /// The disposed
        /// </summary>
        internal bool Disposed;

        /// <summary>
        /// Initializes a new instance of the <see cref="ServiceBusConnector"/> class.
        /// </summary>
        /// <param name="config">The configuration.</param>
        /// <param name="logger">The logger.</param>
        protected ServiceBusConnector([NotNull]ServiceBusManager config, ILogger logger = null)
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
            if (Config.SenderInfo != null)
            {
                Logger?.LogDebug($"Instantiating message sender for entity {Config.SenderInfo.EntityName}");

                Sender = new MessageSender(Config.ConnectionString,
                    Config.SenderInfo.EntityName,
                    new RetryExponential(TimeSpan.FromMilliseconds(100), TimeSpan.FromMilliseconds(500), 3));
            }
        }

        /// <summary>
        /// Setups the service bus reader.
        /// </summary>
        internal void SetupReader()
        {
            if (Config.ReceiverInfo != null)
            {
                Logger?.LogDebug($"Instantiating message receiver for entity {Config.ReceiverInfo.EntityName}");

                ReceiverSleepTime.Reset();
                ReceiverSleepTime.Start();
                Receiver?.CloseAsync().GetAwaiter().GetResult();

                Receiver = new MessageReceiver(Config.ConnectionString,
                    Config.ReceiverInfo.ReceiverFullPath, ReceiveMode.PeekLock,
                    new RetryExponential(TimeSpan.FromMilliseconds(100), TimeSpan.FromMilliseconds(500), 3));
            }
        }

        /// <summary>
        /// Gets the active message count.
        /// </summary>
        /// <returns>Task&lt;System.Int64&gt;.</returns>
        internal async Task<EntityMessageCount> GetReceiverMessageCount()
        {
            // If the receiver has been setup then read the count.
            if (Config.ReceiverInfo != null)
            {
                try
                {
                    //TODO: Implement check for isDisabled.  I haven't done this right now as we need to make sure and surface that the topic/queue is disabled
                    // as theres a chance the caller will see the count is greater than zero BUT wont see that its disabled if they havent called that check explicitly.
                    return await Config.GetReceiverMessageCount();
                }
                catch (Exception ex)
                {
                    Logger?.LogWarning(ex, $"Error \"{ex.Message}\" during message count for {Config.ReceiverInfo.ReceiverFullPath} - [IGNORED]");
                }
            }

            return new EntityMessageCount();
        }

        /// <summary>
        /// If auto back-off is configured, we need to check if the back-off should take place.
        /// </summary>
        /// <returns>Task&lt;System.Boolean&gt;.</returns>
        internal bool ShouldBackOff()
        {
            // Don't check for back-off if not enabled OR if both sender and receiver entities are not configured.
            if (!Config.EnableAutoBackOff)
                return false;

            lock (_backoffLock)
            {
                // If the timer for checking is not running, enable it now.
                if (BackOffChecker == null)
                {
                    BackOffChecker = new Stopwatch();
                    _shouldBackOffResult = (Config.GetSenderEntityUsagePercentage().GetAwaiter().GetResult() > 90);
                    BackOffChecker.Start();

                    // Track is backing off public variable
                    Config.IsBackingOff = _shouldBackOffResult;
                }

                // If we've waited for more than 2 minutes, check the sender queue.
                if (BackOffChecker.Elapsed.Minutes > 2)
                {
                    // Reset back-off timer.
                    BackOffChecker.Stop();
                    BackOffChecker.Reset();

                    // If the percentage of the sender queue is greater than 90% then BACK-OFF! :-)
                    _shouldBackOffResult = (Config.GetSenderEntityUsagePercentage().GetAwaiter().GetResult() > 90);

                    // Restart timer.
                    BackOffChecker.Start();

                    // Track is backing off public variable
                    Config.IsBackingOff = _shouldBackOffResult;
                }


                // No need to back-off just now.
                return _shouldBackOffResult;
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
                Receiver?.CloseAsync().Wait();
                Sender?.CloseAsync().Wait();
            }

            Disposed = true;
        }
    }
}
