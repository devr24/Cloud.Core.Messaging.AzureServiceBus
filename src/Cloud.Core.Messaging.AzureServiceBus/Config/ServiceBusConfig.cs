namespace Cloud.Core.Messaging.AzureServiceBus.Config
{
    using System;
    using System.Text;

    public abstract class ServiceBusConfig
    {
        private int _batchSize = 30;

        /// <summary>
        /// Gets the maximum message size kb, used when sending message to ServiceBus.
        /// </summary>
        /// <value>The maximum message size kb.</value>
        public int MaxMessageSizeKb => IsPremiumTier ? 1024 : 256;

        /// <summary>
        /// Gets a value indicating whether the ServiceBus namespace instance is premium tier.
        /// </summary>
        /// <value><c>true</c> if this instance is premium tier; otherwise, <c>false</c>.</value>
        public bool IsPremiumTier { get; internal set; }

        /// <summary>
        /// Gets or sets the connection string.
        /// </summary>
        /// <value>
        /// The connection string.
        /// </value>
        internal string Connection { get; set; }

        /// <summary>
        /// Gets or sets the name of the entity which to send messages to.
        /// </summary>
        /// <value>
        /// The name of the entity.
        /// </value>
        public string SenderEntity { get; set; }

        /// <summary>
        /// Gets or sets the name of the entity which to receive messages from.
        /// </summary>
        /// <value>
        /// The name of the entity.
        /// </value>
        public string ReceiverEntity { get; set; }

        /// <summary>
        /// Gets or sets the name of the entity subscription to listen for messages.
        /// </summary>
        /// <value>
        /// The name of the entity subscription.
        /// </value>
        public string ReceiverSubscriptionName { get; set; }

        /// <summary>
        /// Gets the receiver full path for a queue and topic.
        /// </summary>
        /// <value>The receiver path.</value>
        public string ReceiverPath => IsTopic ? 
            !ReceiverEntity.IsNullOrEmpty() && !ReceiverSubscriptionName.IsNullOrEmpty() ?  $"{ReceiverEntity}/subscriptions/{ReceiverSubscriptionName}" : null
            :
            ReceiverEntity;

        /// <summary>
        /// Gets or sets a value indicating whether this instance is to use a topic [true] or queue [false].
        /// </summary>
        /// <value>
        ///   <c>true</c> if this instance is topic; otherwise, <c>false</c> indicating it's using queues.
        /// </value>
        public bool IsTopic { get; set; } = true;

        /// <summary>Gets or sets the frequency in which service bus is polled for new messages in seconds.</summary>
        /// <value>The poll frequency in seconds.</value>
        public double PollFrequencyInSeconds { get; set; } = 0.05;

        /// <summary>Gets or sets a value indicating whether [enable string body type support].</summary>
        /// <value>
        ///   <c>true</c> if [enable string body type support]; otherwise, <c>false</c>.</value>
        public bool EnableStringBodyTypeSupport { get; set; }
        
        /// <summary>
        /// Gets or sets the message version to be sent/received.
        /// </summary>
        /// <value>
        /// The message version.
        /// </value>
        public double MessageVersion { get; set; } = 1.0;

        internal string MessageVersionString => MessageVersion.ToString("0.0");
        internal double LockInSeconds { get; set; } = 30;
        internal double LockTimeThreshold => (long)Math.Floor(LockInSeconds * 0.8);

        internal int BatchSize
        {
            get => _batchSize <= 0 ? 10 : _batchSize;
            set => _batchSize = value;
        }

        /// <summary>
        /// Validates this instance.
        /// </summary>
        /// <exception cref="ArgumentException">
        /// ReceiverEntity OR SenderEntity must be set or When connecting to a topic, both ReceiverEntity and Receiver Subscription name must be set
        /// or when connecting to a topic, both ReceiverEntity and Receiver Subscription name must be set
        /// </exception>
        public virtual void Validate()
        {
            if (SenderEntity.IsNullOrEmpty() && ReceiverEntity.IsNullOrEmpty())
                throw new ArgumentException("ReceiverEntity OR SenderEntity must be set");

            if (IsTopic && (!ReceiverEntity.IsNullOrEmpty() && ReceiverSubscriptionName.IsNullOrEmpty()))
                throw new ArgumentException("When connecting to a topic, both ReceiverEntity and ReceiverSubscription name must be set");

            if (IsTopic && (!ReceiverSubscriptionName.IsNullOrEmpty() && ReceiverEntity.IsNullOrEmpty()))
                throw new ArgumentException("When connecting to a topic, both ReceiverEntity and ReceiverSubscription name must be set");
        }

        /// <summary>
        /// Returns a <see cref="string" /> that represents this instance.
        /// </summary>
        /// <returns>A <see cref="string" /> that represents this instance.</returns>
        public override string ToString()
        {
            var sb = new StringBuilder();
            sb.AppendLine("ReceiverEntity: ");
            sb.Append(ReceiverPath ?? "[NOTSET]");
            sb.AppendLine("SenderEntity: ");
            sb.Append(SenderEntity ?? "[NOTSET]");
            sb.AppendLine("MessageVersion: ");
            sb.Append(MessageVersionString);
            sb.AppendLine("IsTopic: ");
            sb.Append(IsTopic);
            sb.AppendLine("LockTime: ");
            sb.Append(LockInSeconds);
            sb.AppendLine("LockRenewalTime: ");
            sb.Append(LockTimeThreshold);
            sb.AppendLine("IsPremiumTier");
            sb.Append(IsPremiumTier);
            sb.AppendLine("MaxMessageSizeKb");
            sb.Append(MaxMessageSizeKb.ToString("0.00"));
            sb.AppendLine("PollFrequencyInSeconds: ");
            sb.Append(PollFrequencyInSeconds);

            return sb.ToString();
        }
    }
}
