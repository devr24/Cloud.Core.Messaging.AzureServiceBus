namespace Cloud.Core.Messaging.AzureServiceBus.Models
{
    using System;
    using System.Collections.Generic;
    using System.Globalization;
    using System.Text;

    /// <summary>
    /// Receiver setup information, used when creating a connection to listen to messages from an entity.
    /// </summary>
    public class ReceiverSetup
    {
        internal const string DeadLetterQueue = "/$deadletterqueue";

        /// <summary>
        /// Gets or sets the name of the entity to receive from.
        /// </summary>
        /// <value>The name of the entity to receive from.</value>
        public string EntityName { get; set; }

        /// <summary>
        /// Gets or sets the entity subscription to receive from (if using topics, otherwise this remains null when using queues as its not applicable).
        /// </summary>
        /// <value>The entity subscription.</value>
        public string EntitySubscriptionName { get; set; }

        /// <summary>
        /// Gets or sets the entity filter that's applied if using a topic.
        /// </summary>
        /// <value>The entity filter.</value>
        public KeyValuePair<string, string>? EntityFilter { get; set; }

        /// <summary>
        /// Entity type to use - either a topic or queue (defaults to topic).
        /// </summary>
        /// <value>The type of the entity to use.</value>
        public EntityType EntityType { get; set; } = EntityType.Topic;

        /// <summary>
        /// Gets or sets a value indicating whether [read error (dead-letter) queue].
        /// </summary>
        /// <value><c>true</c> if [read error queue]; otherwise, <c>false</c>.</value>
        public bool ReadFromErrorQueue { get; set; }

        /// <summary>
        /// Gets or sets a value indicating whether to [create the receiver entity if it does not already exist].
        /// </summary>
        /// <value><c>true</c> if [create entity if not exists]; otherwise, <c>false</c> (don't auto create).</value>
        public bool CreateEntityIfNotExists { get; set; } 

        /// <summary>
        /// Gets or sets a value indicating whether to [support messages of string body type].  Extra overhead when processing string body types.
        /// Expects stream content otherwise but can handle both if this is enabled (default is [false] - not supported).
        /// </summary>
        /// <value><c>true</c> if [support string body type]; otherwise, <c>false</c>.</value>
        public bool SupportStringBodyType { get; set; }

        /// <summary>
        /// Gets or sets the maximum time to wait before a message lock is renewed.  Default is to renew every 60 seconds.
        /// Will be overridden when initializing if this is set to a value larger than the actual max allowed by the topic.
        /// </summary>
        /// <value>The lock renewal time in seconds.</value>
        public double LockRenewalTimeInSeconds { get; set; } = 60;

        /// <summary>
        /// Gets the lock time renewal threshold, this is 80% of the actual renewal lock time for safety reasons.  If the renewal
        /// lock is 1 minute (60 seconds), this will mean the lock will actually be renewed at 80% of that, which is 48 seconds.
        /// </summary>
        /// <value>The lock time threshold.</value>
        public double LockRenewalTimeThreshold => (long)Math.Floor(LockRenewalTimeInSeconds * 0.8);

        /// <summary>
        /// Gets or sets the occurance of "polling" in seconds (how often the message receiver queries service bus for new messages).
        /// </summary>
        /// <value>The poll frequency in seconds.</value>
        public double PollFrequencyInSeconds { get; set; } = 0.05;

        /// <summary>
        /// Validates this instance for basic setup information.
        /// </summary>
        /// <exception cref="System.ArgumentException">
        /// EntityName (topic or queue name) must be set when setting up receiver
        /// or
        /// Both EntityName (topic name) and EntitySubscriptionName (subscription name) must be set when setting up receiver
        /// </exception>
        public void Validate()
        {
            if (EntityName.IsNullOrEmpty())
                throw new ArgumentException("EntityName (topic or queue name) must be set when setting up receiver");

            if (EntityType == EntityType.Topic && EntitySubscriptionName.IsNullOrEmpty())
                throw new ArgumentException("Both EntityName (topic name) and EntitySubscriptionName (subscription name) must be set when setting up receiver");
        }

        /// <summary>
        /// Returns a <see cref="string" /> that represents this instance.
        /// </summary>
        /// <returns>A <see cref="string" /> that represents this instance.</returns>
        public override string ToString()
        {
            var sb = new StringBuilder();

            sb.Append(" EntityName: ");
            sb.AppendLine(EntityName);
            sb.Append(" EntitySubscription: ");
            sb.AppendLine(EntitySubscriptionName ?? "[NOT SET]");
            sb.Append(" EntityFilter: ");
            sb.AppendLine(EntityFilter.IsNullOrDefault() || !EntityFilter.HasValue ? "[NOT SET]" : string.Format(EntityFilter.Value.Key, EntityFilter.Value.Value, "{0}/{1}"));
            sb.Append(" CreateEntityIfNotExists: ");
            sb.AppendLine(CreateEntityIfNotExists.ToString(CultureInfo.InvariantCulture));
            sb.Append(" ReadFromErrorQueue: ");
            sb.AppendLine(ReadFromErrorQueue.ToString(CultureInfo.InvariantCulture));
            sb.Append(" SupportStringBodyType: ");
            sb.AppendLine(SupportStringBodyType.ToString(CultureInfo.InvariantCulture));
            sb.Append(" LockRenewalTimeInSeconds: ");
            sb.AppendLine(LockRenewalTimeInSeconds.ToString(CultureInfo.InvariantCulture));
            sb.Append(" LockTimeThreshold: ");
            sb.AppendLine(LockRenewalTimeThreshold.ToString(CultureInfo.InvariantCulture));
            sb.Append(" PollFrequencyInSeconds: ");
            sb.Append(PollFrequencyInSeconds.ToString(CultureInfo.InvariantCulture));

            return sb.ToString();
        }
        
        internal ReceiverInfo GetBase()
        {
            var destination = new ReceiverInfo();

            var sourceType = GetType();
            var destinationType = destination.GetType();

            foreach (var sourceProperty in sourceType.GetProperties())
            {
                var destinationProperty = destinationType.GetProperty(sourceProperty.Name);

                if (destinationProperty != null && destinationProperty.CanWrite)
                        destinationProperty.SetValue(destination, sourceProperty.GetValue(this, null), null);
            }

            return destination;
        }
    }

    /// <summary>
    /// Receiver informataion class, represents a mix of both ReceiverSetup and ReceiverInfo.
    /// Implements the <see cref="ReceiverSetup" />
    /// Implements the <see cref="ReceiverSetup" />
    /// Implements the <see cref="ReceiverSetup" />
    /// </summary>
    /// <seealso cref="ReceiverSetup" />
    /// <seealso cref="ReceiverSetup" />
    public class ReceiverInfo : ReceiverSetup
    {
        /// <summary>
        /// The maximum lock duration
        /// </summary>
        private TimeSpan _maxLockDuration;

        /// <summary>
        /// Gets the receiver full path for a queue and topic.
        /// </summary>
        /// <value>The receiver path.</value>
        public string ReceiverFullPath
        {
            get
            {
                // If this is a queue, not topic, so just return the entity.
                if (EntityType == EntityType.Queue)
                    return ReadFromErrorQueue ? $"{EntityName}{DeadLetterQueue}" : EntityName;

                // Otherwise build subscriptions path using topic information.
                return !EntityName.IsNullOrEmpty() && !EntitySubscriptionName.IsNullOrEmpty()
                    ? $"{EntityName}/subscriptions/{EntitySubscriptionName}{(ReadFromErrorQueue ? DeadLetterQueue : "")}" : null;
            }
        }

        /// <summary>
        /// Gets the maximum receiver entity size mb, i.e. max allowed size of a queue or topic.
        /// THIS IS READ FROM THE QUEUE OR TOPIC DIRECTLY.
        /// </summary>
        /// <value>The maximum receiver entity size mb.</value>
        public long MaxEntitySizeMb { get; internal set; }
        /// <summary>
        /// Gets the maximum entity size kb.
        /// </summary>
        /// <value>The maximum entity size kb.</value>
        public long MaxEntitySizeKb => MaxEntitySizeMb * 1000;
        /// <summary>
        /// Gets the maximum entity size bytes.
        /// </summary>
        /// <value>The maximum entity size bytes.</value>
        public long MaxEntitySizeBytes => MaxEntitySizeKb * 1000;

        /// <summary>
        /// Gets or sets the maximum duration a message may be locked.
        /// THIS IS READ FROM THE QUEUE OR TOPIC DIRECTLY.
        /// </summary>
        /// <value>The maximum duration of the lock.</value>
        public TimeSpan MaxLockDuration
        {
            get { return _maxLockDuration; }
            internal set
            {
                _maxLockDuration = value;

                // Ensure the requested lock renewal time is not greater than the max allowed 
                // time (threshold is used for renewal so this is fine as a fallback).
                if (LockRenewalTimeInSeconds < _maxLockDuration.Seconds)
                    LockRenewalTimeInSeconds = _maxLockDuration.Seconds;
            }
        }

        /// <summary>
        /// Returns a <see cref="string" /> that represents this instance.
        /// </summary>
        /// <returns>A <see cref="string" /> that represents this instance.</returns>
        public override string ToString()
        {
            var sb = new StringBuilder();

            sb.AppendLine($"RECEIVE FROM {EntityType.ToString().ToUpper() }");
            sb.AppendLine(base.ToString());
            sb.Append(" ReceiverFullPath: ");
            sb.AppendLine(ReceiverFullPath);
            sb.Append(" MaxEntitySizeMb: ");
            sb.AppendLine(MaxEntitySizeMb.ToString());
            sb.Append(" MaxLockDurationSeconds: ");
            sb.AppendLine(MaxLockDuration.Seconds.ToString());

            return sb.ToString();
        }
    }
}
