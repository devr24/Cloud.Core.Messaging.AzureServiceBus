namespace Cloud.Core.Messaging.AzureServiceBus.Models
{
    using System;
    using System.Text;
    
    /// <summary>
    /// Sender setup information, used when creating a connection to send messages to an entity.
    /// </summary>
    public class SenderSetup
    {
        /// <summary>
        /// Gets or sets the name of the entity to send to.
        /// </summary>
        /// <value>The name of the entity.</value>
        public string EntityName { get; set; }

        /// <summary>
        /// Decides entity type to use - either a topic [if true] or queue [if false] - defaults to topic.
        /// </summary>
        /// <value><c>true</c> if [use topic]; otherwise, <c>false</c>. Default is to use topic.</value>
        public EntityType EntityType { get; set; } = EntityType.Topic;

        /// <summary>
        /// Gets or sets a value indicating whether to [create entity if it does not already exist].
        /// </summary>
        /// <value><c>true</c> if [create entity if not exists]; otherwise, <c>false</c> (don't auto create).</value>
        public bool CreateEntityIfNotExists { get; set; }

        /// <summary>
        /// Gets or sets the message version to be sent/received.
        /// </summary>
        /// <value>The message version.</value>
        public double MessageVersion { get; set; } = 1.0;
        /// <summary>
        /// Gets the message version string.
        /// </summary>
        /// <value>The message version string.</value>
        public string MessageVersionString => MessageVersion.ToString("0.00");

        /// <summary>
        /// Validates this instance for basic setup information.
        /// </summary>
        /// <exception cref="System.ArgumentException">EntityName (topic or queue name) must be set when setting up sender</exception>
        public void Validate()
        {
            if (EntityName.IsNullOrEmpty())
                throw new ArgumentException("EntityName (topic or queue name) must be set when setting up sender");
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
            sb.Append(" CreateEntityIfNotExists: ");
            sb.AppendLine(CreateEntityIfNotExists.ToString());
            sb.Append(" MessageVersion: ");
            sb.AppendLine(MessageVersionString);

            return sb.ToString();
        }

        internal SenderInfo GetBase()
        {
            var destination = new SenderInfo();

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
    ///Sender informataion class, represents a mix of both SenderInfo and SenderSetup.
    /// Implements the <see cref="SenderSetup" />
    /// </summary>
    /// <seealso cref="SenderSetup" />
    public class SenderInfo : SenderSetup
    {
        /// <summary>
        /// Gets the maximum sender entity size mb, i.e. max allowed size of a queue or topic.
        /// </summary>
        /// <value>The maximum sender entity size mb.</value>
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
        /// Gets the maximum message size bytes, used when sending message to ServiceBus.
        /// </summary>
        /// <value>The maximum message size bytes.</value>
        public int MaxMessageBatchSizeBytes { get; internal set; } = 256000;
        /// <summary>
        /// Gets the maximum message batch size kb.
        /// </summary>
        /// <value>The maximum message batch size kb.</value>
        public int MaxMessageBatchSizeKb => MaxMessageBatchSizeBytes / 1000;
        /// <summary>
        /// Gets the maximum message batch size mb.
        /// </summary>
        /// <value>The maximum message batch size mb.</value>
        public double MaxMessageBatchSizeMb => (double)MaxMessageBatchSizeKb / 1000;
        
        /// <summary>
        /// Returns a <see cref="string" /> that represents this instance.
        /// </summary>
        /// <returns>A <see cref="string" /> that represents this instance.</returns>
        public override string ToString()
        {
            var sb = new StringBuilder();

            sb.AppendLine($"SEND TO {EntityType.ToString().ToUpper() }");
            sb.Append(" MaxEntitySizeMb: ");
            sb.AppendLine(MaxEntitySizeMb.ToString());
            sb.AppendLine($" MaxMessageBatchSizeKb: {MaxMessageBatchSizeKb} ({MaxMessageBatchSizeMb} Mb)");
            sb.AppendLine(base.ToString());

            return sb.ToString();
        }
    }
}
