namespace Cloud.Core.Messaging.AzureServiceBus.Models
{
    /// <summary>
    /// Class containing Service Bus entity information.
    /// </summary>
    public class EntityInfo
    {
        /// <summary>
        /// Gets or sets the name of the entity.
        /// </summary>
        /// <value>The name of the entity.</value>
        public string EntityName { get; set; }
        /// <summary>
        /// Gets or sets the type of the entity.
        /// </summary>
        /// <value>The type of the entity.</value>
        public EntityType EntityType { get; set; }
        /// <summary>
        /// Gets or sets a value indicating whether this instance is premium.
        /// </summary>
        /// <value><c>true</c> if this instance is premium; otherwise, <c>false</c>.</value>
        public bool IsPremium { get; set; }
        /// <summary>
        /// Gets or sets the maximum message size bytes.
        /// </summary>
        /// <value>The maximum message size bytes.</value>
        public int MaxMessageSizeBytes { get; set; }
        /// <summary>
        /// Gets or sets the maximum entity size mb.
        /// </summary>
        /// <value>The maximum entity size mb.</value>
        public long MaxEntitySizeMb { get; set; }
        /// <summary>
        /// Gets or sets the current entity size mb.
        /// </summary>
        /// <value>The current entity size mb.</value>
        public long CurrentEntitySizeMb { get; set; }
        /// <summary>
        /// Gets or sets the message count.
        /// </summary>
        /// <value>The message count.</value>
        public EntityMessageCount MessageCount { get; set; }
        /// <summary>
        /// Gets or sets the percentage used.
        /// </summary>
        /// <value>The percentage used.</value>
        public decimal PercentageUsed { get; set; }
        /// <summary>
        /// Gets or sets the subscription count.
        /// </summary>
        /// <value>The subscription count.</value>
        public int SubscriptionCount { get; set; }
    }
}
