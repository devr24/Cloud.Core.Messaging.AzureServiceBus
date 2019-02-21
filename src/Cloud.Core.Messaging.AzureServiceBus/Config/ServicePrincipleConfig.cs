namespace Cloud.Core.Messaging.AzureServiceBus.Config
{
    using System;

    /// <summary>Service Principle configuration used to connect to an instance of ServiceBus.</summary>
    public class ServicePrincipleConfig : ServiceBusConfig
    {
        /// <summary>
        /// Gets or sets the application identifier.
        /// </summary>
        /// <value>
        /// The application identifier.
        /// </value>
        public string AppId { get; set; }

        /// <summary>
        /// Gets or sets the application secret.
        /// </summary>
        /// <value>
        /// The application secret string.
        /// </value>
        public string AppSecret { get; set; }

        /// <summary>
        /// Gets or sets the tenant identifier.
        /// </summary>
        /// <value>
        /// The tenant identifier.
        /// </value>
        public string TenantId { get; set; }

        /// <summary>
        /// Gets or sets the subscription identifier.
        /// </summary>
        /// <value>
        /// The subscription identifier.
        /// </value>
        public string SubscriptionId { get; set; }

        /// <summary>
        /// Gets or sets the name of the service bus instance.
        /// </summary>
        /// <value>
        /// The name of the service bus instance.
        /// </value>
        public string InstanceName { get; set; }
        
        /// <summary>
        /// Gets or sets the shared access policy.
        /// </summary>
        /// <value>
        /// The shared access policy.
        /// </value>
        public string SharedAccessPolicy { get; set; }

        /// <summary>
        /// Gets or sets a value indicating whether the shared access policy defined is at the top service level or granular at the topic/queue level.
        /// </summary>
        /// <value><c>true</c> if this instance is service level shared access policy; otherwise, <c>false</c>.</value>
        public bool IsServiceLevelSharedAccessPolicy { get; set; }

        /// <summary>
        /// Returns a <see cref="string" /> that represents this instance.
        /// </summary>
        /// <returns>
        /// A <see cref="string" /> that represents this instance.
        /// </returns>
        /// <inheritdoc />
        public override string ToString()
        {
            return $"AppId: {AppId},TenantId: {TenantId}, SubscriptionId: {SubscriptionId},SharedAccessPolicy: {SharedAccessPolicy}, IsServiceLevelSharedAccessPolicy: {IsServiceLevelSharedAccessPolicy}, " +
                   $"InstanceName: {InstanceName}ReceiverEntity: {ReceiverEntity}, ReceiverSubscriptionName: {ReceiverSubscriptionName}, IsTopic: {IsTopic}" +
                   $", PollFrequency: {PollFrequencyInSeconds},EnableStringBodyTypeSupport: {EnableStringBodyTypeSupport}, MessageVersion: {MessageVersion}";
        }

        /// <summary>
        /// Validates this instance.
        /// </summary>
        /// <exception cref="ArgumentException">
        /// InstanceName must be set and AppId must be set and AppId must be set and
        /// TenantId must be set and SubscriptionId must be set and SharedAccessPolicy must be set
        /// and ReceiverEntity OR SenderEntity must be set.
        /// </exception>
        /// <inheritdoc />
        public override void Validate()
        {
            if (InstanceName.IsNullOrEmpty())
                throw new ArgumentException("InstanceName must be set");

            if (AppId.IsNullOrEmpty())
                throw new ArgumentException("AppId must be set");

            if (AppSecret.IsNullOrEmpty())
                throw new ArgumentException("AppId must be set");

            if (TenantId.IsNullOrEmpty())
                throw new ArgumentException("TenantId must be set");

            if (SubscriptionId.IsNullOrEmpty())
                throw new ArgumentException("SubscriptionId must be set");

            if (SharedAccessPolicy.IsNullOrEmpty())
                throw new ArgumentException("SharedAccessPolicy must be set");

            base.Validate();
        }
    }
}
