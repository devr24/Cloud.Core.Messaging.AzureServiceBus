namespace Cloud.Core.Messaging.AzureServiceBus.Config
{
    using System;
    using Cloud.Core.Extensions;

    /// <summary>
    /// Service Principle configuration used to connect to an instance of ServiceBus.
    /// Implements the <see cref="ConfigBase" />
    /// </summary>
    /// <seealso cref="ConfigBase" />
    public class ServicePrincipleConfig : ConfigBase
    {
        /// <summary>
        /// Gets or sets the application identifier.
        /// </summary>
        /// <value>The application identifier.</value>
        public string AppId { get; set; }

        /// <summary>
        /// Gets or sets the application secret.
        /// </summary>
        /// <value>The application secret string.</value>
        public string AppSecret { get; set; }

        /// <summary>
        /// Gets or sets the tenant identifier.
        /// </summary>
        /// <value>The tenant identifier.</value>
        public string TenantId { get; set; }

        /// <summary>
        /// Gets or sets the subscription identifier.
        /// </summary>
        /// <value>The subscription identifier.</value>
        public string SubscriptionId { get; set; }

        /// <summary>
        /// Gets or sets the name of the service bus instance.
        /// </summary>
        /// <value>The name of the service bus instance.</value>
        public string InstanceName { get; set; }

        /// <summary>
        /// Gets or sets the shared access policy name.
        /// NOTE: MUST BE TOP-LEVEL SHARED ACCESS POLICY, NOT TOPIC/QUEUE LEVEL
        /// (this can be extended later - not used at the moment).
        /// </summary>
        /// <value>The shared access policy to use when connecting.</value>
        public string SharedAccessPolicyName { get; set; } = "RootManageSharedAccessKey";

        /// <summary>
        /// Returns a <see cref="string" /> that represents this instance.
        /// </summary>
        /// <returns>A <see cref="string" /> that represents this instance.</returns>
        /// <inheritdoc />
        public override string ToString()
        {
            return $"AppId: {AppId},TenantId: {TenantId}, SubscriptionId: {SubscriptionId},SharedAccessPolicy: {SharedAccessPolicyName}, InstanceName: { InstanceName}{base.ToString()}";
        }

        /// <summary>
        /// Validates this instance.
        /// </summary>
        /// <exception cref="System.ArgumentException">
        /// InstanceName must be set
        /// or
        /// AppId must be set
        /// or
        /// AppId must be set
        /// or
        /// TenantId must be set
        /// or
        /// SubscriptionId must be set
        /// or
        /// SharedAccessPolicy must be set
        /// </exception>
        /// <exception cref="ArgumentException">InstanceName must be set and AppId must be set and AppId must be set and
        /// TenantId must be set and SubscriptionId must be set and SharedAccessPolicy must be set
        /// and ReceiverEntity OR SenderEntity must be set.</exception>
        /// <inheritdoc />
        public override void Validate()
        {
            if (InstanceName.IsNullOrEmpty())
                throw new ArgumentException("InstanceName must be set");

            if (AppId.IsNullOrEmpty())
                throw new ArgumentException("AppId must be set");

            if (AppSecret.IsNullOrEmpty())
                throw new ArgumentException("AppSecret must be set");

            if (TenantId.IsNullOrEmpty())
                throw new ArgumentException("TenantId must be set");

            if (SubscriptionId.IsNullOrEmpty())
                throw new ArgumentException("SubscriptionId must be set");

            if (SharedAccessPolicyName.IsNullOrEmpty())
                throw new ArgumentException("SharedAccessPolicy must be set");

            base.Validate();
        }
    }
}
