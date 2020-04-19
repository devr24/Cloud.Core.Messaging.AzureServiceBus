namespace Cloud.Core.Messaging.AzureServiceBus.Config
{
    using System;

    /// <summary>
    /// Managed Service Instance OR Managed User Instance config for connecting to an instance of Service Bus.
    /// Implements the <see cref="ConfigBase" />
    /// </summary>
    /// <seealso cref="ConfigBase" />
    public class MsiConfig : ConfigBase
    {
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
            return $"ServiceBusInstance: {InstanceName}, SharedAccessPolicyName: {SharedAccessPolicyName}{base.ToString()}";
        }

        /// <summary>
        /// Validates this instance.
        /// </summary>
        /// <exception cref="System.ArgumentException">
        /// InstanceName must be set
        /// or
        /// TenantId must be set
        /// or
        /// SubscriptionId must be set
        /// or
        /// SharedAccessPolicy must be set
        /// </exception>
        /// <exception cref="ArgumentException">InstanceName must be set and AppId must be set and AppId must be set and
        /// TenantId must be set and SubscriptionId must be set.</exception>
        /// <inheritdoc />
        public override void Validate()
        {
            if (InstanceName.IsNullOrEmpty())
                throw new ArgumentException("InstanceName must be set");
            
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
