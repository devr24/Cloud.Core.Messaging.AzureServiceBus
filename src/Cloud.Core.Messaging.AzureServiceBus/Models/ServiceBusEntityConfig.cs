using System.Collections.Generic;

namespace Cloud.Core.Messaging.AzureServiceBus.Models
{
    /// <summary>
    /// Config for creating an entity such as Queue, Topic or Subscription
    /// </summary>
    /// <seealso cref="IMessageEntityConfig" />
    public class ServiceBusEntityConfig : IMessageEntityConfig
    {
        /// <summary>
        /// The name of the Topic or Queue that will be created.
        /// </summary>
        public string EntityName { get; set; }
        /// <summary>
        /// The type of entity that will be created.
        /// </summary>
        public EntityType EntityType { get; set; }
        /// <summary>
        /// The name of the subscription that will be created.
        /// </summary>
        public string EntitySubscriptionName { get; set; }
        /// <summary>
        /// The sql filter that will be applied to the subscription.
        /// </summary>
        public KeyValuePair<string, string>? SqlFilter { get; set; }
    }
}
