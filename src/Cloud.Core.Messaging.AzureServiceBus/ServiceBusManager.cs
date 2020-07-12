namespace Cloud.Core.Messaging.AzureServiceBus
{
    using System;
    using System.Collections.Generic;
    using System.Text;
    using System.Threading.Tasks;
    using Config;
    using Core;
    using Microsoft.Azure.ServiceBus.Management;
    using Models;

    /// <summary>
    /// Class ServiceBusInfo. This class cannot be inherited.
    /// Implements the <see cref="IMessageEntityManager" />
    /// </summary>
    /// <seealso cref="IMessageEntityManager" />
    public sealed class ServiceBusManager : IMessageEntityManager
    {
        private ManagementClient _manager;
        private readonly ConfigBase _entityConfig;

        private ManagementClient ManagerClient
        {
            get
            {
                if (_manager == null)
                    _manager = new ManagementClient(ConnectionString);
                return _manager;
            }
        }

        /// <summary>
        /// Gets or sets the connection string.
        /// </summary>
        /// <value>The connection string.</value>
        public string ConnectionString { get; private set; }

        /// <summary>
        /// Gets the name of the Service Bus instance name.
        /// </summary>
        /// <value>The name of the instance.</value>
        public string InstanceName { get; private set; }

        /// <summary>
        /// Gets a value indicating whether [enable automatic back off] is configured.
        /// </summary>
        /// <value><c>true</c> if [enable automatic back off]; otherwise, <c>false</c>.</value>
        public bool EnableAutoBackOff { get; internal set; }

        /// <summary>
        /// Gets a value indicating whether this instance is backing off.
        /// </summary>
        /// <value><c>true</c> if this instance is backing off; otherwise, <c>false</c>.</value>
        public bool IsBackingOff { get; internal set; }

        /// <summary>
        /// Initializes a new instance of the <see cref="ServiceBusManager" /> class.
        /// </summary>
        /// <param name="connectionString">The connection string.</param>
        /// <param name="entityConfig">The entity configuration.</param>
        internal ServiceBusManager(string connectionString, ConfigBase entityConfig)
        {
            ConnectionString = connectionString;
            _entityConfig = entityConfig;
        }

        /// <summary>
        /// Initialises this instance.
        /// </summary>
        /// <returns>Task.</returns>
        internal async Task Initialise()
        {
            var instanceInfo = await ManagerClient.GetNamespaceInfoAsync();
            InstanceName = instanceInfo.Name;
            IsPremiumTier = instanceInfo.MessagingSku == MessagingSku.Premium;
            EnableAutoBackOff = _entityConfig.EnableAutobackOff;
            
            if (_entityConfig.Receiver != null)
            {
                ReceiverInfo = _entityConfig.Receiver.GetBase();
                await SetAdditionalReceiverInfo();
            }
            
            if (_entityConfig.Sender != null)
            {
                SenderInfo = _entityConfig.Sender.GetBase();
                await SetAdditionalSenderInfo();
            }
        }

        /// <summary>
        /// Gets a value indicating whether the ServiceBus namespace instance is premium tier.
        /// </summary>
        /// <value><c>true</c> if this instance is premium tier; otherwise, <c>false</c>.</value>
        public bool IsPremiumTier { get; private set; }

        /// <summary>
        /// Gets the receiver information.
        /// </summary>
        /// <value>The receiver information.</value>
        public ReceiverInfo ReceiverInfo { get; set; }

        /// <summary>
        /// Gets the sender information.
        /// </summary>
        /// <value>The sender information.</value>
        public SenderInfo SenderInfo { get; private set; }

        /// <summary>
        /// Determines whether [is receiver entity is disabled].
        /// </summary>
        /// <returns>Task&lt;System.Boolean&gt;.</returns>
        /// <exception cref="InvalidOperationException">Receiver entity has not been configured</exception>
        public async Task<bool> IsReceiverEntityDisabled()
        {
            if (ReceiverInfo == null)
                throw new InvalidOperationException("Receiver entity has not been configured");

            if (ReceiverInfo.EntityType == EntityType.Topic)
                return await _manager.IsTopicOrSubscriptionDisabled(ReceiverInfo.EntityName, ReceiverInfo.EntitySubscriptionName);
            else
                return await _manager.IsQueueDisabled(ReceiverInfo.EntityName);
        }

        /// <summary>
        /// Determines whether [is sender entity is disabled].
        /// </summary>
        /// <returns>Task&lt;System.Boolean&gt;.</returns>
        /// <exception cref="InvalidOperationException">Receiver entity has not been configured</exception>
        public async Task<bool> IsSenderEntityDisabled()
        {
            if (SenderInfo == null)
                throw new InvalidOperationException("Receiver entity has not been configured");

            if (SenderInfo.EntityType == EntityType.Topic)
                return await _manager.IsTopicOrSubscriptionDisabled(SenderInfo.EntityName);
            else
                return await _manager.IsQueueDisabled(SenderInfo.EntityName);
        }

        /// <summary>
        /// Gets the receiver entity usage percentage.
        /// </summary>
        /// <returns>Task&lt;System.Double&gt;.</returns>
        /// <exception cref="InvalidOperationException">Receiver entity has not been configured</exception>
        /// <exception cref="System.InvalidOperationException">Receiver entity has not been configured</exception>
        public async Task<decimal> GetReceiverEntityUsagePercentage()
        {
            if (ReceiverInfo == null)
                throw new InvalidOperationException("Receiver entity has not been configured");

            if (ReceiverInfo.EntityType == EntityType.Queue)
                return await ManagerClient.GetQueueUsagePercentage(ReceiverInfo.EntityName, ReceiverInfo.MaxEntitySizeBytes);
            else
                return await ManagerClient.GetTopicUsagePercentage(ReceiverInfo.EntityName, ReceiverInfo.MaxEntitySizeBytes);
        }

        /// <summary>
        /// Gets the sender entity usage percentage.
        /// </summary>
        /// <returns>Task&lt;System.Double&gt;.</returns>
        /// <exception cref="InvalidOperationException">Sender entity has not been configured</exception>
        /// <exception cref="System.InvalidOperationException">Sender entity has not been configured</exception>
        public async Task<decimal> GetSenderEntityUsagePercentage()
        {
            if (SenderInfo == null)
                throw new InvalidOperationException("Sender entity has not been configured");

            if (SenderInfo.EntityType == EntityType.Queue)
                return await ManagerClient.GetQueueUsagePercentage(SenderInfo.EntityName, SenderInfo.MaxEntitySizeBytes);
            else
                return await ManagerClient.GetTopicUsagePercentage(SenderInfo.EntityName, SenderInfo.MaxEntitySizeBytes);
        }

        /// <summary>
        /// Gets the receiver message count.
        /// </summary>
        /// <returns>Task&lt;EntityMessageCount&gt;.</returns>
        /// <exception cref="InvalidOperationException">Receiver entity has not been configured</exception>
        /// <exception cref="System.InvalidOperationException">Receiver entity has not been configured</exception>
        public async Task<EntityMessageCount> GetReceiverMessageCount()
        {
            if (ReceiverInfo == null)
                throw new InvalidOperationException("Receiver entity has not been configured");

            if (ReceiverInfo.EntityType == EntityType.Queue)
                return await ManagerClient.GetQueueMessageCount(ReceiverInfo.EntityName);
            else
                return await ManagerClient.GetTopicSubscriptionMessageCount(ReceiverInfo.EntityName, ReceiverInfo.EntitySubscriptionName);
        }

        /// <summary>
        /// Gets the sender message count.
        /// </summary>
        /// <returns>Task&lt;EntityMessageCount&gt;.</returns>
        /// <exception cref="InvalidOperationException">Sender entity has not been configured</exception>
        /// <exception cref="System.InvalidOperationException">Sender entity has not been configured</exception>
        public async Task<EntityMessageCount> GetSenderMessageCount()
        {
            if (SenderInfo == null)
                throw new InvalidOperationException("Sender entity has not been configured");

            if (SenderInfo.EntityType == EntityType.Queue)
                return await ManagerClient.GetQueueMessageCount(SenderInfo.EntityName);
            else
                return await ManagerClient.GetTopicMessageCount(SenderInfo.EntityName);
        }

        #region Additional Manager Functionality

        /// <summary>
        /// Deletes a topic/queue entity.
        /// </summary>
        /// <param name="entityType">Type of the entity.</param>
        /// <param name="entityName">Name of the entity to delete.</param>
        /// <returns>Task.</returns>
        public async Task DeleteEntity(EntityType entityType, string entityName)
        {
            if (entityType == EntityType.Topic)
                await ManagerClient.DeleteTopicIfExists(entityName);
            else
                await ManagerClient.DeleteQueueIfExists(entityName);
        }

        /// <summary>
        /// Deletes a topic subscription entity.
        /// </summary>
        /// <param name="entityName">Name of the topic entity.</param>
        /// <returns>Task.</returns>
        public async Task DeleteEntity(string entityName)
        {
            var isTopic = await ManagerClient.IsTopic(entityName);
            if (isTopic)
                await ManagerClient.DeleteTopicIfExists(entityName);
            else
                await ManagerClient.DeleteQueueIfExists(entityName);
        }

        /// <summary>
        /// Creates a new Queue, Topic and or Subscription.
        /// </summary>
        /// <param name="config">The config with the creation details, <see cref="ServiceBusEntityConfig"/>.</param>
        /// <exception cref="NullReferenceException"></exception>
        public async Task CreateEntity(IEntityConfig config)
        {
            if (!(config is ServiceBusEntityConfig sbConfig))
            {
                throw new InvalidOperationException("ServiceBusCreateEntityConfig is required ");
            }

            if (sbConfig.EntityType == EntityType.Topic)
            {
                await Task.Run(() => ManagerClient.CreateTopicIfNotExists(sbConfig.EntityName, sbConfig.EntitySubscriptionName, sbConfig.SqlFilter));
            }
            else
            {
                await Task.Run(() => ManagerClient.CreateQueueIfNotExists(sbConfig.EntityName));
            }
        }

        /// <summary>
        /// Gets a count for the specified queue/topic.
        /// </summary>
        /// <param name="entityName">Name of the entity to count on.</param>
        /// <returns>Task&lt;EntityMessageCount&gt;.</returns>
        public async Task<EntityMessageCount> EntityCount(string entityName)
        {
            var isTopic = await ManagerClient.IsTopic(entityName);

            return isTopic ?
                await ManagerClient.GetTopicMessageCount(entityName) :
                await ManagerClient.GetQueueMessageCount(entityName);
        }

        /// <summary>
        /// Purges the entity of all messages (very crude - will purge all queues).
        /// </summary>
        /// <param name="entityName">Name of the entity to purge.</param>
        /// <param name="preserveState">if set to <c>true</c> [preserve state while purging] - ONLY RELEVANT FOR TOPICS.</param>
        /// <returns>Task.</returns>
        public async Task EntityFullPurge(string entityName, bool preserveState = true)
        {
            var isTopic = await ManagerClient.IsTopic(entityName);

            if (isTopic)
                await ManagerClient.PurgeTopic(entityName, preserveState);
            else
                await ManagerClient.PurgeQueue(entityName);
        }

        /// <summary>
        /// Gets the entity.
        /// </summary>
        /// <param name="entityName">Name of the entity.</param>
        /// <returns>Task&lt;EntityInfo&gt;.</returns>
        public async Task<EntityInfo> GetEntity(string entityName)
        {
            return await ManagerClient.GetEntity(entityName);
        }

        /// <summary>
        /// Check the queue entity exists.
        /// </summary>
        /// <param name="entityName">The entity name to check exists.</param>
        /// <returns>Boolean true if exists and false if not.</returns>
        public async Task<bool> EntityExists(string entityName)
        {
            var isTopic = await ManagerClient.IsTopic(entityName);

            if (isTopic)
                return await ManagerClient.TopicExistsAsync(entityName);
            else
                return await ManagerClient.QueueExistsAsync(entityName);
        }

        /// <summary>Scotches the namespace by deleting all topics and queues.</summary>
        public async Task ScotchNamespace()
        {
            var topics = await ManagerClient.GetTopicsAsync();
            var queues = await ManagerClient.GetQueuesAsync();

            foreach (var topic in topics)
            {
                await ManagerClient.DeleteTopicIfExists(topic.Path);
            }

            foreach (var queue in queues)
            {
                await ManagerClient.DeleteQueueIfExists(queue.Path);
            }
        }

        #endregion

        /// <summary>
        /// Returns a <see cref="string" /> that represents this instance.
        /// </summary>
        /// <returns>A <see cref="string" /> that represents this instance.</returns>
        public override string ToString()
        {
            var sb = new StringBuilder();

            sb.AppendLine("INSTANCE INFO");
            sb.Append(" InstanceName: ");
            sb.AppendLine(InstanceName);
            sb.Append(" IsPremiumTierInstance: ");
            sb.AppendLine(IsPremiumTier.ToString());
            sb.Append(" ConnectionString: ");
            sb.AppendLine(ConnectionString.IsNullOrEmpty() ? "[NOT SET]" : "[SET]");
            sb.Append(Environment.NewLine);
            sb.AppendLine(ReceiverInfo != null ? ReceiverInfo.ToString() : "[RECEIVER NOT SET]");
            sb.AppendLine(SenderInfo != null ? SenderInfo.ToString() : "[SENDER NOT SET]");

            return sb.ToString();
        }

        /// <summary>
        /// Sets the additional receiver information.
        /// </summary>
        /// <returns>Task.</returns>
        private async Task SetAdditionalReceiverInfo()
        {
            // Get information about the topic/queue for the receiver.
            if (ReceiverInfo.EntityType == EntityType.Topic)
            {
                if (ReceiverInfo.CreateEntityIfNotExists)
                {
                    await Task.Run(() => ManagerClient.CreateTopicIfNotExists(ReceiverInfo.EntityName, ReceiverInfo.EntitySubscriptionName, ReceiverInfo.EntityFilter));
                }

                var topic = await ManagerClient.GetTopicAsync(ReceiverInfo.EntityName);
                var subscription = await ManagerClient.GetSubscriptionAsync(ReceiverInfo.EntityName, ReceiverInfo.EntitySubscriptionName);
                ReceiverInfo.MaxEntitySizeMb = topic.MaxSizeInMB;
                ReceiverInfo.MaxLockDuration = subscription.LockDuration;
            }
            else
            {
                if (ReceiverInfo.CreateEntityIfNotExists)
                    await Task.Run(() => ManagerClient.CreateQueueIfNotExists(ReceiverInfo.EntityName));

                var queue = await ManagerClient.GetQueueAsync(ReceiverInfo.EntityName);
                ReceiverInfo.MaxLockDuration = queue.LockDuration;
                ReceiverInfo.MaxEntitySizeMb = queue.MaxSizeInMB;
            }
        }

        /// <summary>
        /// Sets the additional sender information.
        /// </summary>
        /// <returns>Task.</returns>
        private async Task SetAdditionalSenderInfo()
        {
            // Get information about the topic/queue for the sender.
            if (SenderInfo.EntityType == EntityType.Topic)
            {
                if (SenderInfo.CreateEntityIfNotExists)
                {
                    await Task.Run(() => ManagerClient.CreateTopicIfNotExists(SenderInfo.EntityName));
                }

                var queue = await ManagerClient.GetTopicAsync(SenderInfo.EntityName);
                SenderInfo.MaxEntitySizeMb = queue.MaxSizeInMB;
            }
            else
            {
                if (SenderInfo.CreateEntityIfNotExists)
                {
                    await Task.Run(() => ManagerClient.CreateQueueIfNotExists(SenderInfo.EntityName));
                }

                var queue = await ManagerClient.GetQueueAsync(SenderInfo.EntityName);
                SenderInfo.MaxEntitySizeMb = queue.MaxSizeInMB;
            }

            SenderInfo.MaxMessageBatchSizeBytes = IsPremiumTier ? 1024000 : 256000;
        }

        /// <summary>
        /// Update the receiver entity using generic parameters
        /// </summary>
        /// <param name="entityName">The updated entity to listen to.</param>
        /// <param name="entitySubscriptionName">The updated subscription on the entity to listen to.</param>
        /// <param name="createIfNotExists">Creates the entity if it does not exist</param>
        /// <param name="entityFilter">A filter that will be applied to the entity if created through this method</param>
        /// <param name="supportStringBodyType">Support reading Service Bus message body as a string</param>
        /// <returns></returns>
        internal async Task UpdateReceiver(string entityName, string entitySubscriptionName, bool createIfNotExists, KeyValuePair<string, string>? entityFilter, bool supportStringBodyType = false)
        {
            // If no receiver exists (wasnt subscribed to), then create new.
            if (ReceiverInfo == null)
            {
                ReceiverInfo = new ReceiverInfo();
            }

            // Update the reciever info with the new config.
            ReceiverInfo.EntityType = EntityType.Topic; //TODO: Future - need to know this, presumed Topic.
            ReceiverInfo.EntityName = entityName;
            ReceiverInfo.EntitySubscriptionName = entitySubscriptionName;
            ReceiverInfo.CreateEntityIfNotExists = createIfNotExists;
            ReceiverInfo.EntityFilter = entityFilter;
            ReceiverInfo.SupportStringBodyType = supportStringBodyType;

            // Ensure the entity manager is configured up.
            await SetAdditionalReceiverInfo();
        }

        /// <summary>
        /// A service bus specific way to update the receiver
        /// </summary>
        /// <param name="receiverInfo">Updated receiver information</param>
        /// <returns></returns>
        public async Task UpdateReceiver(ReceiverInfo receiverInfo)
        {
            ReceiverInfo = receiverInfo;

            await SetAdditionalReceiverInfo();
        }
    }
}
