namespace Microsoft.Azure.ServiceBus.Management
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading.Tasks;
    using Cloud.Core;
    using Cloud.Core.Extensions;
    using Cloud.Core.Messaging.AzureServiceBus.Models;

    /// <summary>
    /// Class Management Client extensions.
    /// </summary>
    public static class ManagementClientExtensions
    {
        private static readonly object CreationLock = new object();

        /// <summary>
        /// Create a topic if it does not exist.
        /// </summary>
        /// <param name="manager">The manager.</param>
        /// <param name="topicName">Name of the topic.</param>
        /// <returns><see cref="TopicDescription"/> Topic description.</returns>
        public static TopicDescription CreateTopicIfNotExists(this ManagementClient manager, string topicName)
        {
            // Setup Topic (if not exists).
            return manager.CreateTopicIfNotExists(topicName, null, default(List<KeyValuePair<string, string>>));

        }

        /// <summary>
        /// Creates a topic and subscription if either do not exist.
        /// </summary>
        /// <param name="manager">The manager.</param>
        /// <param name="topicName">Name of the topic.</param>
        /// <param name="subscriptionName">Name of the subscription.</param>
        /// <returns>Task.</returns>
        public static TopicDescription CreateTopicIfNotExists(this ManagementClient manager, string topicName, string subscriptionName)
        {
            // Create topic and subscription if not exists.
            return manager.CreateTopicIfNotExists(topicName, subscriptionName, default(List<KeyValuePair<string, string>>));
        }

        /// <summary>
        /// Creates a topic and subscription if they do not exist and adds a filter to the subscription.
        /// </summary>
        /// <param name="manager">The manager.</param>
        /// <param name="topicName">Name of the topic.</param>
        /// <param name="subscriptionName">Name of the subscription.</param>
        /// <param name="filter">The SQL filter.</param>
        /// <returns><see cref="TopicDescription"/> Topic description.</returns>
        public static TopicDescription CreateTopicIfNotExists(this ManagementClient manager, string topicName, string subscriptionName, KeyValuePair<string, string>? filter)
        {
            var filters = new List<KeyValuePair<string, string>>();
            if (filter.HasValue)
            {
                filters.Add(filter.Value);
            }

            // Create topic, subscriptions and filter if not exists.
            return manager.CreateTopicIfNotExists(topicName, subscriptionName, filters);
        }

        /// <summary>
        /// Creates a topic and subscription if they do not exist and adds a filter to the subscription.
        /// </summary>
        /// <param name="manager">The manager.</param>
        /// <param name="topicName">Name of the topic.</param>
        /// <param name="subscriptionName">Name of the subscription.</param>
        /// <param name="filters">The SQL filters to apply.</param>
        /// <returns><see cref="TopicDescription"/> Topic description.</returns>
        public static TopicDescription CreateTopicIfNotExists(this ManagementClient manager, string topicName, string subscriptionName, List<KeyValuePair<string, string>> filters)
        {
            lock (CreationLock)
            {
                // Setup Topic (if not exists).
                var description = EnsureTopic(manager, topicName).GetAwaiter().GetResult();

                // Setup Subscription (if not exists).
                if (subscriptionName != null)
                {
                    EnsureSubscription(manager, topicName, subscriptionName).GetAwaiter().GetResult();
                }

                // Setup Subscription Filter (if not exists).
                if (!filters.IsNullOrDefault() && filters.Count > 0)
                {
                    // Apply each filter.
                    foreach (var filter in filters)
                    {
                        EnsureSubscriptionFilter(manager, topicName, subscriptionName, filter).GetAwaiter().GetResult();
                    }
                }

                return description;
            }
        }

        /// <summary>
        /// Creates a queue if not exists.
        /// </summary>
        /// <param name="manager">The manager.</param>
        /// <param name="queueName">Name of the queue.</param>
        /// <returns><see cref="QueueDescription"/> Queue description.</returns>
        /// <exception cref="ArgumentException">Cannot create queue as a topic with this name exists already - queueName</exception>
        public static QueueDescription CreateQueueIfNotExists(this ManagementClient manager, string queueName)
        {
            lock (CreationLock)
            {
                QueueDescription queue;

                try
                {
                    queue = manager.GetQueueAsync(queueName).GetAwaiter().GetResult();
                }
                catch (Exception)
                {
                    try
                    {
                        // Check to see if a topic exists already with the name of the queue you are trying to create.
                        if (manager.GetTopicAsync(queueName).GetAwaiter().GetResult() != null)
                            throw new ArgumentException($"Cannot create queue as a topic with this name exists already: {queueName}", "queueName");
                    }
                    catch (ArgumentException)
                    {
                        throw;
                    }
                    catch (Exception)
                    {
                        // do nothing in this case as topic does not exist (and its ok to create a queue with this name).
                    }


                    // Has failed as the topic does not exist, therefore, create!
                    var instanceInfo = manager.GetNamespaceInfoAsync().GetAwaiter().GetResult();
                    var queueDesc = new QueueDescription(queueName)
                    {
                        // Add duplicate detection.
                        RequiresDuplicateDetection = true,
                        DuplicateDetectionHistoryTimeWindow = TimeSpan.FromMinutes(60),

                        // Set the max topic size - allowed values: 1024;2048;3072;4096;5120;8192000;
                        // Our premium SB instances should all be 80gb (819200mb), otherwise 5gb (5120mb).
                        MaxSizeInMB = (instanceInfo.MessagingSku == MessagingSku.Premium ? 8192000 : 5120)
                    };

                    queue = manager.CreateQueueAsync(queueDesc).GetAwaiter().GetResult();
                }

                return queue;
            }
        }

        /// <summary>
        /// Gets subscription message count for a particular topic.
        /// </summary>
        /// <param name="manager">The manager.</param>
        /// <param name="topicName">Name of the topic.</param>
        /// <param name="subscriptionName">Name of the subscription.</param>
        /// <returns>Task&lt;EntityMessageCount&gt;.</returns>
        public static async Task<EntityMessageCount> GetTopicSubscriptionMessageCount(this ManagementClient manager, string topicName, string subscriptionName)
        {
            var runtimeInfo = await manager.GetSubscriptionRuntimeInfoAsync(topicName, subscriptionName);
            return new EntityMessageCount
            {
                ErroredEntityCount = runtimeInfo.MessageCountDetails.DeadLetterMessageCount,
                ActiveEntityCount = runtimeInfo.MessageCountDetails.ActiveMessageCount
            };
        }

        /// <summary>
        /// Gets count for ALL subscriptions on the topic (as one specific topic is not being specified).
        /// </summary>
        /// <param name="manager">The manager.</param>
        /// <param name="topicName">Name of the topic.</param>
        /// <returns>Task&lt;EntityMessageCount&gt;.</returns>
        public static async Task<EntityMessageCount> GetTopicMessageCount(this ManagementClient manager, string topicName)
        {
            var allSubscriptions = await manager.GetSubscriptionsAsync(topicName);
            long totalActive = 0;
            long totalErrored = 0;

            // Because no one specific subscription was requested, count up messages across all subscriptions.
            foreach (var sub in allSubscriptions)
            {
                var runtimeInfo = await manager.GetSubscriptionRuntimeInfoAsync(topicName, sub.SubscriptionName);
                totalActive += runtimeInfo.MessageCountDetails.ActiveMessageCount;
                totalErrored += runtimeInfo.MessageCountDetails.DeadLetterMessageCount;
            }

            return new EntityMessageCount
            {
                ErroredEntityCount = totalErrored,
                ActiveEntityCount = totalActive
            };
        }

        /// <summary>
        /// Gets the queue message count.
        /// </summary>
        /// <param name="manager">The manager.</param>
        /// <param name="queueName">Name of the queue.</param>
        /// <returns>Task&lt;EntityMessageCount&gt;.</returns>
        public static async Task<EntityMessageCount> GetQueueMessageCount(this ManagementClient manager, string queueName)
        {
            var runtimeInfo = await manager.GetQueueRuntimeInfoAsync(queueName);
            return new EntityMessageCount
            {
                ErroredEntityCount = runtimeInfo.MessageCountDetails.DeadLetterMessageCount,
                ActiveEntityCount = runtimeInfo.MessageCountDetails.ActiveMessageCount
            };
        }

        /// <summary>
        /// Determines whether [the specified queue] is disabled.
        /// </summary>
        /// <param name="manager">The manager.</param>
        /// <param name="queueName">Name of the queue.</param>
        /// <returns>Task&lt;System.Boolean&gt;.</returns>
        public static async Task<bool> IsQueueDisabled(this ManagementClient manager, string queueName)
        {
            var queue = await manager.GetQueueAsync(queueName);
            return (queue.Status == EntityStatus.Disabled || queue.Status == EntityStatus.ReceiveDisabled);
        }

        /// <summary>
        /// Determines whether a particular topic or subscription is disabled.
        /// </summary>
        /// <param name="manager">The manager.</param>
        /// <param name="topicName">Name of the topic.</param>
        /// <param name="subscriptionName">Name of the subscription.</param>
        /// <returns>Task&lt;System.Boolean&gt;.</returns>
        public static async Task<bool> IsTopicOrSubscriptionDisabled(this ManagementClient manager, string topicName, string subscriptionName = null)
        {
            var topic = await manager.GetTopicAsync(topicName);

            // If topic is disabled, return true here.
            if (topic.Status == EntityStatus.Disabled || topic.Status == EntityStatus.ReceiveDisabled)
                return true;

            // If no subscription was requested, then return false as the topic is enabled.
            if (subscriptionName == null)
                return false;

            // Otherwise, carry on and check subscription.
            var subscription = await manager.GetSubscriptionAsync(topicName, subscriptionName);
            return (subscription.Status == EntityStatus.Disabled || subscription.Status == EntityStatus.ReceiveDisabled);
        }

        /// <summary>
        /// Gets the usage percentage for a topic, e.g if topic was 10Gb and 3Gb was used, the result would be 30% (expressed as 30.00).  If the same topic was full, the result would be 100.
        /// </summary>
        /// <param name="manager">The manager.</param>
        /// <param name="topicName">Name of the topic.</param>
        /// <param name="maxSizeInBytes">The maximum size in bytes.</param>
        /// <returns>Task&lt;System.Double&gt;.</returns>
        public static async Task<decimal> GetTopicUsagePercentage(this ManagementClient manager, string topicName, long maxSizeInBytes)
        {
            var topicInfo = await manager.GetTopicRuntimeInfoAsync(topicName);
            decimal currentKb = topicInfo.SizeInBytes;
            return Convert.ToDecimal($"{(currentKb / maxSizeInBytes):P}".Replace("%", ""));
        }

        /// <summary>
        /// Gets the usage percentage for a queue, e.g if topic was 10Gb and 3Gb was used, the result would be 30.00.  If the same queue was full, the result would be 100.
        /// </summary>
        /// <param name="manager">The manager.</param>
        /// <param name="queueName">Name of the queue.</param>
        /// <param name="maxSizeInBytes">The maximum size in bytes.</param>
        /// <returns>Task&lt;System.Double&gt;.</returns>
        public static async Task<decimal> GetQueueUsagePercentage(this ManagementClient manager, string queueName, long maxSizeInBytes)
        {
            var queueInfo = await manager.GetQueueRuntimeInfoAsync(queueName);
            decimal currentKb = queueInfo.SizeInBytes;
            return Convert.ToDecimal($"{(currentKb / maxSizeInBytes):P}".Replace("%", ""));
        }

        /// <summary>
        /// Deletes the topic if exists.
        /// </summary>
        /// <param name="manager">The manager.</param>
        /// <param name="topicName">Name of the topic.</param>
        /// <returns>Task boolean true if successful and false if not.</returns>
        public static async Task<bool> DeleteTopicIfExists(this ManagementClient manager, string topicName)
        {
            try
            {
                await manager.DeleteTopicAsync(topicName);
                return true;
            }
            catch (Exception)
            {
                // Do nothing if queue didn't exist.
                return false;
            }
        }

        /// <summary>
        /// Deletes the topic subscription if exists.
        /// </summary>
        /// <param name="manager">The manager.</param>
        /// <param name="topicName">Name of the topic.</param>
        /// <param name="subscriptionName">Name of the subscription.</param>
        /// <returns>Task boolean true if successful and false if not.</returns>
        public static async Task<bool> DeleteSubscriptionIfExists(this ManagementClient manager, string topicName, string subscriptionName)
        {
            try
            {
                await manager.DeleteSubscriptionAsync(topicName, subscriptionName);
                return true;
            }
            catch (Exception)
            {
                // Do nothing if queue didnt exist.
                return false;
            }
        }

        /// <summary>
        /// Deletes the queue if exists.
        /// </summary>
        /// <param name="manager">The manager.</param>
        /// <param name="queueName">Name of the queue.</param>
        /// <returns>Task boolean true if successful and false if not.</returns>
        public static async Task<bool> DeleteQueueIfExists(this ManagementClient manager, string queueName)
        {
            try
            {
                await manager.DeleteQueueAsync(queueName);
                return true;
            }
            catch (Exception)
            {
                // Do nothing if queue didnt exist.
                return false;
            }
        }

        /// <summary>
        /// Purges the specified queue.  Purge is done by deleting and re-creating the queue - fastest way to purge.
        /// </summary>
        /// <param name="manager">The manager to extend.</param>
        /// <param name="queueName">Name of the queue to purge.</param>
        /// <returns>Task.</returns>
        /// <exception cref="ArgumentException">Cannot purge queue as it has been defined as a topic - queueName</exception>
        public static async Task PurgeQueue(this ManagementClient manager, string queueName)
        {
            try
            {
                // Delete the queue.
                await manager.DeleteQueueAsync(queueName);

                // Then recreate it.
                manager.CreateQueueIfNotExists(queueName);
            }
            catch (Exception ex)
            {
                try
                {
                    // Check to see if a topic exists already with the name of the queue you are trying to create.
                    if (await manager.GetTopicAsync(queueName) != null)
                        throw new ArgumentException($"Cannot purge queue as it has been defined as a topic: {queueName}", "queueName");
                }
                catch (ArgumentException)
                {
                    throw;
                }
                catch (Exception)
                {
                    throw;
                }
            }
        }

        /// <summary>
        /// Purges the specified topic.  Purge is done by deleting and re-creating the topic - fastest way to purge.
        /// Topic will have same susbcriptions and filters re-applied after recreation if specified.
        /// </summary>
        /// <param name="manager">The manager to extend.</param>
        /// <param name="topicName">Name of the topic.</param>
        /// <param name="reapplyState">if set to <c>true</c> [reapply state] - reapplies all subscriptions and filters.</param>
        /// <returns>Task.</returns>
        /// <exception cref="ArgumentException">Cannot purge queue as it has been defined as a queue - topicName</exception>
        public static async Task PurgeTopic(this ManagementClient manager, string topicName, bool reapplyState)
        {
            try
            {
                // Get topic to begin with.
                var topic = await manager.GetTopicAsync(topicName);
                IList<SubscriptionDescription> subscriptions = null;
                Dictionary<string, IList<RuleDescription>> allFilters = null;

                // If we need to reapply state, being by preserving information.
                if (reapplyState)
                {
                    // Preserve subscriptions and filters.
                    subscriptions = await manager.GetSubscriptionsAsync(topicName);
                    allFilters = new Dictionary<string, IList<RuleDescription>>();

                    foreach (var sub in subscriptions)
                    {
                        allFilters.Add(sub.SubscriptionName, await manager.GetRulesAsync(topicName, sub.SubscriptionName));
                    }
                }

                // Delete the topic first.
                await manager.DeleteTopicIfExists(topicName);

                if (reapplyState)
                {
                    // Create topic, all subscriptions and all filters again.
                    if (subscriptions.Count > 0)
                    {
                        foreach (var sub in subscriptions)
                        {
                            allFilters.TryGetValue(sub.SubscriptionName, out var filters);

                            List<KeyValuePair<string, string>> sqlFilters = null;

                            if (filters != null && filters.Count > 0)
                                sqlFilters = filters.Select(r => new KeyValuePair<string, string>(r.Name, ((SqlFilter)r.Filter).SqlExpression)).ToList();

                            manager.CreateTopicIfNotExists(topicName, sub.SubscriptionName, sqlFilters);
                        }
                    }
                }
                else
                {
                    // Recreate ONLY the topic if we don't need to repply state.
                    manager.CreateTopicIfNotExists(topicName);
                }

            }
            catch (Exception ex)
            {
                try
                {
                    // Check to see if a queue exists already with the name of the topic you are trying to create.
                    if (await manager.GetQueueAsync(topicName) != null)
                        throw new ArgumentException($"Cannot purge topic as it has been defined as a queue: {topicName}", "topicName");
                }
                catch (ArgumentException)
                {
                    throw;
                }
                catch (Exception)
                {
                    throw ex;
                }
            }
        }

        /// <summary>
        /// Ensure the topic exists.
        /// </summary>
        /// <param name="manager">The manager.</param>
        /// <param name="topicName">Name of the topic.</param>
        /// <returns>Task&lt;TopicDescription&gt;.</returns>
        /// <exception cref="ArgumentException">Cannot create topic as a queue with this name exists already - topicName</exception>
        private static async Task<TopicDescription> EnsureTopic(ManagementClient manager, string topicName)
        {
            TopicDescription topic;

            try
            {
                topic = await manager.GetTopicAsync(topicName);
            }
            catch (Exception)
            {
                try
                {
                    // Check to see if a queue exists already with the name of the topic you are trying to create.
                    if (await manager.GetQueueAsync(topicName) != null)
                        throw new ArgumentException($"Cannot create topic as a queue with this name exists already: {topicName}", "topicName");
                }
                catch (ArgumentException)
                {
                    throw;
                }
                catch (Exception)
                {
                    // do nothing in this case as queue does not exist (and its ok to create a topic with this name).
                }

                // Has failed as the topic does not exist, therefore, create!
                var instanceInfo = await manager.GetNamespaceInfoAsync();
                var topicDesc = new TopicDescription(topicName)
                {
                    // Add duplicate detection.
                    RequiresDuplicateDetection = true, 
                    DuplicateDetectionHistoryTimeWindow = TimeSpan.FromMinutes(60),

                    // Set the max topic size - allowed values: 1024;2048;3072;4096;5120;8192000;
                    // Our premium SB instances should all be 80gb (819200mb), otherwise 5gb (5120mb).
                    MaxSizeInMB = (instanceInfo.MessagingSku == MessagingSku.Premium ? 8192000 : 5120)
                };

                topic = await manager.CreateTopicAsync(topicDesc);
            }

            return topic;
        }

        /// <summary>
        /// Ensures the subscription exists.
        /// </summary>
        /// <param name="manager">The manager.</param>
        /// <param name="topicName">Name of the topic.</param>
        /// <param name="subscriptionName">Name of the subscription.</param>
        /// <returns>Task&lt;SubscriptionDescription&gt;.</returns>
        private static async Task<SubscriptionDescription> EnsureSubscription(ManagementClient manager, string topicName, string subscriptionName)
        {
            SubscriptionDescription subscription;

            try
            {
                subscription = await manager.GetSubscriptionAsync(topicName, subscriptionName);
            }
            catch (Exception)
            {
                // Has failed as the subscription does not exist, therefore, create!
                subscription = await manager.CreateSubscriptionAsync(topicName, subscriptionName);

                // Ensure locks last for the maximum allowed time of five minutes.
                subscription.LockDuration = new TimeSpan(0, 5, 0);
            }

            return subscription;
        }

        /// <summary>
        /// Ensures the subscription filter exists.
        /// </summary>
        /// <param name="manager">The manager.</param>
        /// <param name="topicName">Name of the topic.</param>
        /// <param name="subscriptionName">Name of the subscription.</param>
        /// <param name="filter">The filter.</param>
        /// <returns>Task&lt;RuleDescription&gt;.</returns>
        private static async Task<RuleDescription> EnsureSubscriptionFilter(ManagementClient manager, string topicName, string subscriptionName, KeyValuePair<string, string> filter)
        {
            // Get the rule, if it doesn't exist then create it.
            RuleDescription rule;
            var ruleName = filter.Key;
            var ruleExpression = filter.Value;

            try
            {
                rule = await manager.GetRuleAsync(topicName, subscriptionName, ruleName);
            }
            catch (Exception)
            {
                // Has failed as the filter rule does not exist, therefore, create!

                // And go ahead, create the rule.
                rule = await manager.CreateRuleAsync(topicName, subscriptionName, new RuleDescription { Name = ruleName, Filter = new SqlFilter(ruleExpression) });

                try
                {
                    // If we're applying a rule it means we DON'T want the default rule to exist.  Therefore, remove.
                    await manager.DeleteRuleAsync(topicName, subscriptionName, "$Default");
                }
                catch (Exception)
                {
                    // Do nothing.
                }
            }

            return rule;
        }

        /// <summary>
        /// Gets the entity - can be either a queue or topic.
        /// </summary>
        /// <param name="manager">The manager.</param>
        /// <param name="entityName">Name of the entity.</param>
        /// <returns>Task&lt;EntityInfo&gt;.</returns>
        public static async Task<EntityInfo> GetEntity(this ManagementClient manager, string entityName)
        {
            try
            {
                // Get all topic related info to populate the results.
                var topic = await manager.GetTopicAsync(entityName);
                var instanceInfo = await manager.GetNamespaceInfoAsync();
                var runtimeInfo = await manager.GetTopicRuntimeInfoAsync(entityName);
                var messageCount = await manager.GetTopicMessageCount(entityName);
                var percentageUsed = await manager.GetTopicUsagePercentage(entityName, topic.MaxSizeInMB * 1000000);

                return new EntityInfo()
                {
                    EntityName = entityName,
                    EntityType = EntityType.Topic,
                    SubscriptionCount = runtimeInfo.SubscriptionCount,
                    MaxEntitySizeMb = topic.MaxSizeInMB,
                    MessageCount = messageCount,
                    PercentageUsed = percentageUsed,
                    CurrentEntitySizeMb = runtimeInfo.SizeInBytes,
                    MaxMessageSizeBytes = instanceInfo.MessagingSku == MessagingSku.Premium ? 1000000 : 256000,
                    IsPremium = instanceInfo.MessagingSku == MessagingSku.Premium
                };
            }
            catch (Exception)
            {
                // do nothing here as it could be a queue entity.
            }

            try
            {
                // Get ALL queue related info to populate the results.
                var queue = await manager.GetQueueAsync(entityName);
                var instanceInfo = await manager.GetNamespaceInfoAsync();
                var runtimeInfo = await manager.GetQueueRuntimeInfoAsync(entityName);
                var messageCount = await manager.GetQueueMessageCount(entityName);
                var percentageUsed = await manager.GetQueueUsagePercentage(entityName, queue.MaxSizeInMB * 1000000);

                return new EntityInfo()
                {
                    EntityName = entityName,
                    EntityType = EntityType.Queue,
                    SubscriptionCount = 0,
                    MaxEntitySizeMb = queue.MaxSizeInMB,
                    MessageCount = messageCount,
                    PercentageUsed = percentageUsed,
                    CurrentEntitySizeMb = runtimeInfo.SizeInBytes,
                    MaxMessageSizeBytes = instanceInfo.MessagingSku == MessagingSku.Premium ? 1000000 : 256000,
                    IsPremium = instanceInfo.MessagingSku == MessagingSku.Premium
                };
            }
            catch (Exception)
            {
                // Do nothing here, we couldn't find the queue either.  Will fall down to return null.
            }

            return null;
        }

        /// <summary>
        /// Converts to pic.
        /// </summary>
        /// <param name="manager">The manager.</param>
        /// <param name="entityName">Name of the entity.</param>
        /// <returns>Task&lt;System.Boolean&gt;.</returns>
        public static async Task<bool> IsTopic(this ManagementClient manager, string entityName)
        {
            try
            {
                var result = await manager.GetTopicAsync(entityName);
                return result != null;
            }
            catch (Exception)
            {
                return false;
            }
        }
    }
}
