using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Cloud.Core.Messaging.AzureServiceBus.Config;
using Cloud.Core.Messaging.AzureServiceBus.Models;
using Cloud.Core.Testing;
using Cloud.Core.Testing.Lorem;
using FluentAssertions;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Xunit;

namespace Cloud.Core.Messaging.AzureServiceBus.Tests.Integration
{
    [IsIntegration]
    public class ServiceBusManagementTests
    {
        private readonly IConfiguration _config;
        private readonly ILogger _logger;
        private static bool _hasClearedDown;

        public ServiceBusManagementTests()
        {
            _config = new ConfigurationBuilder().AddJsonFile("appSettings.json").Build();
            _logger = new ServiceCollection().AddLogging(builder => builder.AddConsole())
                .BuildServiceProvider().GetService<ILogger<ServiceBusSendReceiveTests>>();

            // Clear down tables to begin with.
            RemoveEntities();
        }

        /// <summary>Ensure receiver/sender queue count is updated accurately after sending messages.</summary>
        [Theory]
        [InlineData(50)]
        [InlineData(100)]
        [InlineData(200)]
        public void Test_ServiceBusQueueEntityManager_ReceiverMessageCount(int numMessagesToSend)
        {
            const string tableName = "ManagementCountQueue";

            // Arrange - create queue entity using manager.
            var manager = GetEntityManagerInstance();
            manager.CreateEntity(new ServiceBusEntityConfig { EntityType = EntityType.Queue, EntityName = tableName }).GetAwaiter().GetResult();

            // Quick test to verify we can't create a topic when a queue already exists (should probably be its own test...).
            Assert.Throws<ArgumentException>(() => manager.CreateEntity(new ServiceBusEntityConfig { EntityType = EntityType.Topic, EntityName = tableName }).GetAwaiter().GetResult());

            var queueMessenger = GetQueueMessenger(tableName);

            // Grab the counts from the queue.
            var initialReceiverCount = (queueMessenger.EntityManager.GetReceiverMessageCount().GetAwaiter().GetResult()).ActiveEntityCount;
            var paragraphs = Lorem.GetParagraphs(numMessagesToSend).ToList();
            var expectedCount = initialReceiverCount + paragraphs.Count;

            // Act - send messages to increment counts on the queue.
            queueMessenger.SendBatch(new List<string>(paragraphs)).GetAwaiter().GetResult();

            Thread.Sleep(2500 + numMessagesToSend);

            var updatedReceiverCount = (queueMessenger.EntityManager.GetReceiverMessageCount().GetAwaiter().GetResult()).ActiveEntityCount;
            var senderTargetCount = (queueMessenger.EntityManager.GetSenderMessageCount().GetAwaiter().GetResult()).ActiveEntityCount;
            var verifyCount = manager.EntityCount(tableName).GetAwaiter().GetResult();

            // Assert.
            var entity = manager.GetEntity(tableName).GetAwaiter().GetResult();
            entity.EntityName.Should().Be(queueMessenger.ConnectionManager.ReceiverInfo.EntityName);
            manager.DeleteEntity(tableName).GetAwaiter().GetResult();
            _ = updatedReceiverCount.Should().Be(expectedCount);
            _ = senderTargetCount.Should().Be(expectedCount);
            verifyCount.ActiveEntityCount.Should().Be(updatedReceiverCount);
        }

        /// <summary>Ensure receiver/sender topic count is updated accurately after sending messages.</summary>
        [Theory]
        [InlineData(100)]
        [InlineData(200)]
        [InlineData(500)]
        public void Test_ServiceBusTopicEntityManager_ReceiverMessageCount(int numMessagesToSend)
        {
            const string tableName = "ManagementCountTopic1";

            // Arrange - Create the queue entity using the manager, that will be used to count messages for.
            var manager = GetEntityManagerInstance();
            manager.CreateEntity(new ServiceBusEntityConfig { EntityType = EntityType.Topic, EntityName = tableName }).GetAwaiter().GetResult();

            var topicMessenger = GetTopicMessenger(tableName, "testSub");

            // Get the initial message count and expected message count after sending messages to the topic.
            var initialReceiverCount = (topicMessenger.EntityManager.GetReceiverMessageCount().GetAwaiter().GetResult()).ActiveEntityCount;
            var paragraphs = Lorem.GetParagraphs(numMessagesToSend).ToList();
            var expectedCount = initialReceiverCount + paragraphs.Count;

            // Act - send the test messages.
            topicMessenger.SendBatch(paragraphs).GetAwaiter().GetResult();

            Thread.Sleep(numMessagesToSend * 6);

            // Get the message count (both the sender and receiver will be the same - just verifying this).
            var updatedReceiverCount = (topicMessenger.EntityManager.GetReceiverMessageCount().GetAwaiter().GetResult()).ActiveEntityCount;
            var senderTargetCount = (topicMessenger.EntityManager.GetSenderMessageCount().GetAwaiter().GetResult()).ActiveEntityCount;

            // Assert - count results are incremented as expected.
            var entity = manager.GetEntity(tableName).GetAwaiter().GetResult();
            entity.EntityName.Should().Be(tableName);
            _ = updatedReceiverCount.Should().Be(expectedCount);
            _ = senderTargetCount.Should().Be(expectedCount);
        }

        /// <summary>Verify the disabled status of an entity. As newly created entities are NOT disabled by default when they are created, we will verify disabled is false.</summary>
        [Fact]
        public void Test_ServiceBusManager_CheckDisabled()
        {
            const string topicTableName = "ManagementDisabledTopic";
            var topicMessenger = GetTopicMessenger(topicTableName, "testSub");

            const string queueTableName = "ManagementDisabledQueue";
            var queueMessenger = GetQueueMessenger(queueTableName);

            topicMessenger.EntityManager.IsReceiverEntityDisabled().GetAwaiter().GetResult().Should().Be(false);
            topicMessenger.EntityManager.IsSenderEntityDisabled().GetAwaiter().GetResult().Should().Be(false);

            queueMessenger.EntityManager.IsReceiverEntityDisabled().GetAwaiter().GetResult().Should().Be(false);
            queueMessenger.EntityManager.IsSenderEntityDisabled().GetAwaiter().GetResult().Should().Be(false);
        }

        /// <summary>Ensure receiver/sender topic usage percentage is updated accurately after sending messages.</summary>
        [Theory]
        [InlineData(200)]
        [InlineData(400)]
        [InlineData(600)]
        public void Test_ServiceBusTopicEntityManager_ReceiverEntityUsagePercentage(int numMessagesToSend)
        {
            const string tableName = "ManagementPercentTopic";

            // Create the test entity.
            var manager = GetEntityManagerInstance();
            manager.CreateEntity(new ServiceBusEntityConfig { EntityName = tableName, EntitySubscriptionName = "testSub", EntityType = EntityType.Topic }).GetAwaiter().GetResult();

            // Quick test to verify "IsPremium" property (could be it's own test...).
            var entityInfo = manager.GetEntity(tableName).GetAwaiter().GetResult();
            entityInfo.IsPremium.Should().BeFalse();
            entityInfo.MaxEntitySizeMb.Should().BeGreaterThan(0);
            entityInfo.MaxMessageSizeBytes.Should().BeGreaterThan(0);

            var topicMessenger = GetTopicMessenger(tableName, "testSub");

            // Arrange - get the percent used.
            var receiverInitialUsagePercentage = topicMessenger.EntityManager.GetReceiverEntityUsagePercentage().GetAwaiter().GetResult();
            var senderInitialUsagePercentage = topicMessenger.EntityManager.GetSenderEntityUsagePercentage().GetAwaiter().GetResult();

            // Act - send messages to increase percentage used.
            var messages = new List<string>(Lorem.GetParagraphs(numMessagesToSend));
            topicMessenger.SendBatch(messages).GetAwaiter().GetResult();

            Thread.Sleep(2000);

            // Get the updated percentage used.
            var receiverUpdatedUsagePercentage = topicMessenger.EntityManager.GetReceiverEntityUsagePercentage().GetAwaiter().GetResult();
            var senderUpdatedUsagePercentage = topicMessenger.EntityManager.GetSenderEntityUsagePercentage().GetAwaiter().GetResult();

            // Assert.
            _ = receiverUpdatedUsagePercentage.Should().BeGreaterThan(0.00m);
            _ = receiverUpdatedUsagePercentage.Should().BeGreaterThan(receiverInitialUsagePercentage);
            _ = senderUpdatedUsagePercentage.Should().BeGreaterThan(senderInitialUsagePercentage);
            _ = receiverUpdatedUsagePercentage.Should().Be(senderUpdatedUsagePercentage);
        }

        /// <summary>Ensure receiver/sender queue usage percentage is updated accurately after sending messages.</summary>
        [Theory]
        [InlineData(100)]
        [InlineData(200)]
        [InlineData(500)]
        public void Test_ServiceBusQueueEntityManager_ReceiverEntityUsagePercentage(int numMessagesToSend)
        {
            const string tableName = "ManagementPercentQueue";

            // Create the test entity which we will measure the percentage used on.
            var manager = GetEntityManagerInstance();
            manager.CreateEntity(new ServiceBusEntityConfig { EntityType = EntityType.Queue, EntityName = tableName }).GetAwaiter().GetResult();

            var queueMessenger = GetQueueMessenger(tableName);

            // Arrange - get the initial percentage used.
            var receiverInitialUsagePercentage = queueMessenger.EntityManager.GetReceiverEntityUsagePercentage().GetAwaiter().GetResult();
            var senderInitialUsagePercentage = queueMessenger.EntityManager.GetSenderEntityUsagePercentage().GetAwaiter().GetResult();

            // Act - send messages to increase percentage used.
            var messages = new List<string>(Lorem.GetParagraphs(numMessagesToSend));
            queueMessenger.SendBatch(messages).GetAwaiter().GetResult();

            Thread.Sleep(numMessagesToSend * 6);

            // Get the updated percent used.
            var receiverUpdatedUsagePercentage = queueMessenger.EntityManager.GetReceiverEntityUsagePercentage().GetAwaiter().GetResult();
            var senderUpdatedUsagePercentage = queueMessenger.EntityManager.GetSenderEntityUsagePercentage().GetAwaiter().GetResult();

            // Assert - verify the increase.
            receiverUpdatedUsagePercentage.Should().BeGreaterThan(0.00m);
            receiverUpdatedUsagePercentage.Should().BeGreaterThan(receiverInitialUsagePercentage);
            senderUpdatedUsagePercentage.Should().BeGreaterThan(senderInitialUsagePercentage);
            receiverUpdatedUsagePercentage.Should().Be(senderUpdatedUsagePercentage);
        }

        /// <summary>Verify subscription rules are applied after entity purge.</summary>
        [Fact]
        public async Task Test_ServiceBusTopicMessenger_ManagerEntityPurge()
        {
            // Arrange
            var manager = GetEntityManagerInstance();
            string topicName = "managementpurgetopic";

            // Act/Assert - Create multiple subscriptions.
            manager.CreateEntity(new ServiceBusEntityConfig { EntityType = EntityType.Topic, EntityName = topicName, EntitySubscriptionName = "testsub1", SqlFilter = new KeyValuePair<string, string>("versionRule1", "Version = '2.0'") }).GetAwaiter().GetResult();
            manager.CreateEntity(new ServiceBusEntityConfig { EntityType = EntityType.Topic, EntityName = topicName, EntitySubscriptionName = "testsub1", SqlFilter = new KeyValuePair<string, string>("versionRule2", "Version = '3.0'") }).GetAwaiter().GetResult();
            manager.CreateEntity(new ServiceBusEntityConfig { EntityType = EntityType.Topic, EntityName = topicName, EntitySubscriptionName = "testsub2", SqlFilter = new KeyValuePair<string, string>("versionRule1", "Version = '2.0'") }).GetAwaiter().GetResult();
            manager.CreateEntity(new ServiceBusEntityConfig { EntityType = EntityType.Topic, EntityName = topicName, EntitySubscriptionName = "testsub2", SqlFilter = new KeyValuePair<string, string>("versionRule2", "Version = '3.0'") }).GetAwaiter().GetResult();

            // Get topic info.
            var returnedTopic = await manager.GetEntity(topicName);

            returnedTopic.EntityName.Should().Be(topicName);
            returnedTopic.SubscriptionCount.Should().Be(2);
            returnedTopic.MessageCount.ActiveEntityCount.Should().Be(0);
            returnedTopic.MessageCount.ErroredEntityCount.Should().Be(0);
            returnedTopic.PercentageUsed.Should().Be(0);
            returnedTopic.CurrentEntitySizeMb.Should().Be(0);

            // Purge the topic and preserve state.  We will verify everything is intact as expected after the purge.
            await manager.EntityFullPurge(topicName);

            // Get the topic info again - the subscription count is the most important thing to verify here.
            returnedTopic = await manager.GetEntity(topicName);

            returnedTopic.EntityName.Should().Be(topicName);
            returnedTopic.SubscriptionCount.Should().Be(2);
            returnedTopic.MessageCount.ActiveEntityCount.Should().Be(0);
            returnedTopic.MessageCount.ErroredEntityCount.Should().Be(0);
            returnedTopic.PercentageUsed.Should().Be(0);
            returnedTopic.CurrentEntitySizeMb.Should().Be(0);

            // Purge the topic but do not preserve the state.
            await manager.EntityFullPurge(topicName, false);

            // Get the topic info and verify everything was reset as expected.
            returnedTopic = await manager.GetEntity(topicName);

            returnedTopic.EntityName.Should().Be(topicName);
            returnedTopic.SubscriptionCount.Should().Be(0);
            returnedTopic.MessageCount.ActiveEntityCount.Should().Be(0);
        }

        /// <summary>
        /// Test that entity manager can create a queue.
        /// </summary>
        [Fact]
        public async Task Test_ServiceBusEntityManager_CreateEntity_CreateQueue()
        {
            // Arrange
            var entityName = "new-queue-test";
            var config = new ServiceBusEntityConfig() { EntityType = EntityType.Queue, EntityName = entityName };
            var manager = GetEntityManagerInstance();

            // Act - Create entity
            await manager.CreateEntity(config);

            //Assert - entity is created
            var entity = await manager.GetEntity(entityName);
            Assert.NotNull(entity);
            Assert.True(entity.EntityName == entityName);
            Assert.True(entity.EntityType == EntityType.Queue);
        }

        /// <summary>
        /// Test that entity manager can create a topic with no subs.
        /// </summary>
        [Fact]
        public async Task Test_ServiceBusEntityManager_CreateEntity_CreateTopic_NoSubsciption()
        {
            // Arrange
            var entityName = "new-topic-test";
            var config = new ServiceBusEntityConfig() { EntityType = EntityType.Topic, EntityName = entityName };
            var manager = GetEntityManagerInstance();

            // Act - Create entity
            await manager.CreateEntity(config);

            // Assert - entity is created
            var entity = await manager.GetEntity(entityName);
            Assert.NotNull(entity);
            Assert.True(entity.EntityName == entityName);
            Assert.True(entity.EntityType == EntityType.Topic);
            Assert.True(entity.SubscriptionCount == 0);
        }

        /// <summary>
        /// Test that entity manager can create a topic with subs.
        /// </summary>
        [Fact]
        public async Task Test_ServiceBusEntityManager_CreateEntity_CreateTopic_WithSubsciption()
        {
            // Arrange
            var entityName = "new-topic-test2";
            var config = new ServiceBusEntityConfig() { EntityType = EntityType.Topic, EntityName = entityName, EntitySubscriptionName = "test-sub" };
            var manager = GetEntityManagerInstance();

            // Act - Create entity
            await manager.CreateEntity(config);

            // Assert - entity is created
            var entity = await manager.GetEntity(entityName);
            Assert.NotNull(entity);
            Assert.True(entity.EntityName == entityName);
            Assert.True(entity.EntityType == EntityType.Topic);
            Assert.True(entity.SubscriptionCount == 1);
        }

        private ServiceBusManager GetEntityManagerInstance()
        {
            return new ServiceBusMessenger(new ConnectionConfig
            {
                ConnectionString = _config.GetValue<string>("ConnectionString")
            }, _logger).ConnectionManager;
        }

        private ServiceBusMessenger GetTopicMessenger(string entityName, string entitySub)
        {
            return new ServiceBusMessenger(
                new ConnectionConfig
                {
                    ConnectionString = _config.GetValue<string>("ConnectionString"),
                    Receiver = new ReceiverSetup()
                    {
                        EntityName = entityName,
                        EntitySubscriptionName = entitySub,
                        CreateEntityIfNotExists = true,
                        SupportStringBodyType = true
                    },
                    Sender = new SenderSetup { EntityName = entityName, MessageVersion = 2.00 }
                }, _logger);
        }

        private ServiceBusMessenger GetQueueMessenger(string entityName)
        {
            return new ServiceBusMessenger(new ConnectionConfig
            {
                ConnectionString = _config.GetValue<string>("ConnectionString"),
                Receiver = new ReceiverSetup()
                {
                    EntityType = EntityType.Queue,
                    EntityName = entityName,
                    CreateEntityIfNotExists = true
                },
                Sender = new SenderSetup
                {
                    EntityType = EntityType.Queue,
                    EntityName = entityName
                }
            }, _logger);
        }

        private void RemoveEntities()
        {
            if (_hasClearedDown)
                return;

            var manager = GetEntityManagerInstance();
            var entityTopics = new[]
            {
                "ManagementPurgeTopic",
                "ManagementPercentTopic",
                "ManagementDisabledTopic",
                "ManagementCountTopic"
            };
            var entityQueues = new[]
            {
                "ManagementPercentQueue",
                "ManagementDisabledQueue",
                "ManagementCountQueue"
            };

            foreach (var entity in entityTopics)
            {
                manager.DeleteEntity(EntityType.Topic, entity).GetAwaiter().GetResult();
            }

            foreach (var entity in entityQueues)
            {
                manager.DeleteEntity(EntityType.Queue, entity).GetAwaiter().GetResult();
            }

            _hasClearedDown = true;
        }
    }
}
