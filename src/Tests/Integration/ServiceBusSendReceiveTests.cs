using System;
using System.Collections.Generic;
using System.Linq;
using System.Reactive.Linq;
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
    public class ServiceBusSendReceiveTests
    {
        private readonly IConfiguration _config;
        private readonly ILogger _logger;
        private static bool _hasClearedDown;
        private bool _stopTimeout;

        public ServiceBusSendReceiveTests()
        {
            _config = new ConfigurationBuilder().AddJsonFile("appSettings.json").Build();
            _logger = new ServiceCollection().AddLogging(builder => builder.AddConsole())
                .BuildServiceProvider().GetService<ILogger<ServiceBusSendReceiveTests>>();

            // Clear down all queues/topics before starting.
            RemoveEntities();
        }

        [Fact]
        public void ServiceBusMessenger_Count()
        {
            var queueMessenger = GetQueueMessenger("testCountQueue");
            queueMessenger.ConnectionManager.EntityFullPurge(queueMessenger.ConnectionManager.ReceiverInfo.EntityName).GetAwaiter().GetResult();
            queueMessenger.ConnectionManager.EntityFullPurge(queueMessenger.ConnectionManager.SenderInfo.EntityName).GetAwaiter().GetResult();
            Assert.NotEmpty(queueMessenger.ConnectionManager.ToString());

            var topicMessenger = GetTopicMessenger("testCountTopic", "testSub");
            topicMessenger.ConnectionManager.EntityFullPurge(topicMessenger.ConnectionManager.ReceiverInfo.EntityName).GetAwaiter().GetResult();
            topicMessenger.ConnectionManager.EntityFullPurge(topicMessenger.ConnectionManager.SenderInfo.EntityName).GetAwaiter().GetResult();
            Assert.NotEmpty(topicMessenger.ConnectionManager.ToString());

            var queueCount = queueMessenger.ConnectionManager.GetReceiverMessageCount().GetAwaiter().GetResult();
            queueCount.ActiveEntityCount.Should().Be(0);
            queueCount.ErroredEntityCount.Should().Be(0);

            var topicCount = topicMessenger.ConnectionManager.GetReceiverMessageCount().GetAwaiter().GetResult();
            topicCount.ActiveEntityCount.Should().Be(0);
            topicCount.ErroredEntityCount.Should().Be(0);
        }

        [Fact]
        public void ServiceBusTopicMessenger_ReadPropertiesTyped()
        {
            var topicMessenger = GetTopicMessenger("testReadPropertiesTyped", "testSub");
            var listOfMessages = new List<TestProps>();

            for (int i = 1; i < 100; i++)
            {
                listOfMessages.Add(new TestProps { Test1 = i, Test2 = true });
            }

            // Send the message with property - customising each one to have unique value for "SomeProp1".  We will then verify this.
            topicMessenger.SendBatch(listOfMessages, (item) =>
                new[] { new KeyValuePair<string, object>("Test1", item.Test1) }).GetAwaiter().GetResult();

            Thread.Sleep(2000);

            var msg = topicMessenger.ReceiveOneEntity<TestProps>();

            // Work around for failing tests
            if (msg == null)
                msg = topicMessenger.ReceiveOneEntity<TestProps>();

            do
            {
                msg.Body.Test1.Should().BeGreaterThan(0).And.BeLessThan(100);
                msg.Properties.Keys.Should().Contain("Test1");
                msg.GetPropertiesTyped<TestProps>().Test1.Should().Be(msg.Body.Test1);
                msg = topicMessenger.ReceiveOneEntity<TestProps>();
            } while (msg != null);
        }

        [Fact]
        public void ServiceBusTopicMessenger_ReceiveOne()
        {
            var topicMessenger = GetTopicMessenger("testReceiveOne", "testSub");
            var testMessages = Lorem.GetSentence(3);

            // Send the message to initiate receive.
            topicMessenger.Send(testMessages).GetAwaiter().GetResult();
            Thread.Sleep(2000);
            var receivedMessage = topicMessenger.ReceiveOne<string>();

            // Work around for failing tests
            if (receivedMessage == null)
                receivedMessage = topicMessenger.ReceiveOne<string>();

            topicMessenger.QueueConnectors.TryGetValue(typeof(string), out var connector);

            if (connector == null)
                Assert.False(true, "Could not retrieve queue connector");

            var count = connector.GetReceiverMessageCount().GetAwaiter().GetResult();
            connector.Should().NotBeNull();

            Assert.NotNull(receivedMessage);
            topicMessenger.Complete(receivedMessage).GetAwaiter().GetResult();

            _ = topicMessenger.ReceiveOne<string>();
            Assert.NotEmpty(receivedMessage);

            count.ActiveEntityCount.Should().BeGreaterThan(0);
            connector.ShouldBackOff().Should().BeFalse();

            connector.Config.EnableAutoBackOff = true;
            connector.ShouldBackOff().Should().BeFalse();
            connector.Config.IsBackingOff.Should().BeFalse();
        }

        [Fact]
        public void ServiceBusTopicMessenger_UpdateReceiverReceiveOne()
        {
            // Arrange
            var topicOne = GetTopicMessenger("updatereceiverreceiveroneA", "subone");
            var topicTwo = GetTopicMessenger("updatereceiverreceivertwoB", "subtwo");

            ((ServiceBusManager)topicOne.EntityManager).EntityFullPurge("updatereceiverreceiveroneA", true).GetAwaiter().GetResult();
            ((ServiceBusManager)topicTwo.EntityManager).EntityFullPurge("updatereceiverreceivertwoB", true).GetAwaiter().GetResult();

            // Act and Assert
            var testMessageOne = "Message One";
            var testMessageTwo = "Message Two";

            // Send the message to initiate receive.
            topicOne.Send(testMessageOne).GetAwaiter().GetResult();
            topicTwo.Send(testMessageTwo).GetAwaiter().GetResult();

            Thread.Sleep(2000);

            var receivedMessageOne = topicOne.ReceiveOne<string>();

            var count = 0;
            while (receivedMessageOne == null && count < 10)
            {
                receivedMessageOne = topicOne.ReceiveOne<string>();
                count++;
            }

            Assert.Equal(receivedMessageOne, testMessageOne);
            topicOne.Complete(receivedMessageOne).GetAwaiter().GetResult();

            //Update receiver to listen to the second subscription
            (topicOne.EntityManager as ServiceBusManager)?.UpdateReceiver(new ReceiverInfo()
            {
                EntityName = "updatereceiverreceivertwoB",
                EntitySubscriptionName = "subtwo",
                SupportStringBodyType = true,
                CreateEntityIfNotExists = true
            }).GetAwaiter().GetResult();

            var receivedMessageTwo = topicOne.ReceiveOne<string>();
            count = 0;
            while (receivedMessageTwo == null && count < 10)
            {
                receivedMessageTwo = topicOne.ReceiveOne<string>();
                count++;
            }

            Assert.Equal(receivedMessageTwo, testMessageTwo);
            topicOne.Complete(receivedMessageTwo).GetAwaiter().GetResult();

            // Remove topic
            (topicOne.EntityManager as ServiceBusManager)?.DeleteEntity(EntityType.Topic, "updateReceiverReceiveOneA").GetAwaiter().GetResult();
            (topicOne.EntityManager as ServiceBusManager)?.DeleteEntity(EntityType.Topic, "updateReceiverReceiveTwoB").GetAwaiter().GetResult();
        }

        [Fact]
        public void ServiceBusTopicMessenger_ReceiveOneTyped()
        {
            var topicMessenger = GetTopicMessenger("testReceiveOneTyped", "testSub");
            var testMessage = Lorem.GetSentence(3);
            var props = new[]
            {
                new KeyValuePair<string, object>("Test1", 1), new KeyValuePair<string, object>("Test2", true)
            };

            var items = new List<string>
            {
                testMessage,
                testMessage,
                testMessage
            };

            // Send the message to initiate receive.
            topicMessenger.SendBatch(items, props).GetAwaiter().GetResult();

            Thread.Sleep(2000);
            var receivedMessageEntity = topicMessenger.ReceiveOneEntity<string>();

            // Work around for failing tests
            if (receivedMessageEntity == null)
                receivedMessageEntity = topicMessenger.ReceiveOneEntity<string>();

            topicMessenger.QueueConnectors.TryGetValue(typeof(string), out var connector);

            if (connector == null)
                Assert.False(true, "Could not retrieve queue connector");

            var count = connector.GetReceiverMessageCount().GetAwaiter().GetResult();
            connector.Should().NotBeNull();

            Assert.NotNull(receivedMessageEntity);
            receivedMessageEntity.Body.Should().Be(testMessage);
            receivedMessageEntity.Properties.Should().Contain(props);
            var typedProps = receivedMessageEntity.GetPropertiesTyped<TestProps>();
            typedProps.Test1.Should().Be(1);
            typedProps.Test2.Should().Be(true);
            typedProps.Version.Should().Be("2.00");

            topicMessenger.Complete(receivedMessageEntity.Body).GetAwaiter().GetResult();

            _ = topicMessenger.ReceiveOneEntity<string>();

            count.ActiveEntityCount.Should().BeGreaterThan(0);
            connector.ShouldBackOff().Should().BeFalse();

            connector.Config.EnableAutoBackOff = true;
            connector.ShouldBackOff().Should().BeFalse();
            connector.Config.IsBackingOff.Should().BeFalse();
        }

        [Fact]
        public void ServiceBusTopicMessenger_CompleteManyMessages()
        {
            var topicMessenger = GetTopicMessenger("testCompleteMany", "testSub");
            var testMessages = new List<string>(Lorem.GetParagraphs());

            topicMessenger.SendBatch(testMessages).GetAwaiter().GetResult();
            Thread.Sleep(2000);

            topicMessenger.QueueConnectors.TryGetValue(typeof(string), out var connector);

            if (connector == null)
                Assert.False(true, "Could not retrieve queue connector");

            var count = connector.GetReceiverMessageCount().GetAwaiter().GetResult();

            Assert.True(count.ActiveEntityCount == testMessages.Count);

            var messageList = new List<string>();

            while (messageList.Count != 3)
            {
                var message = topicMessenger.ReceiveOne<string>();
                if (message != null)
                {
                    messageList.Add(message);
                }
            }

            topicMessenger.CompleteAll(messageList).GetAwaiter().GetResult();

            count = connector.GetReceiverMessageCount().GetAwaiter().GetResult();

            Assert.True(count.ActiveEntityCount == 0);
        }

        [Fact]
        public void ServiceBusQueueMessenger_ReceiveOne()
        {
            var queueMessenger = GetQueueMessenger("testReceiveOneQueue");
            var testMessages = "TestMessage";

            // Send the message to initiate receive.
            queueMessenger.Send(testMessages).GetAwaiter().GetResult();
            Thread.Sleep(10000);

            var queueMessengerCount = queueMessenger.EntityManager.GetReceiverMessageCount().GetAwaiter().GetResult().ActiveEntityCount;

            for (long i = queueMessengerCount; i > 0; i--)
            {
                var receivedMessage = queueMessenger.ReceiveOne<string>();

                Assert.NotNull(receivedMessage);
                queueMessenger.Complete(receivedMessage).GetAwaiter().GetResult();

                Assert.NotEmpty(receivedMessage);
            }

            var lastMessage = queueMessenger.ReceiveOne<string>();
            lastMessage.Should().BeNull();
        }

        [Fact]
        public void ServiceBusQueueMessenger_LargeMessage()
        {
            var queueMessenger = GetQueueMessenger("testLargeMessage");
            var testMessage = string.Join(" ", Lorem.GetParagraphs(100));

            // Send the message to initiate receive.
            Assert.Throws<ArgumentOutOfRangeException>(() => queueMessenger.Send(testMessage).GetAwaiter().GetResult());
        }

        [Fact]
        public void ServiceBusTopicMessenger_ReceiveComplete()
        {
            var topicMessenger = GetTopicMessenger("testReceiveComplete", "testSub");
            WaitTimeoutAction(async () =>
            {
                var testMessage = Lorem.GetSentences(10).ToList();

                // Send the message to initiate receive.
                await topicMessenger.SendBatch(testMessage, (msgBody) =>
            {
                return new[] {
                            new KeyValuePair<string, object>("a", "b")
                            };
            }, 5);

                Thread.Sleep(2000);

                topicMessenger.Receive<string>(async m =>
                {
                    var props = topicMessenger.ReadProperties(m);
                    await topicMessenger.Abandon(m);
                    _stopTimeout = true;
                    topicMessenger.CancelReceive<string>();
                    props.Count.Should().BeGreaterThan(0);
                }, err =>
                {
                    Assert.True(false, err.Message);
                    _stopTimeout = true;
                    topicMessenger.CancelReceive<string>();
                });
            });
        }

        [Fact]
        public void ServiceBusTopicMessenger_ReceiveObservableBatch()
        {
            var topicMessenger = GetTopicMessenger("testObservableBatch", "testSub");
            WaitTimeoutAction(async () =>
            {
                var testMessage = Lorem.GetSentences(100).ToList();

                // Send the message to initiate receive.
                await topicMessenger.SendBatch(testMessage, (msgBody) =>
            {
                return new[] {
                            new KeyValuePair<string, object>("a", "b")
                            };
            }, 5);

                var counter = 0;

                topicMessenger.StartReceive<string>().
                    Buffer(() => Observable.Timer(TimeSpan.FromSeconds(5))).
                    Subscribe(msgs =>
                   {
                       if (msgs.Count > 0)
                       {
                           msgs.Count().Should().BeGreaterThan(1);
                           _stopTimeout = true;
                       }

                       if (counter > 2)
                           _stopTimeout = true;

                       counter++;
                   },
                        err =>
                        {
                            Assert.True(false, err.Message);
                            _stopTimeout = true;
                        });
            }, () =>
            {
                topicMessenger.CancelReceive<string>();
            });
        }

        [Fact]
        public void ServiceBusQueueMessenger_ReceiveComplete()
        {
            var queueMessenger = GetQueueMessenger("testReceiveCompleteQueue");
            WaitTimeoutAction(async () =>
            {
                var testMessage = Lorem.GetSentence();

                // Send the message to initiate receive.
                await queueMessenger.Send(testMessage);

                Thread.Sleep(2000);

                queueMessenger.Receive<string>(async m =>
                {
                    await queueMessenger.Error(m);
                    _stopTimeout = true;
                    queueMessenger.CancelReceive<string>();
                }, err =>
                {
                    Assert.True(false, err.Message);
                    _stopTimeout = true;
                    queueMessenger.CancelReceive<string>();
                });
            });
        }

        [Fact]
        public void ServiceBusTopicMessenger_ReceiveObservableComplete()
        {
            var topicMessenger = GetTopicMessenger("testObservableComplete", "testSub");
            WaitTimeoutAction(async () =>
            {
                var testMessage = Lorem.GetSentence();

                // Send the message to initiate receive.
                await topicMessenger.Send(testMessage);

                Thread.Sleep(2000);

                topicMessenger.StartReceive<string>().Take(1).Subscribe(async m =>
                {
                    Thread.Sleep(60);
                    await topicMessenger.Complete(m);
                    _stopTimeout = true;
                    topicMessenger.CancelReceive<string>();
                }, err =>
                {
                    topicMessenger.CancelReceive<string>();
                    _stopTimeout = true;
                    throw err;
                });
            });
        }

        [Fact]
        public void ServiceBusQueueMessenger_ReceiveObservableComplete()
        {
            var queueMessenger = GetQueueMessenger("testObservableCompleteQueue");
            WaitTimeoutAction(async () =>
            {
                var testMessage = Lorem.GetSentence();

                // Send the message to initiate receive.
                await queueMessenger.Send(testMessage);

                Thread.Sleep(2000);

                queueMessenger.StartReceive<string>().Take(1).Subscribe(async m =>
                {
                    Thread.Sleep(60);
                    await queueMessenger.Complete(m);
                    _stopTimeout = true;
                    queueMessenger.CancelReceive<string>();
                }, err =>
                {
                    Assert.True(false, err.Message);
                    queueMessenger.CancelReceive<string>();
                    _stopTimeout = true;
                });
            });
        }

        [Fact]
        public void ServiceBusTopicMessenger_ReceiveError()
        {
            var topicMessenger = GetTopicMessenger("testReceiveError", "testSub");
            WaitTimeoutAction(async () =>
            {
                var testMessage = Lorem.GetSentence();

                // Send the message to initiate receive.
                await topicMessenger.Send(testMessage);

                Thread.Sleep(2000);

                topicMessenger.StartReceive<string>(15).Take(1).Subscribe(async m =>
                {
                    await topicMessenger.Error(m);
                    _stopTimeout = true;
                    topicMessenger.CancelReceive<string>();
                }, err =>
                {
                    topicMessenger.CancelReceive<string>();
                    _stopTimeout = true;
                    throw err;
                });
            });
        }

        [Fact]
        public void ServiceBusQueueMessenger_ReceiveError()
        {
            var queueMessenger = GetQueueMessenger("testReceiveErrorQueue");
            WaitTimeoutAction(async () =>
            {
                var testMessage = Lorem.GetSentence();

                // Send the message to initiate receive.
                await queueMessenger.Send(testMessage);

                Thread.Sleep(2000);

                queueMessenger.StartReceive<string>(15).Take(1).Subscribe(async m =>
                {
                    await queueMessenger.Error(m);
                    _stopTimeout = true;
                    queueMessenger.CancelReceive<string>();
                }, err =>
                {
                    queueMessenger.CancelReceive<string>();
                    _stopTimeout = true;
                    throw err;
                });
            });
        }

        [Fact]
        public void ServiceBusTopicMessenger_ReceiveAbandon()
        {
            var topicMessenger = GetTopicMessenger("testReceiveAbandon", "testSub");
            WaitTimeoutAction(async () =>
            {
                var testMessage = Lorem.GetSentence();

                // Send the message to initiate receive.
                await topicMessenger.Send(testMessage);

                Thread.Sleep(2000);

                topicMessenger.StartReceive<string>().Take(1).Subscribe(async m =>
                {
                    await topicMessenger.Abandon(m);
                    _stopTimeout = true;
                    topicMessenger.CancelReceive<string>();
                }, err =>
                {
                    Assert.True(false, err.Message);
                    _stopTimeout = true;
                    topicMessenger.CancelReceive<string>();
                });
            });
        }

        [Fact]
        public void ServiceBusQueueMessenger_ReceiveAbandon()
        {
            var queueMessenger = GetQueueMessenger("testReceiveAbandonQueue");
            WaitTimeoutAction(async () =>
            {
                var testMessage = Lorem.GetSentence();

                // Send the message to initiate receive.
                await queueMessenger.Send(testMessage);

                Thread.Sleep(2000);

                queueMessenger.StartReceive<string>().Take(1).Subscribe(async m =>
                {
                    await queueMessenger.Abandon(m);
                    _stopTimeout = true;
                    queueMessenger.CancelReceive<string>();
                }, err =>
                {
                    Assert.True(false, err.Message);
                    _stopTimeout = true;
                    queueMessenger.CancelReceive<string>();
                });
            });
        }

        [Fact]
        public void ServiceBusTopicMessenger_SendBatch()
        {
            var topicMessenger = GetTopicMessenger("testSendBatchTopic", "testSub");
            var messages = new List<string>();
            messages.AddRange(Lorem.GetParagraphs());
            AssertExtensions.DoesNotThrow(async () => await topicMessenger.SendBatch(messages));
        }

        [Fact]
        public void ServiceBusQueueMessenger_SendBatch()
        {
            var queueMessenger = GetQueueMessenger("testSendBatchQueue");
            var messages = new List<string>();
            messages.AddRange(Lorem.GetParagraphs());
            AssertExtensions.DoesNotThrow(async () => await queueMessenger.SendBatch(messages));
        }

        [Fact]
        public void ServiceBusTopicMessenger_SendBatchWithProperties()
        {
            var topicMessenger = GetTopicMessenger("testSendPropsBatchTopic", "testSub");
            var messages = new List<string>();
            var properties = new KeyValuePair<string, object>[1];
            properties[0] = new KeyValuePair<string, object>("TestProperties", "Test");
            messages.AddRange(Lorem.GetParagraphs());
            AssertExtensions.DoesNotThrow(async () => await topicMessenger.SendBatch(messages, properties));
        }

        [Fact]
        public async System.Threading.Tasks.Task ServiceBusTopicMessenger_ReceiveSetupError()
        {
            var config = new ConfigurationBuilder().AddJsonFile("appSettings.json").Build();
            var testMessenger = new ServiceBusMessenger(new ConnectionConfig()
            {
                ConnectionString = config.GetValue<string>("ConnectionString"),
            });

            Assert.Throws<InvalidOperationException>(() => testMessenger.Receive<string>((msg) => { }, (err) => { }));
            Assert.Null(testMessenger.ReceiveOne<string>());
            await Assert.ThrowsAsync<NullReferenceException>(() => testMessenger.Send("test"));
        }

        [Fact]
        public void ServiceBusQueueMessenger_SendBatchWithProperties()
        {
            var topicMessenger = GetTopicMessenger("testSendMorePropsBatchTopic", "testSub");
            var messages = new List<string>();
            var properties = new KeyValuePair<string, object>[1];
            properties[0] = new KeyValuePair<string, object>("TestProperties", "Test");
            messages.AddRange(Lorem.GetParagraphs());
            AssertExtensions.DoesNotThrow(async () => await topicMessenger.SendBatch(messages, properties));
        }

        [Fact]
        public void ServiceBusQueueMessenger_UpdateReceiverChangesSubscription()
        {
            // Arrange
            var topicOne = GetTopicMessenger("updatereceiverreceiverone", "subone");
            var topicTwo = GetTopicMessenger("updatereceiverreceivertwo", "subtwo");

            ((ServiceBusManager)topicOne.EntityManager).EntityFullPurge("updatereceiverreceiverone", true).GetAwaiter().GetResult();
            ((ServiceBusManager)topicTwo.EntityManager).EntityFullPurge("updatereceiverreceivertwo", true).GetAwaiter().GetResult();

            // Act and Assert
            var testMessageOne = "Message One";
            var testMessageTwo = "Message Two";

            // Send the message to initiate receive.
            topicOne.Send(testMessageOne).GetAwaiter().GetResult();
            topicTwo.Send(testMessageTwo).GetAwaiter().GetResult();

            Thread.Sleep(2000);

            var config = new ConfigurationBuilder().AddJsonFile("appSettings.json").Build();
            var testMessenger = new ServiceBusMessenger(new ConnectionConfig()
            {
                ConnectionString = config.GetValue<string>("ConnectionString"),
                Receiver = new ReceiverSetup()
                {
                    EntityType = EntityType.Topic,
                    EntityName = "updatereceiverreceiverone",
                    EntitySubscriptionName = "subone"
                }
            });

            var receivedMessageOne = testMessenger.ReceiveOne<string>();

            var count = 0;
            while (receivedMessageOne == null && count < 10)
            {
                receivedMessageOne = testMessenger.ReceiveOne<string>();
                count++;
            }

            Assert.Equal(receivedMessageOne, testMessageOne);
            testMessenger.Complete(receivedMessageOne).GetAwaiter().GetResult();

            //Update receiver to listen to the second subscription
            testMessenger.UpdateReceiver("updatereceiverreceivertwo", "subtwo").GetAwaiter().GetResult();

            var receivedMessageTwo = testMessenger.ReceiveOne<string>();
            count = 0;
            while (receivedMessageTwo == null && count < 10)
            {
                receivedMessageTwo = testMessenger.ReceiveOne<string>();
                count++;
            }

            Assert.Equal(receivedMessageTwo, testMessageTwo);
            testMessenger.Complete(receivedMessageTwo).GetAwaiter().GetResult();
        }

        [Fact]
        public void ServiceBusQueueMessenger_UpdateReceiverChangesSubscriptionWithDifferentType()
        {
            // Arrange
            var topicOne = GetTopicMessenger("updatereceiverreceiverone1", "subone1");
            var topicTwo = GetTopicMessenger("updatereceiverreceivertwo2", "subtwo2");

            ((ServiceBusManager)topicOne.EntityManager).EntityFullPurge("updatereceiverreceiverone1", true).GetAwaiter().GetResult();
            ((ServiceBusManager)topicTwo.EntityManager).EntityFullPurge("updatereceiverreceivertwo2", true).GetAwaiter().GetResult();

            // Act and Assert
            var testMessageOne = "Message One";
            var testMessageTwo = new TestProps() { Test1 = 2, Test2 = true, Version = "new" };

            // Send the message to initiate receive.
            topicOne.Send(testMessageOne).GetAwaiter().GetResult();
            topicTwo.Send(testMessageTwo).GetAwaiter().GetResult();

            Thread.Sleep(2000);

            var config = new ConfigurationBuilder().AddJsonFile("appSettings.json").Build();
            var testMessenger = new ServiceBusMessenger(new ConnectionConfig()
            {
                ConnectionString = config.GetValue<string>("ConnectionString"),
                Receiver = new ReceiverSetup()
                {
                    EntityType = EntityType.Topic,
                    EntityName = "updatereceiverreceiverone1",
                    EntitySubscriptionName = "subone1"
                }
            });

            var receivedMessageOne = testMessenger.ReceiveOne<string>();

            var count = 0;
            while (receivedMessageOne == null && count < 10)
            {
                receivedMessageOne = testMessenger.ReceiveOne<string>();
                Task.Delay(500);
                count++;
            }

            receivedMessageOne.Should().BeEquivalentTo(testMessageOne);
            testMessenger.Complete(receivedMessageOne).GetAwaiter().GetResult();

            //Update receiver to listen to the second subscription
            testMessenger.UpdateReceiver("updatereceiverreceivertwo2", "subtwo2").GetAwaiter().GetResult();

            var receivedMessageTwo = testMessenger.ReceiveOne<TestProps>();

            count = 0;
            while (receivedMessageTwo == null && count < 10)
            {
                receivedMessageTwo = testMessenger.ReceiveOne<TestProps>();
                Task.Delay(500);
                count++;
            }
            testMessenger.Complete(receivedMessageTwo).GetAwaiter().GetResult();

            Assert.NotNull(receivedMessageTwo);
            receivedMessageTwo.Should().BeEquivalentTo(testMessageTwo);

            topicOne.CompleteAllMessages().GetAwaiter().GetResult();
            topicTwo.CompleteAllMessages().GetAwaiter().GetResult();
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
                        SupportStringBodyType = true,
                        EntityType = EntityType.Topic
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

        private void WaitTimeoutAction(Action action, Action finished = null)
        {
            _stopTimeout = false;
            Thread.Sleep(2000);
            int count = 0;

            action();

            do
            {
                Thread.Sleep(1000);
                count++;
            } while (!_stopTimeout && count < 20);

            finished?.Invoke();
        }

        private void RemoveEntities()
        {
            if (_hasClearedDown)
                return;

            var manager = GetEntityManagerInstance();
            var entityTopics = new[]
            {
                "testCountTopic", "testReadPropertiesTyped", "testReceiveOne",
                "testReceiveOneTyped", "testCompleteMany", "testReceiveComplete",
                "testObservableBatch", "testObservableComplete", "testReceiveError",
                "testReceiveAbandon", "testSendBatchTopic", "testSendPropsBatchTopic",
                "testSendMorePropsBatchTopic", "testReceiver"
            };
            var entityQueues = new[]
            {
                "testCountQueue", "testReceiveOneQueue", "testLargeMessage",
                "testReceiveErrorQueue", "testSendBatchQueue", "testreceiveabandonqueue",
                "testreceivecompletequeue", "testqueue", "testobservablecompletequeue",
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

        private class TestProps : IEquatable<TestProps>
        {
            public int Test1 { get; set; }
            public bool Test2 { get; set; }
            public string Version { get; set; }

            public bool Equals(TestProps other)
            {
                if (other == null)
                {
                    return false;
                }

                if (Test1 == other.Test1 && Test2 == other.Test2 && Version == other.Version)
                {
                    return true;
                }

                return false;
            }
        }

    }
}
