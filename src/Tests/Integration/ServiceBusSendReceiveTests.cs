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
    public class ServiceBusSendReceiveTests : IDisposable
    {
        private readonly IConfiguration _config;
        private readonly ILogger _logger;
        private bool _stopTimeout;

        public ServiceBusSendReceiveTests()
        {
            _config = new ConfigurationBuilder().AddJsonFile("appSettings.json").Build();
            _logger = new ServiceCollection().AddLogging(builder => builder.AddConsole())
                .BuildServiceProvider().GetService<ILogger<ServiceBusSendReceiveTests>>();
        }

        /// <summary>Ensure the message entity counts work as expected.</summary>
        [Fact]
        public void Test_ServiceBusMessenger_Count()
        {
            // Arrange
            var queueMessenger = GetQueueMessenger("testCountQueue");
            queueMessenger.ConnectionManager.EntityFullPurge(queueMessenger.ConnectionManager.ReceiverInfo.EntityName).GetAwaiter().GetResult();
            queueMessenger.ConnectionManager.EntityFullPurge(queueMessenger.ConnectionManager.SenderInfo.EntityName).GetAwaiter().GetResult();
            
            // Act/Assert
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

        /// <summary>Ensure message properties can be received as a typed object.</summary>
        [Fact]
        public void Test_ServiceBusTopicMessenger_ReadPropertiesTyped()
        {
            // Arrange
            var topicMessenger = GetTopicMessenger("testReadPropertiesTyped", "testSub");
            var listOfMessages = new List<TestProps>();

            for (int i = 1; i < 100; i++)
            {
                listOfMessages.Add(new TestProps { Test1 = i, Test2 = true });
            }

            // Act - Send the message with property - customising each one to have unique value for "SomeProp1".  We will then verify this.
            topicMessenger.SendBatch(listOfMessages, (item) =>
                new[] { new KeyValuePair<string, object>("Test1", item.Test1) }).GetAwaiter().GetResult();

            Thread.Sleep(2000);

            var msg = topicMessenger.ReceiveOneEntity<TestProps>();

            // Work around for failing tests
            if (msg == null)
                msg = topicMessenger.ReceiveOneEntity<TestProps>();

            do
            {
                // Arrange
                msg.Body.Test1.Should().BeGreaterThan(0).And.BeLessThan(100);
                msg.Properties.Keys.Should().Contain("Test1");
                ((ServiceBusMessageEntity<TestProps>)msg).GetPropertiesTyped<TestProps>().Test1.Should().Be(msg.Body.Test1);
                msg = topicMessenger.ReceiveOneEntity<TestProps>();
            } while (msg != null);
        }

        /// <summary>Ensure receiving a single message from a topic works as expected.</summary>
        [Fact]
        public void Test_ServiceBusTopicMessenger_ReceiveOne()
        {
            // Arrange
            var topicMessenger = GetTopicMessenger("testReceiveOne", "testSub");

            // Act - Send the message to initiate receive.
            CreateStringTestMessages(topicMessenger, 10).GetAwaiter().GetResult();
            Thread.Sleep(2000);
            var receivedMessage = topicMessenger.ReceiveOne<string>();

            // Work around for failing tests
            if (receivedMessage == null)
                receivedMessage = topicMessenger.ReceiveOne<string>();

            topicMessenger.QueueConnectors.TryGetValue(typeof(string), out var connector);

            if (connector == null)
                Assert.False(true, "Could not retrieve queue connector");

            // Assert
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
            ServiceBusMessenger.ConnectionStrings.Should().NotBeEmpty();
        }

        /// <summary>Ensure the update receiver works after a receive one was executed.</summary>
        [Fact]
        public void Test_ServiceBusTopicMessenger_UpdateReceiverReceiveOne()
        {
            // Arrange
            var topicOne = GetTopicMessenger("updatereceiverreceiveroneA", "subone");
            var topicTwo = GetTopicMessenger("updatereceiverreceivertwoB", "subtwo");

            ((ServiceBusManager)topicOne.EntityManager).EntityFullPurge("updatereceiverreceiveroneA", true).GetAwaiter().GetResult();
            ((ServiceBusManager)topicTwo.EntityManager).EntityFullPurge("updatereceiverreceivertwoB", true).GetAwaiter().GetResult();

            // Act/Assert
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

        /// <summary>Ensure receive a single message where its contents is typed, works as expected.</summary>
        [Fact]
        public void Test_ServiceBusTopicMessenger_ReceiveOneTyped()
        {
            // Arrange
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

            // Act/Assert - Send the message to initiate receive.
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
            var typedProps = ((ServiceBusMessageEntity<string>)receivedMessageEntity).GetPropertiesTyped<TestProps>();
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

        /// <summary>Ensure the topic can complete all messages in a list.</summary>
        [Fact]
        public void Test_ServiceBusTopicMessenger_CompleteAllMessages()
        {
            // Arrange
            var topicMessenger = GetTopicMessenger("testCompleteMany", "testSub");
            var testMessages = new List<string>(Lorem.GetParagraphs());

            // Act/Assert
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

        /// <summary>Ensure receiving a batch of messages from a queue works as expected.</summary>
        [Fact]
        public void Test_ServiceBusQueueMessenger_ReceiveBatch()
        {
            // Arrange
            var loopCounter = 0;
            var batchSize = 10;
            var messageCount = 55;
            var queueMessenger = GetQueueMessenger("batchEntityQueueMessenger");
            CreateStringTestMessages(queueMessenger, messageCount).GetAwaiter().GetResult();

            // Act - Send the message to initiate receive.
            Thread.Sleep(2000);

            do
            {
                var messages = queueMessenger.ReceiveBatch<string>(batchSize).GetAwaiter().GetResult();

                foreach (var msg in messages)
                {
                    queueMessenger.Complete(msg).GetAwaiter().GetResult();
                }

                loopCounter+= messages.Count();
            } while (loopCounter < messageCount);

            Console.WriteLine($"Messages processed {loopCounter}");
            loopCounter.Should().BeGreaterOrEqualTo(messageCount);
        }

        /// <summary>Ensure receiving a batch of entity messages from a queue works as expected.</summary>
        [Fact]
        public void Test_ServiceBusQueueMessenger_ReceiveBatchEntity()
        {
            // Arrange
            var loopCounter = 0;
            var batchSize = 100;
            var messageCount = 500;
            var queueMessenger = GetQueueMessenger("batchQueueMessenger");
            CreateStringTestMessages(queueMessenger, messageCount).GetAwaiter().GetResult();

            // Act - Send the message to initiate receive.
            Thread.Sleep(2000);

            do
            {
                var messages = queueMessenger.ReceiveBatchEntity<string>(batchSize).GetAwaiter().GetResult();
                
                messages.Count().Should().Be(100);
                
                queueMessenger.CompleteAll(messages.Select(m => m.Body)).GetAwaiter().GetResult();
                
                loopCounter += messages.Count();
            } while (loopCounter < messageCount);

            Console.WriteLine($"Messages processed {loopCounter}");
            loopCounter.Should().BeGreaterOrEqualTo(messageCount);
        }

        /// <summary>Ensure receiving a single message from a queue works as expected.</summary>
        [Fact]
        public void Test_ServiceBusQueueMessenger_ReceiveOne()
        {
            // Arrange
            var queueMessenger = GetQueueMessenger("testReceiveOneQueue");
            
            // Act - Send the message to initiate receive.
            CreateStringTestMessages(queueMessenger, 10).GetAwaiter().GetResult();
            Thread.Sleep(10000);

            var receivedMessage = queueMessenger.ReceiveOne<string>();

            // Assert
            Assert.NotNull(receivedMessage);
            queueMessenger.Complete(receivedMessage).GetAwaiter().GetResult();
        }

        /// <summary>Ensure a sending a large message on a queue throws argument out of range execption as expected.</summary>
        [Fact]
        public void Test_ServiceBusQueueMessenger_LargeMessage()
        {
            // Arrange
            var queueMessenger = GetQueueMessenger("testLargeMessage");
            var testMessage = string.Join(" ", Lorem.GetParagraphs(100));

            // Act/Assert - Send the message to initiate receive.
            Assert.Throws<ArgumentOutOfRangeException>(() => queueMessenger.Send(testMessage).GetAwaiter().GetResult());
        }

        /// <summary>Ensure a message on a topic can be received and completed as expected.</summary>
        [Fact]
        public void Test_ServiceBusTopicMessenger_ReceiveComplete()
        {
            // Arrange
            var topicMessenger = GetTopicMessenger("testReceiveComplete", "testSub");
            WaitTimeoutAction(async () =>
            {
                var testMessage = Lorem.GetSentences(10).ToList();

                // Act - Send the message to initiate receive.
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
                    // Assert
                    Assert.True(false, err.Message);
                    _stopTimeout = true;
                    topicMessenger.CancelReceive<string>();
                });
            });
        }

        /// <summary>Ensure an observable batch of messages on a topic can be received and completed as expected.</summary>
        [Fact]
        public void Test_ServiceBusTopicMessenger_ReceiveObservableBatch()
        {
            // Arrange
            var topicMessenger = GetTopicMessenger("testObservableBatch", "testSub");
            WaitTimeoutAction(async () =>
            {
                var testMessage = Lorem.GetSentences(100).ToList();

                // Act - Send the message to initiate receive.
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
                            // Assert
                            Assert.True(false, err.Message);
                            _stopTimeout = true;
                        });
            }, () =>
            {
                topicMessenger.CancelReceive<string>();
            });
        }

        /// <summary>Ensure a message on a queue can be received and completed as expected.</summary>
        [Fact]
        public void Test_ServiceBusQueueMessenger_ReceiveComplete()
        {
            // Arrange
            var queueMessenger = GetQueueMessenger("testReceiveCompleteQueue");
            WaitTimeoutAction(async () =>
            {
                var testMessage = Lorem.GetSentence();

                // Act - Send the message to initiate receive.
                await queueMessenger.Send(testMessage);

                Thread.Sleep(2000);

                queueMessenger.Receive<string>(async m =>
                {
                    await queueMessenger.Error(m);
                    _stopTimeout = true;
                    queueMessenger.CancelReceive<string>();
                }, err =>
                {
                    // Assert
                    Assert.True(false, err.Message);
                    _stopTimeout = true;
                    queueMessenger.CancelReceive<string>();
                });
            });
        }

        /// <summary>Ensure completing a message on a topic when received from an observable, works as expected.</summary>
        [Fact]
        public void Test_ServiceBusTopicMessenger_ReceiveObservableComplete()
        {
            // Arrange
            var topicMessenger = GetTopicMessenger("testObservableComplete", "testSub");
            WaitTimeoutAction(async () =>
            {
                var testMessage = Lorem.GetSentence();

                // Act - Send the message to initiate receive.
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
                    // Assert
                    Assert.True(false, err.Message);
                    topicMessenger.CancelReceive<string>();
                    _stopTimeout = true;
                    throw err;
                });
            });
        }

        /// <summary>Ensure completing a message on a queue when received from an observable, works as expected.</summary>
        [Fact]
        public void Test_ServiceBusQueueMessenger_ReceiveObservableComplete()
        {
            // Arrange
            var queueMessenger = GetQueueMessenger("testObservableCompleteQueue");
            WaitTimeoutAction(async () =>
            {
                var testMessage = Lorem.GetSentence();

                // Act - Send the message to initiate receive.
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
                    // Assert
                    Assert.True(false, err.Message);
                    queueMessenger.CancelReceive<string>();
                    _stopTimeout = true;
                });
            });
        }

        /// <summary>Ensure erroring a message on a topic works as expected.</summary>
        [Fact]
        public void Test_ServiceBusTopicMessenger_ReceiveError()
        {
            // Arrange
            var topicMessenger = GetTopicMessenger("testReceiveError", "testSub");
            WaitTimeoutAction(async () =>
            {
                var testMessage = Lorem.GetSentence();

                // Act - Send the message to initiate receive.
                await topicMessenger.Send(testMessage);

                Thread.Sleep(2000);

                topicMessenger.StartReceive<string>(15).Take(1).Subscribe(async m =>
                {
                    await topicMessenger.Error(m);
                    _stopTimeout = true;
                    topicMessenger.CancelReceive<string>();
                }, err =>
                {
                    // Assert
                    Assert.True(false, err.Message);
                    topicMessenger.CancelReceive<string>();
                    _stopTimeout = true;
                    throw err;
                });
            });
        }

        /// <summary>Ensure erroring a message on a queue works as expected.</summary>
        [Fact]
        public void Test_ServiceBusQueueMessenger_ReceiveError()
        {
            // Arrange
            var queueMessenger = GetQueueMessenger("testReceiveErrorQueue");
            WaitTimeoutAction(async () =>
            {
                var testMessage = Lorem.GetSentence();

                // Act - Send the message to initiate receive.
                await queueMessenger.Send(testMessage);

                Thread.Sleep(2000);

                queueMessenger.StartReceive<string>(15).Take(1).Subscribe(async m =>
                {
                    await queueMessenger.Error(m);
                    _stopTimeout = true;
                    queueMessenger.CancelReceive<string>();
                }, err =>
                {
                    // Assert
                    Assert.True(false, err.Message);
                    queueMessenger.CancelReceive<string>();
                    _stopTimeout = true;
                    throw err;
                });
            });
        }

        /// <summary>Ensure abandoning a message on a topic works as expected.</summary>
        [Fact]
        public void Test_ServiceBusTopicMessenger_ReceiveAbandon()
        {
            // Arrange
            var topicMessenger = GetTopicMessenger("testReceiveAbandon", "testSub");
            WaitTimeoutAction(async () =>
            {
                var testMessage = Lorem.GetSentence();

                // Act - Send the message to initiate receive.
                await topicMessenger.Send(testMessage);

                Thread.Sleep(2000);

                topicMessenger.StartReceive<string>().Take(1).Subscribe(async m =>
                {
                    await topicMessenger.Abandon(m);
                    _stopTimeout = true;
                    topicMessenger.CancelReceive<string>();
                }, err =>
                {
                    // Assert
                    Assert.True(false, err.Message);
                    _stopTimeout = true;
                    topicMessenger.CancelReceive<string>();
                });
            });
        }

        /// <summary>Ensure abandoning a message on a queue works as expected.</summary>
        [Fact]
        public void Test_ServiceBusQueueMessenger_ReceiveAbandon()
        {
            // Arrange
            var queueMessenger = GetQueueMessenger("testReceiveAbandonQueue");
            WaitTimeoutAction(async () =>
            {
                var testMessage = Lorem.GetSentence();

                // Act - Send the message to initiate receive.
                await queueMessenger.Send(testMessage);

                Thread.Sleep(2000);

                queueMessenger.StartReceive<string>().Take(1).Subscribe(async m =>
                {
                    await queueMessenger.Abandon(m, new KeyValuePair<string, object>[] { new KeyValuePair<string, object>("key", "value"),  });
                    _stopTimeout = true;
                    queueMessenger.CancelReceive<string>();
                }, err =>
                {
                    // Assert
                    Assert.True(false, err.Message);
                    _stopTimeout = true;
                    queueMessenger.CancelReceive<string>();
                });
            });
        }

        /// <summary>Ensure sending a batch of messages to a topic works as expected.</summary>
        [Fact]
        public void Test_ServiceBusTopicMessenger_SendBatch()
        {
            // Arrange
            var topicMessenger = GetTopicMessenger("testSendBatchTopic", "testSub");
            var messages = new List<string>();
            messages.AddRange(Lorem.GetParagraphs());

            // Act/Assert
            AssertExtensions.DoesNotThrow(async () => await topicMessenger.SendBatch(messages));
        }

        /// <summary>Ensure sending a batch of messages to a queue works as expected.</summary>
        [Fact]
        public void Test_ServiceBusQueueMessenger_SendBatch()
        {
            // Arrange
            var queueMessenger = GetQueueMessenger("testSendBatchQueue");
            var messages = new List<string>();
            messages.AddRange(Lorem.GetParagraphs());

            // Act/Assert
            AssertExtensions.DoesNotThrow(async () => await queueMessenger.SendBatch(messages));
        }

        /// <summary>Ensure sending a batch of messages to a topic with properties works as expected.</summary>
        [Fact]
        public void Test_ServiceBusTopicMessenger_SendBatchWithProperties()
        {
            // Arrange
            var topicMessenger = GetTopicMessenger("testSendPropsBatchTopic", "testSub");
            var messages = new List<string>();
            var properties = new KeyValuePair<string, object>[1];
            properties[0] = new KeyValuePair<string, object>("TestProperties", "Test");
            messages.AddRange(Lorem.GetParagraphs());

            // Act/Assert
            AssertExtensions.DoesNotThrow(async () => await topicMessenger.SendBatch(messages, properties));
        }

        /// <summary>Ensure an error is thrown during receiver setup when expected.</summary>
        [Fact]
        public async Task ServiceBusTopicMessenger_ReceiveSetupError()
        {
            // Arrange
            var config = new ConfigurationBuilder().AddJsonFile("appSettings.json").Build();
            var testMessenger = new ServiceBusMessenger(new ConnectionConfig()
            {
                ConnectionString = config.GetValue<string>("ConnectionString"),
            });

            // Act/Assert
            Assert.Throws<InvalidOperationException>(() => testMessenger.Receive<string>((msg) => { }, (err) => { }));
            Assert.Null(testMessenger.ReceiveOne<string>());
            await Assert.ThrowsAsync<NullReferenceException>(() => testMessenger.Send("test"));
        }

        /// <summary>Ensure sending a batch of messages to a queue with properties works as expected.</summary>
        [Fact]
        public void Test_ServiceBusQueueMessenger_SendBatchWithProperties()
        {
            // Arrannge
            var topicMessenger = GetTopicMessenger("testSendMorePropsBatchTopic", "testSub");
            var messages = new List<string>();
            var properties = new KeyValuePair<string, object>[1];
            properties[0] = new KeyValuePair<string, object>("TestProperties", "Test");

            // Act
            messages.AddRange(Lorem.GetParagraphs());

            // Assert
            AssertExtensions.DoesNotThrow(async () => await topicMessenger.SendBatch(messages, properties));
        }

        /// <summary>Ensure the receiver can be updated with different receiver info.</summary>
        [Fact]
        public void Test_ServiceBusQueueMessenger_UpdateReceiverChangesSubscription()
        {
            // Arrange
            var topicOne = GetTopicMessenger("updatereceiverreceiverone", "subone");
            var topicTwo = GetTopicMessenger("updatereceiverreceivertwo", "subtwo");

            ((ServiceBusManager)topicOne.EntityManager).EntityFullPurge("updatereceiverreceiverone", true).GetAwaiter().GetResult();
            ((ServiceBusManager)topicTwo.EntityManager).EntityFullPurge("updatereceiverreceivertwo", true).GetAwaiter().GetResult();

            var testMessageOne = "Message One";
            var testMessageTwo = "Message Two";

            // Act/Assert
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

        /// <summary>Ensure the receiver can be updated with different receiver info, moving from topic to queue.</summary>
        [Fact]
        public void Test_ServiceBusQueueMessenger_UpdateReceiverChangesSubscriptionWithDifferentType()
        {
            // Arrange
            var topicOne = GetTopicMessenger("updatereceiverreceiverone1", "subone1");
            var topicTwo = GetTopicMessenger("updatereceiverreceivertwo2", "subtwo2");

            ((ServiceBusManager)topicOne.EntityManager).EntityFullPurge("updatereceiverreceiverone1", true).GetAwaiter().GetResult();
            ((ServiceBusManager)topicTwo.EntityManager).EntityFullPurge("updatereceiverreceivertwo2", true).GetAwaiter().GetResult();

            var testMessageOne = "Message One";
            var testMessageTwo = new TestProps() { Test1 = 2, Test2 = true, Version = "new" };

            // Act/Assert
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

        [Fact]
        public void Test_ServiceBusTopic_ReceiveDefer()
        {
            // Arrange
            var topicMessenger = GetTopicMessenger("testReceiveAbandon", "testSub");
            WaitTimeoutAction(async () =>
            {
                var testMessage = Lorem.GetSentence();

                // Act - Send the message to initiate receive.
                await topicMessenger.Send(testMessage);

                Thread.Sleep(2000);

                topicMessenger.StartReceive<string>().Take(1).Subscribe(async m =>
                {
                    var props1 = topicMessenger.ReadProperties(m);
                    await topicMessenger.Defer(m, props1.Select(p => new KeyValuePair<string, object>(p.Key, p.Value)).ToArray());

                    await Task.Delay(2000);

                    var messages = await topicMessenger.ReceiveDeferredBatchEntity<string>(new List<long> { (long)props1["SequenceNumber"] });
                    var msg2 = messages.FirstOrDefault();
                    await topicMessenger.Defer(msg2.Body);

                    // This will be null because it takes time for the deferrals to propogate through.
                    var msgs = await topicMessenger.ReceiveDeferredBatch<string>(new List<long> { (long)msg2.Properties["SequenceNumber"] });  

                    // Assert
                    messages.Should().NotBeNull();
                    messages.FirstOrDefault().Should().NotBeNull();
                    msgs.Should().NotBeNull();
                    
                    _stopTimeout = true;
                    topicMessenger.CancelReceive<string>();

                }, err =>
                {
                    // Assert
                    Assert.True(false, err.Message);
                    _stopTimeout = true;
                    topicMessenger.CancelReceive<string>();
                });
            });
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

        private async Task CreateStringTestMessages(ServiceBusMessenger messenger, int numberToCreate)
        {
            var messages = new List<string>();
            for (int i = 0; i < numberToCreate; i++)
            {
                messages.Add(Lorem.GetSentence());
            }
            await messenger.SendBatch(messages);
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
            var manager = GetEntityManagerInstance();
            var entityTopics = new[]
            {
                "testCountTopic", "testReadPropertiesTyped", "testReceiveOne",
                "testReceiveOneTyped", "testCompleteMany", "testReceiveComplete",
                "testObservableBatch", "testObservableComplete", "testReceiveError",
                "testReceiveAbandon", "testSendBatchTopic", "testSendPropsBatchTopic", "updatereceiverreceivertwo", "updatereceiverreceiverone",
                "testSendMorePropsBatchTopic", "testReceiver", "updatereceiverreceivertwo2", "updatereceiverreceiverone1"
            };
            var entityQueues = new[]
            {
                "testCountQueue", "testReceiveOneQueue", "testLargeMessage","batchEntityQueueMessenger","batchQueueMessenger",
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

        #region IDisposable Support
        private bool disposedValue = false; // To detect redundant calls

        protected virtual void Dispose(bool disposing)
        {
            if (!disposedValue)
            {
                if (disposing)
                {
                    // Clear down all queues/topics before starting.
                    RemoveEntities();
                }

                disposedValue = true;
            }
        }

        // This code added to correctly implement the disposable pattern.
        public void Dispose()
        {
            // Do not change this code. Put cleanup code in Dispose(bool disposing) above.
            Dispose(true);
            GC.SuppressFinalize(this);
        }
        #endregion
    }
}
