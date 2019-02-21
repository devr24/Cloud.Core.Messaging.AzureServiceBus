using System.Collections.Generic;
using System.Reactive.Linq;
using Cloud.Core.Testing.Lorem;
using Microsoft.Extensions.Logging;
using System;
using System.Threading;
using Cloud.Core.Messaging.AzureServiceBus.Config;
using Cloud.Core.Testing;
using Microsoft.Extensions.Configuration;
using Xunit;

namespace Cloud.Core.Messaging.AzureServiceBus.Tests
{
    public class ServiceBusTests : IDisposable
    {
        private readonly ServiceBusMessenger _messenger;

        public ServiceBusTests()
        {
            var config = new ConfigurationBuilder().AddJsonFile("appSettings.json").Build();
            var sbConfig = new ServicePrincipleConfig
            {
                InstanceName = config.GetValue<string>("InstanceName"),
                AppSecret = config.GetValue<string>("AppSecret"),
                TenantId = config.GetValue<string>("TenantId"),
                AppId = config.GetValue<string>("AppId"),
                SubscriptionId = config.GetValue<string>("SubscriptionId"),
                SharedAccessPolicy = config.GetValue<string>("SharedAccessPolicy"),
                ReceiverSubscriptionName = config.GetValue<string>("ReceiverSubscriptionName"),
                ReceiverEntity = config.GetValue<string>("ReceiverEntity"),
                SenderEntity = config.GetValue<string>("SenderEntity"),
                MessageVersion = config.GetValue<double>("MessageVersion"),
                IsTopic = config.GetValue<bool>("IsTopic"),
                IsServiceLevelSharedAccessPolicy = true
            };
            Assert.NotEmpty(sbConfig.ToString());
            ILoggerFactory loggerFactory = new LoggerFactory()
                .AddConsole();

            ILogger<IReactiveMessenger> logger = loggerFactory.CreateLogger<IReactiveMessenger>();
            _messenger = new ServiceBusMessenger(sbConfig, logger);
            Assert.NotEmpty(_messenger.ConnectionConfig.ToString());
        }

        [Fact, IsIntegration]
        public void Test_ServiceBusMessenger_Msi()
        {
            var config = new ConfigurationBuilder().AddJsonFile("appSettings.json").Build();
            ILoggerFactory loggerFactory = new LoggerFactory().AddConsole();
            ILogger<IReactiveMessenger> logger = loggerFactory.CreateLogger<IReactiveMessenger>();
            var sbConfig = new MsiConfig
            {
                InstanceName = config.GetValue<string>("InstanceName"),
                TenantId = config.GetValue<string>("TenantId"),
                SubscriptionId = config.GetValue<string>("SubscriptionId"),
                SharedAccessPolicy = config.GetValue<string>("SharedAccessPolicy"),
                ReceiverSubscriptionName = config.GetValue<string>("ReceiverSubscriptionName"),
                ReceiverEntity = config.GetValue<string>("ReceiverEntity"),
                SenderEntity = config.GetValue<string>("SenderEntity"),
                MessageVersion = config.GetValue<double>("MessageVersion"),
                IsTopic = config.GetValue<bool>("IsTopic"),
                IsServiceLevelSharedAccessPolicy = true
            };
            AssertExtensions.DoesNotThrow(() =>
            {
                var messenger = new ServiceBusMessenger(sbConfig, logger);
                messenger.Send(Lorem.GetSentence()).GetAwaiter().GetResult();
            });
            Thread.Sleep(1000);
        }

        [Fact, IsIntegration]
        public void Test_ServiceBusMessenger_InitializeWithConnectionString()
        {
            var configFile = new ConfigurationBuilder().AddJsonFile("appSettings.json").Build();
            ILoggerFactory loggerFactory = new LoggerFactory().AddConsole();
            ILogger<IReactiveMessenger> logger = loggerFactory.CreateLogger<IReactiveMessenger>();

            var config = new ConnectionConfig()
            {
                ConnectionString = configFile.GetValue<string>("ConnectionString"),
                ReceiverEntity = "test",
                ReceiverSubscriptionName = "test",
                SenderEntity = "test"
            };

            Assert.NotEmpty(config.ToString());

            var messenger = new ServiceBusMessenger(config, logger);
            messenger.Dispose();
        }

        [Fact, IsIntegration]
        public void Test_ServiceBusMessenger_InitializeFail()
        {
            ILoggerFactory loggerFactory = new LoggerFactory().AddConsole();

            ILogger<IReactiveMessenger> logger = loggerFactory.CreateLogger<IReactiveMessenger>();
            var sbConfig = new ServicePrincipleConfig
            {
                AppSecret = "fakesecret",
                TenantId = "faketenantid",
                AppId = "fakeappid",
                SubscriptionId = "fakesubscription",
                InstanceName = "fakeinstance",
                SharedAccessPolicy = "fakepolicy",
                ReceiverSubscriptionName = "fakesubscriptionname",
                ReceiverEntity = "fakeentityname",
                SenderEntity = "fakeentityname",
                MessageVersion = 2.0,
                IsTopic = true
            };

            var messenger = new ServiceBusMessenger(sbConfig, logger);
            messenger.Dispose();
        }
        [Fact, IsIntegration]
        public void Test_ServiceBusMessenger_SendSingle()
        {
            AssertExtensions.DoesNotThrow(async () => await _messenger.Send(Lorem.GetSentence()));
        }
        
        [Fact, IsIntegration]
        public void Test_ServiceBusMessenger_SendBatch()
        {
            var messages = new List<string>();
            messages.AddRange(Lorem.GetParagraphs());
            AssertExtensions.DoesNotThrow(async () => await _messenger.SendBatch(messages));
        }

        [Fact, IsIntegration]
        public void Test_ServiceBusMessenger_ReceiveComplete()
        {
            AssertExtensions.DoesNotThrow(() =>
            {
                WaitTimeoutAction(async () =>
                {
                    var testMessage = Lorem.GetSentence();

                    // Send the message to initiate receive.
                    await _messenger.Send<string>(testMessage);

                    Thread.Sleep(2000);

                    _messenger.Receive<string>(async m =>
                    {
                        await _messenger.Complete(m);
                        stop = true;
                    }, err =>
                    {

                    });
                });
            });
        }

        [Fact, IsIntegration]
        public void Test_ServiceBusMessenger_ReceiveObservableComplete()
        {
            AssertExtensions.DoesNotThrow(() =>
            {
                WaitTimeoutAction(async () =>
                {
                    var testMessage = Lorem.GetSentence();

                    // Send the message to initiate receive.
                    await _messenger.Send<string>(testMessage);

                    Thread.Sleep(2000);

                    _messenger.StartReceive<string>().Take(1).Subscribe(async m =>
                    {
                        Thread.Sleep(60);
                        await _messenger.Complete(m);
                        stop = true;
                    });
                });
            });
        }

        [Fact, IsIntegration]
        public void Test_ServiceBusMessenger_ReceiveError()
        {
            AssertExtensions.DoesNotThrow(() =>
            {
                WaitTimeoutAction(async () =>
                {
                    var testMessage = Lorem.GetSentence();

                    // Send the message to initiate receive.
                    await _messenger.Send(testMessage);

                    Thread.Sleep(2000);

                    _messenger.StartReceive<string>(15).Take(1).Subscribe(async m =>
                    {
                        await _messenger.Error(m);
                        stop = true;
                    });
                });
            });
        }

        [Fact, IsIntegration]
        public void Test_ServiceBusMessenger_ReceiveAbandon()
        {
            AssertExtensions.DoesNotThrow(() =>
            {
                WaitTimeoutAction(async () =>
                {
                    var testMessage = Lorem.GetSentence();

                    // Send the message to initiate receive.
                    await _messenger.Send(testMessage);

                    Thread.Sleep(2000);

                    _messenger.StartReceive<string>().Take(1).Subscribe(async m =>
                    {
                        await _messenger.Abandon(m);
                        _messenger.CancelReceive<string>();
                        stop = true;
                    });
                });
            });
        }

        bool stop = false;
        private void WaitTimeoutAction(Action action)
        {
            stop = false;
            Thread.Sleep(2000);
            int count = 0;

            action();

            do
            {
                Thread.Sleep(1000);
                count++;
            } while (!stop && count < 20);
        }

        public void Dispose()
        {
            _messenger.Dispose();
        }
    }
}
