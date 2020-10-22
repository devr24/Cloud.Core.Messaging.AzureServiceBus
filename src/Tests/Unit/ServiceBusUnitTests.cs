using System;
using System.Collections.Generic;
using System.Reactive.Subjects;
using Cloud.Core.Messaging.AzureServiceBus.Config;
using Cloud.Core.Messaging.AzureServiceBus.Models;
using Cloud.Core.Testing;
using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace Cloud.Core.Messaging.AzureServiceBus.Tests.Unit
{
    [IsUnit]
    public class ServiceBusUnitTests
    {
        /// <summary>Ensure the connector is disposed as expected.</summary>
        [Fact]
        public void Test_ServiceBusConnector_ServiceDispose()
        {
            // Assert
            var messenger = new ServiceBusMessenger(new ConnectionConfig { ConnectionString = "test.test" });
            var connector = new ServiceBusConnector<object>(new ServiceBusManager("", new ConnectionConfig()), new Subject<object>(), null);

            // Act - call dispose.
            messenger.Dispose();
            connector.Dispose();

            // Called a second time to test branch.
            messenger.Dispose();
            connector.Dispose();

            // Assert - disposed as expected.
            messenger.Disposed.Should().BeTrue();
            connector.Disposed.Should().BeTrue();
        }

        /// <summary>Ensure the enable backoff only works when sender is configured.</summary>
        [Fact]
        public void Test_ConfigBase_EnableBackoffEnabled()
        {
            // Arrange
            var config = new ConfigTest();

            // Act/Assert - backoff can only be set when both receiver and sender has been setup.
            config.EnableAutobackOff.Should().BeFalse();
            config.EnableAutobackOff = true;
            config.EnableAutobackOff.Should().BeFalse();
            config.Receiver = new ReceiverSetup();
            config.EnableAutobackOff.Should().BeFalse();
            config.Sender = new SenderSetup();
            config.EnableAutobackOff.Should().BeTrue();
        }

        /// <summary>Ensure config instance name is picked out as expected from the connection string.</summary>
        [Fact]
        public void Test_ConnectionConfig_InstanceName()
        {
            var config = new ConnectionConfig();
            config.InstanceName.Should().BeNull();

            config.ConnectionString = "AB";
            config.InstanceName.Should().Be(null);

            config.ConnectionString = "A.B";
            config.InstanceName.Should().Be(null);

            config.ConnectionString = "Endpoint=sb://A.B;C";
            config.InstanceName.Should().Be("A");
        }

        /// <summary>Check the add service collection extension configures as expected.</summary>
        [Fact]
        public void Test_ServiceBusMessenger_ServiceCollectionAddSingleton()
        {
            // Accept
            IServiceCollection serviceCollection = new ServiceCollection();

            // Act/Assert
            serviceCollection.AddServiceBusSingleton<IReactiveMessenger>("test", "test", "test");
            serviceCollection.ContainsService(typeof(IReactiveMessenger)).Should().BeTrue();
            serviceCollection.ContainsService(typeof(NamedInstanceFactory<IReactiveMessenger>)).Should().BeTrue();
            serviceCollection.Clear();

            serviceCollection.AddServiceBusSingletonNamed<IMessenger>("key1", "test", "test", "test");
            serviceCollection.AddServiceBusSingletonNamed<IMessenger>("key2", "test", "test", "test");
            serviceCollection.AddServiceBusSingletonNamed<IReactiveMessenger>("key3", "test", "test", "test");
            serviceCollection.AddServiceBusSingletonNamed<IReactiveMessenger>("key4", "test", "test", "test");
            serviceCollection.AddServiceBusSingleton<IMessenger>("test1", "test", "test");
            serviceCollection.AddServiceBusSingleton<IReactiveMessenger>("test2", "test", "test");
            serviceCollection.AddServiceBusSingletonNamed<IMessenger>("key5", new ConnectionConfig() { ConnectionString = "test" });
            serviceCollection.AddServiceBusSingletonNamed<IReactiveMessenger>("key6", new ConnectionConfig() { ConnectionString = "test" });
            serviceCollection.ContainsService(typeof(IReactiveMessenger)).Should().BeTrue();
            serviceCollection.ContainsService(typeof(IMessenger)).Should().BeTrue();
            serviceCollection.ContainsService(typeof(NamedInstanceFactory<IReactiveMessenger>)).Should().BeTrue();
            serviceCollection.ContainsService(typeof(NamedInstanceFactory<IMessenger>)).Should().BeTrue();

            var resolvedFactory1 = serviceCollection.BuildServiceProvider().GetService<NamedInstanceFactory<IReactiveMessenger>>();
            var resolvedFactory2 = serviceCollection.BuildServiceProvider().GetService<NamedInstanceFactory<IMessenger>>();
            resolvedFactory1["key3"].Should().NotBeNull();
            resolvedFactory1["key4"].Should().NotBeNull();
            resolvedFactory1["test2"].Should().NotBeNull();
            resolvedFactory2["key1"].Should().NotBeNull();
            resolvedFactory2["key2"].Should().NotBeNull();
            resolvedFactory2["test1"].Should().NotBeNull();
            resolvedFactory2["key5"].Should().NotBeNull();
            resolvedFactory1["key6"].Should().NotBeNull();
            serviceCollection.Clear();

            serviceCollection.AddServiceBusSingleton<IMessenger>("test", "test", "test");
            serviceCollection.ContainsService(typeof(IMessenger)).Should().BeTrue();
            serviceCollection.ContainsService(typeof(NamedInstanceFactory<IMessenger>)).Should().BeTrue();
            serviceCollection.Clear();

            serviceCollection.AddServiceBusSingleton<IReactiveMessenger>(new ServicePrincipleConfig
            {
                InstanceName = "test",
                AppId = "test",
                AppSecret = "test",
                TenantId = "test",
                SubscriptionId = "test"
            });
            serviceCollection.ContainsService(typeof(IReactiveMessenger)).Should().BeTrue();
            serviceCollection.Clear();

            serviceCollection.AddServiceBusSingleton<IReactiveMessenger>(new ConnectionConfig { ConnectionString = "test" });
            serviceCollection.ContainsService(typeof(IReactiveMessenger)).Should().BeTrue();
        }

        /// <summary>Check receiver info validate works as expected for the error queue.</summary>
        [Fact]
        public void Test_ReceiverInfo_ValidateErrorReceiver()
        {
            // Arrange/Act
            var errorReceiver = new ReceiverInfo()
            {
                EntityName = "test",
                EntityType = EntityType.Queue,
                CreateEntityIfNotExists = true,
                ReadFromErrorQueue = true,
                LockRenewalTimeInSeconds = 1,
                MaxLockDuration = new TimeSpan(0, 0, 3)
            };

            // Assert
            errorReceiver.ReceiverFullPath.Should().Be($"{errorReceiver.EntityName}{ReceiverSetup.DeadLetterQueue}");
            errorReceiver.LockRenewalTimeInSeconds.Should().Be(3);
            AssertExtensions.DoesNotThrow(() => errorReceiver.Validate());
        }

        /// <summary>Check receiver info validate works as expected for the active queue.</summary>
        [Fact]
        public void Test_ReceiverInfo_ValidateActiveReceiver()
        {
            // Arrange
            var activeReceiver = new ReceiverInfo()
            {
                EntityName = "test",
                EntityType = EntityType.Queue,
                CreateEntityIfNotExists = true,
                LockRenewalTimeInSeconds = 1,
                MaxLockDuration = new TimeSpan(0, 0, 1)
            };

            // Act/Assert
            activeReceiver.ReceiverFullPath.Should().Be($"{activeReceiver.EntityName}");
            activeReceiver.LockRenewalTimeInSeconds.Should().Be(1);
        }

        /// <summary>Check receiver info ReceiverFullPath gets configured as expected.</summary>
        [Fact]
        public void Test_ReceiverInfo_ReceiverFullPath()
        {
            // Arrange/Act
            var receiver = new ReceiverInfo { EntityType = EntityType.Topic };

            // Assert
            receiver.ReceiverFullPath.Should().BeNull();
            receiver.EntityName = "test";
            receiver.ReceiverFullPath.Should().BeNull();
            receiver.EntitySubscriptionName = "test";
            receiver.ReceiverFullPath.Should().Be("test/subscriptions/test");
            receiver.ReadFromErrorQueue = true;
            receiver.ReceiverFullPath.Should().Be("test/subscriptions/test/$deadletterqueue");
        }

        /// <summary>Ensure to string method works as expected for Connection config.</summary>
        [Fact]
        public void Test_ConnectionConfig_ToString()
        {
            // Arrange
            var conn = new ConnectionConfig();

            // Act/Assert
            conn.ToString().Should().StartWith("ConnectionString: [NOT SET]");
            conn.ConnectionString = "test";
            conn.ToString().Should().StartWith("ConnectionString: [SET]");
        }

        /// <summary>Ensure validation method works as expected for Msi config.</summary>
        [Fact]
        public void Test_MsiConfig_Validate()
        {
            // Arrange
            var msiConfig = new MsiConfig() { SharedAccessPolicyName = "" };

            // Act/Assert - Check the msi config validation.
            Assert.Throws<ArgumentException>(() => msiConfig.Validate());
            msiConfig.InstanceName = "test";
            Assert.Throws<ArgumentException>(() => msiConfig.Validate());
            msiConfig.TenantId = "test";
            Assert.Throws<ArgumentException>(() => msiConfig.Validate());
            msiConfig.SubscriptionId = "test";
            Assert.Throws<ArgumentException>(() => msiConfig.Validate());
            msiConfig.SharedAccessPolicyName = "test";
            AssertExtensions.DoesNotThrow(() => msiConfig.Validate());
            msiConfig.ToString().Should().NotBeNullOrEmpty();
        }

        /// <summary>Ensure validation method works as expected for Connection config.</summary>
        [Fact]
        public void Test_ConnectionConfig_Validate()
        {
            // Arrange
            var connectionConfig = new ConnectionConfig();

            // Act/Assert - Check connection string config validation.
            Assert.Throws<ArgumentException>(() => connectionConfig.Validate());
            connectionConfig.ConnectionString = "test";
            AssertExtensions.DoesNotThrow(() => connectionConfig.Validate());
            connectionConfig.ToString().Should().NotBeNullOrEmpty();
        }

        /// <summary>Ensure validation method works as expected for Service Principle config.</summary>
        [Fact]
        public void Test_ervicePrincipleConfig_Validate()
        {
            // Arrange
            var spConfig = new ServicePrincipleConfig() { SharedAccessPolicyName = "" };

            // Act/Assert - Check the service Principle config validation.
            Assert.Throws<ArgumentException>(() => spConfig.Validate());
            spConfig.InstanceName = "test";
            Assert.Throws<ArgumentException>(() => spConfig.Validate());
            spConfig.AppId = "test";
            Assert.Throws<ArgumentException>(() => spConfig.Validate());
            spConfig.AppSecret = "test";
            Assert.Throws<ArgumentException>(() => spConfig.Validate());
            spConfig.TenantId = "test";
            Assert.Throws<ArgumentException>(() => spConfig.Validate());
            spConfig.SubscriptionId = "test";
            Assert.Throws<ArgumentException>(() => spConfig.Validate());
            spConfig.SharedAccessPolicyName = "test";
            AssertExtensions.DoesNotThrow(() => spConfig.Validate());
            spConfig.ToString().Should().NotBeNullOrEmpty();
        }

        /// <summary>Ensure validation method works as expected for Receiver config.</summary>
        [Fact]
        public void Test_ReceiverConfig_Validate()
        {
            // Arrange
            var setup = new ReceiverSetup();

            // Act/Assert
            Assert.Throws<ArgumentException>(() => setup.Validate());
            setup.EntityType = EntityType.Queue;
            setup.EntityName = "test";
            AssertExtensions.DoesNotThrow(() => setup.Validate());
            setup.EntityType = EntityType.Topic;
            Assert.Throws<ArgumentException>(() => setup.Validate());
            setup.EntitySubscriptionName = "test";
            AssertExtensions.DoesNotThrow(() => setup.Validate());
            setup.ToString().Should().NotBeNull();
            setup.EntityFilter = new KeyValuePair<string, string>("test", "test");
            setup.ToString().Should().NotBeNull();
        }

        /// <summary>Verify attempt to call not implemented interface method gives expected error.</summary>
        [Fact]
        public void Test_ServiceBusMessenger_GetAccessTokenUrlNotImplemented()
        {
            // Arrange/Act
            var msiConfig = new MsiConfig() { SharedAccessPolicyName = "" };
            var connectionConfig = new ConnectionConfig();
            var spConfig = new ServicePrincipleConfig() { SharedAccessPolicyName = "TestPolicy", InstanceName = "testSBInstance", AppId = "TestAppId", AppSecret = "TestAppSecret", TenantId = "TestTenantId", SubscriptionId = "FakeSubscriptionId" };
            var serviceBus = new ServiceBusMessenger(spConfig);

            // Assert
            serviceBus.ToString().Should().NotBeNullOrEmpty();
            serviceBus.Name.Should().Be("testSBInstance");
            (serviceBus.Config as ServicePrincipleConfig).Should().NotBeNull();
        }

        /// <summary>Verify implicit conversion works as expected.</summary>
        [Fact]
        public void Test_MessageEntity_ImplicitConversion()
        {
            // Arrange
            var test = "hello";

            // Act
            var messageEntity = (ServiceBusMessageEntity<string>)test;

            // Assert
            messageEntity.Body.Should().BeEquivalentTo(test);
        }

        /// <summary>Check validate throws errors as expected.</summary>
        [Fact]
        public void Test_SenderInfo_Validate()
        {
            // Arrange
            var sender = new SenderInfo();

            // Act/Assert
            Assert.Throws<ArgumentException>(() => sender.Validate());
        }

        /// <summary>Verify typed properties are mapped as expected.</summary>
        [Fact]
        public void Test_ServiceBus_MessageEntity()
        {
            // Arrange
            var entity = new MessageEntity<TestObject>
            {
                Body = new TestObject
                {
                    Prop1 = "test"
                },
                Properties = new Dictionary<string, object>
                {
                    { "Prop1", "PropTest" }
                }
            };

            // Act
            var propEntity = entity.GetPropertiesTyped<TestObject>();

            // Assert
            entity.Body.Prop1.Should().BeEquivalentTo("test");
            propEntity.Prop1.Should().BeEquivalentTo("PropTest");
        }

        private class TestObject
        {
            public string Prop1 { get; set; }
        }
    }
    public class ConfigTest : ConfigBase
    {

    }
}
