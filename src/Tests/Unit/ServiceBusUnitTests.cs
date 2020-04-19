using System;
using System.Collections.Generic;
using System.Linq;
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
        [Fact]
        public void Test_ServiceDispose()
        {
            var client = new ServiceBusMessenger(new ConnectionConfig { ConnectionString = "test.test" });
            client.Name.Should().BeNull();
            client.Dispose();
            client.Dispose();
            client.Disposed.Should().BeTrue();

            var connector = new ServiceBusConnector<object>(new ServiceBusManager("", new ConnectionConfig()), new Subject<object>(), null);
            connector.Dispose();
            connector.Dispose();
            connector.Disposed.Should().BeTrue();
        }

        [Fact]
        public void Test_ConfigBase_Initialise()
        {
            var config = new ConfigTest();
            config.EnableAutobackOff.Should().BeFalse();
            config.EnableAutobackOff = true;
            config.EnableAutobackOff.Should().BeFalse();
            config.Receiver = new ReceiverSetup();
            config.EnableAutobackOff.Should().BeFalse();
            config.Sender = new SenderSetup();
            config.EnableAutobackOff.Should().BeTrue();
        }

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

        [Fact]
        public void Test_ServiceBusMessenger_ServiceCollectionAddSingleton()
        {
            // Principle needs "Set" permissions to run this.
            IServiceCollection serviceCollection = new ServiceCollection();

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

        [Fact]
        public void Test_ServiceBusMessenge_ReceiverSetup()
        {
            var errorReceiver = new ReceiverInfo()
            {
                EntityName = "test",
                EntityType = EntityType.Queue,
                CreateEntityIfNotExists = true,
                ReadFromErrorQueue = true,
                LockRenewalTimeInSeconds = 1,
                MaxLockDuration = new TimeSpan(0, 0, 3)
            };

            errorReceiver.ReceiverFullPath.Should().Be($"{errorReceiver.EntityName}{ReceiverSetup.DeadLetterQueue}");
            errorReceiver.LockRenewalTimeInSeconds.Should().Be(3);

            var activeReceiver = new ReceiverInfo()
            {
                EntityName = "test",
                EntityType = EntityType.Queue,
                CreateEntityIfNotExists = true,
                LockRenewalTimeInSeconds = 1,
                MaxLockDuration = new TimeSpan(0, 0, 1)
            };

            activeReceiver.ReceiverFullPath.Should().Be($"{errorReceiver.EntityName}");
            activeReceiver.LockRenewalTimeInSeconds.Should().Be(1);

            AssertExtensions.DoesNotThrow(() => errorReceiver.Validate());

            var receiver = new ReceiverInfo { EntityType = EntityType.Topic };
            receiver.ReceiverFullPath.Should().BeNull();
            receiver.EntityName = "test";
            receiver.ReceiverFullPath.Should().BeNull();
            receiver.EntitySubscriptionName = "test";
            receiver.ReceiverFullPath.Should().Be("test/subscriptions/test");
            receiver.ReadFromErrorQueue = true;
            receiver.ReceiverFullPath.Should().Be("test/subscriptions/test/$deadletterqueue");

            var conn = new ConnectionConfig();
            conn.ToString().Should().StartWith("ConnectionString: [NOT SET]");
            conn.ConnectionString = "test";
            conn.ToString().Should().StartWith("ConnectionString: [SET]");
        }

        [Fact]
        public void Test_ServiceBusMessenger_ConfigValidation()
        {
            var msiConfig = new MsiConfig() { SharedAccessPolicyName = "" };
            var connectionConfig = new ConnectionConfig();
            var spConfig = new ServicePrincipleConfig() { SharedAccessPolicyName = "" };
            var setup = new ReceiverSetup();

            // Check the msi config validation.
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

            // Check connection string config validation.
            Assert.Throws<ArgumentException>(() => connectionConfig.Validate());
            connectionConfig.ConnectionString = "test";
            AssertExtensions.DoesNotThrow(() => connectionConfig.Validate());
            connectionConfig.ToString().Should().NotBeNullOrEmpty();

            // Check the service Principle config validation.
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

        [Fact]
        public void Test_ServiceBusMessenger_GetAccessTokenUrl()
        {
            var msiConfig = new MsiConfig() { SharedAccessPolicyName = "" };
            var connectionConfig = new ConnectionConfig();
            var spConfig = new ServicePrincipleConfig() { SharedAccessPolicyName = "TestPolicy", InstanceName = "testSBInstance", AppId = "TestAppId", AppSecret = "TestAppSecret", TenantId = "TestTenantId", SubscriptionId = "FakeSubscriptionId" };
            var setup = new ReceiverSetup();
            var serviceBus = new ServiceBusMessenger(spConfig);
            var sharedAccessConfig = new SignedAccessConfig(new List<AccessPermission> { AccessPermission.Read }, DateTimeOffset.UtcNow.AddDays(1));

            Assert.Throws<NotImplementedException>(() => serviceBus.GetSignedAccessUrl(sharedAccessConfig));
        }
    }

    public class ConfigTest : ConfigBase
    {

    }
}
