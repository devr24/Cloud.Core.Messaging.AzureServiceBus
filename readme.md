# **Cloud.Core.Messaging.ServiceBus**  
[![Build status](https://dev.azure.com/cloudcoreproject/CloudCore/_apis/build/status/Cloud.Core%20Packages/Cloud.Core.Messenger.AzureServiceBus_Package)](https://dev.azure.com/cloudcoreproject/CloudCore/_build/latest?definitionId=11) ![Code Coverage](https://cloud1core.blob.core.windows.net/codecoveragebadges/Cloud.Core.Messaging.AzureServiceBus-LineCoverage.png) [![Cloud.Core.Messaging.AzureServiceBus package in Cloud.Core feed in Azure Artifacts](https://feeds.dev.azure.com/cloudcoreproject/dfc5e3d0-a562-46fe-8070-7901ac8e64a0/_apis/public/Packaging/Feeds/8949198b-5c74-42af-9d30-e8c462acada6/Packages/10bce412-14c4-4eb3-b2fb-8c0def43f9eb/Badge)](https://dev.azure.com/cloudcoreproject/CloudCore/_packaging?_a=package&feed=8949198b-5c74-42af-9d30-e8c462acada6&package=10bce412-14c4-4eb3-b2fb-8c0def43f9eb&preferRelease=true)

<div id="description">

A lightweight adapter for Azure Service Bus (Queues and Topics), that abstracts consuming applications from the Service Bus technology stack by 
implementing common messaging interfaces.

</div>

## Design

One of the patterns used within this package (specifically when receiving messages) is the observable pattern.  This is possible because messages are "pumped" out from the receiver
as an observable collection.  You can read more on the observable pattern here: https://docs.microsoft.com/en-us/dotnet/standard/events/observer-design-pattern


## Usage

### Interface with Core
The *Cloud.Core* package contains these public interfaces for messaging (chain shown below)


</div>

The *Cloud.Core* package contains these public interfaces for messaging (chain shown below).  This package implements the releavant interfaces for ServiceBus.  The main focus of this package being separate from all the other Azure specific packages is to allow for a layer of abstraction in the calling applications.

If in the future the calling application was to be run in AWS or Google Cloud, the only thing that would need to be changed in code is the 
instantiation of IMessage.  Using this package, it would look like this:

```csharp
IReactiveMessenger sbMessenger = new ServiceBusMessenger(new MsiConfig());
```

Whereas the instantiation could easily be changed to use Google as follows:

```csharp
IReactiveMessenger gcMessenger = new PubSubMessenger(CloudPubSubConfig());
```



### IMessenger interface 

If you just want to only send messages, you would consume `ISendMessages`.

If you want to send and receive with a simple call back interface use the `IMessenger`.

If instead you want a reactive IObservable that you can subscribe to, use the `IReactiveMessenger`.

You can call `Abandon`, `Complete` and `Error` on the `IMessage` interface.

- All messages arrive with a perpetual Lock applied in the form of a renewal timer on the message.

- `Abandon` will abandon the message, returning it to the original queue.

- `Complete` will actually perform the "Read" on the message taking it from the queue.

- `Error` will move the message to the error queue (dead letter queue), used during critical processing errors where there may be problems with 
validation, business rules, incorrect data, etc.


### Why wrap the ServiceBus API?


The main application of the IMessenger interfaces is to allow calling applications to switch between their instance adapters without changing code.  The following code demonstates this:

```csharp
IReactiveMessenger msgQueue = new ServiceBusMessenger()
```

Can easily be changed (when developed) to:

```csharp
IReactiveMessenger msgQueue = new RabbitMQMessenger()
```


### Send and receive messages

The messenger implementation allows for generic [POCOs](https://en.wikipedia.org/wiki/Plain_Old_CLR_Object) class types to be used to define the type of messages being sent and received.  Using a generic allows the object types, already used within the calling app, to be reused as message contents.

Here's an example of a simple class that we'll send:

```csharp
public class TestMessage : IMessage
{
    public string Name { get; set; }
    public string Stuff { get; set; }
}
```
### Security and Configuration
There are three ways you can instantiate the Blob Storage Client.  Each way dictates the security mechanism the client uses to connect.  The three mechanisms are:

1. Connection String
2. Service Principle
3. Managed Service Identity or Managed User Identity


Below are examples of instantiating each type.

### 1. Connection String
Create an instance of the Service Bus client with ConnectionConfig for connection string as follows:

```csharp
var config = new ConnectionConfig
    {
        ConnectionString = "<connectionstring>"
		
        // All other required config set here
    };

// Service Bus client.
var messenger = new ServiceBusMessenger(config);	
```
Note: Instance name not required to be specified anywhere in configuration here as it is taken from the connection string.

### 2. Service Principle
Create an instance of the Blob Storage client with BlobStorageConfig for Service Principle as follows:

```csharp
var config = new ServicePrincipleConfig
    {
        AppId = "<appid>",
        AppSecret = "<appsecret>",
        TenantId = "<tenantid>",
        SubscriptionId = "<subscriptionId>",
        InstanceName = "<instanceName>",
		
        // All other required config set here
    };

// Service Bus client.
var messenger = new ServiceBusMessenger(config);	
```

Usually the AppId, AppSecret (both of which are setup when creating a new service principle within Azure) and TenantId are specified in 
Configuration (environment variables/AppSetting.json file/key value pair files [for Kubernetes secret store] or command line arguments).

SubscriptionId can be accessed through the secret store (this should not be stored in configuration).

### 3. Management Service Idenity (MSI) or MUI
Create an instance of the Blob Storage client with MSI authentication as follows:

```csharp
var config = new MsiConfig
    {
        TenantId = "<tenantid>",
        SubscriptionId = "<subscriptionId>",
        InstanceName = "<instanceName>",
		
        // All other required config set here
    };

// Service Bus client.
var messenger = new ServiceBusMessenger(config);		
```

All that's required is the instance name to connect to.  Authentication runs under the context the application is running.

### Configuring the client
The above shows the security specific configuration, there are other configurations needed.  Below is an example using Msi auth:

```csharp
var config = new ConfigurationBuilder().AddJsonFile("appSettings.json").Build();
var msiConfig = new MsiConfig
    {
        TenantId = config.GetValue<string>("tenantid"),
        InstanceName = config.GetValue<string>("instanceName"),
        SubscriptionId = config.GetValue<string>("subscriptionId")
		
        // Information about the queue or topic that will be listened to.
        Receiver = new ReceiverSetup
        {
            // use topic instead of queue (in this case topic - topic is set by default).
            UseTopic = true,
            // automatically create if its not there
            CreateEntityIfNotExists = true, 
            // name of topic
            EntityName = "RobertsTestTopic", 
            // subscription to listen on
            EntitySubscriptionName = "RobertsTestSubscription",  
            // (if creating) specify filter to apply to subscription.  This example filters on messages tagged version 1.0.
            EntityFilter = new KeyValuePair<string, string>("RobertFilterExample", "Version = '1.0'"),
            // How ofter to renew the lock on the message.  Uses the Max Allowed Locktime from service bus to make sure its 
            // always less than the max allowed value if a larger value is specified.  Actually renews on 80% of this time.
            LockRenewalTimeInSeconds = 60,
            // How often to check for messages.  0.05 is the default BUT can be slowed down (or sped up) as required.
            PollFrequencyInSeconds = 0.05,
            // Old versions of service bus defaulted to string content.  Now it's default is stream.  There's a bit of overhead
            // when dealing with the string types, so this is defaulted to false unless you know you want extra compatibility when
            // setting up your listener (like listening off a subscription an old function sends to).
            SupportStringBodyType = false
        },
        // Information about the queue or topic to send messages to.
        Sender = new SenderSetup
        {
            // use queue instead of topic.
            UseTopic = false,
            // Create the queue if it doesn't already exist.
            CreateEntityIfNotExists = true,
            // Name of queue to send to.
            EntityName = "RobertsTestQueue",
            // Property "Version" for the message.  Always set, can be used for filtering.
            MessageVersion = 2.1
        }
    };

// Service Bus client.
var messenger = new ServiceBusMessenger(msiConfig);		
```

This can be simplified to:

```csharp

  var messenger = new ServiceBusMessenger(new  MsiConfig
  {
      InstanceName = namespaceHelper.MessagingServiceInstanceName,
      SubscriptionId = Settings.SubscriptionId,
      TenantId = Settings.TenantId,

      // Information about the queue or topic that will be listened to.
      Receiver = new ReceiverSetup
      {
          EntityName = "RobertsTestTopic",
          EntitySubscriptionName = "RobertsTestSubscription"
      },
      // Information about the queue or topic to send messages to.
      Sender = new SenderSetup
      {
          // use queue instead of topic.
          UseTopic = false,
          EntityName = "RobertsTestQueue"
      }
  });
 ```
_Note: only differences here is message version defaults to 1.0 if not specified._

### How to send a message

The simplest way to do it is by consuming IMessenger and calling `Send` for a single message and `SendBatch` to send a batch of messages (the package handles sending the list of items in batches for you):
```csharp
IMessenger msn = new ServiceBusMessenger(configuration);

msn.Send(new TestMessage{ Name = "Some Name", Stuff = "Some Stuff"  });

msn.SendBatch(new List<TestMessage> {  
  new TestMessage{ Name = "Some Name 1", Stuff = "Some Stuff 1"  },
  new TestMessage{ Name = "Some Name 2", Stuff = "Some Stuff 2"  },
  new TestMessage{ Name = "Some Name 3", Stuff = "Some Stuff 2"  }
});
```
If you set `.ConfigureAwait(false)` the code will continue in a synchronous manner, without awaitng for the task to complete.

### Entity Disabled Exception
If you try to send messages to a queue or topic (or topic subscription) that is disabled, the `Send` code will throw an Cloud.Core.Messenger.EntityDisabledException that you can specifically catch and handle in your code. 

The custom error allows code to stay generic and not need to look for service bus specific errors.  Littering code with platform specific api code.  Extend these custom errors as new scenarios arise.

**Example**
```csharp
try
{
    // Example of sending a single message to the configured queue.
    await messenger.Send<string[]>(new[] { "test" });
}
catch (Core.Exceptions.EntityDisabledException edEx)
{
    // exception occured - handle - potentially put app to sleep and try again shortly...
}
```

### How to constantly receive messages using observables
You can subscribe to new messages using the observable provided by the IReactiveMessenger interface.

```csharp
IReactiveMessenger msn = new ServiceBusMessenger(config);
            
msn.StartReceive<TestMessage>().Subscribe(
  async receivedMsg => {
  
      // Write processing code here...

      // after processing, complete the message.
      await msn.Complete(receivedMsg);
  },
  failedEx => {  
      // an exception has occurred.
  });
```

### How to constantly receive messages using callbacks
You can pass callback's into the Receive method as provided IMessenger interface.

```csharp
IMessenger msn = new ServiceBusMessenger(config);
            
msn.Receive<TestMessage>(
  async receivedMsg => {
  
      // Write processing code here...

      // after processing, complete the message.
      await msn.Complete(receivedMsg);
  },
  failedEx => {  
      // an exception has occurred.
  });
```

### How to receive one message at a time
You can stay in control of messages arrive by using the receive one method as shown below.  This is for scenarios where messages are not to be constantly streamed.

```csharp
IMessenger msn = new ServiceBusMessenger(config);
            
var singleMessage = msn.ReceiveOne<TestMessage>();

// Process message...

await msn.Complete(singleMessage);
```


## Full working example

```csharp
var messenger = new ServiceBusMessenger(new Core.Messaging.AzureServiceBus.Config.MsiConfig
{
    InstanceName = namespaceHelper.MessagingServiceInstanceName,
    SubscriptionId = Settings.SubscriptionId,
    TenantId = Settings.TenantId,
                    
    // Information about the queue or topic that will be listened to.
    Receiver = new ReceiverSetup
    {
        // use topic instead of queue (in this case topic - topic is set by default).
        UseTopic = true,
        // automatically create if its not there
        CreateEntityIfNotExists = true, 
        // name of topic
        EntityName = "RobertsTestTopic", 
        // subscription to listen on
        EntitySubscriptionName = "RobertsTestSubscription",  
        // (if creating) specify filter to apply to subscription.  This example filters on messages tagged version 1.0.
        EntityFilter = new KeyValuePair<string, string>("RobertFilterExample", "Version = '1.0'"),
        // How ofter to renew the lock on the message.  Uses the Max Allowed Locktime from service bus to make sure its 
        // always less than the max allowed value if a larger value is specified.  Actually renews on 80% of this time.
        LockRenewalTimeInSeconds = 60,
        // How often to check for messages.  0.05 is the default BUT can be slowed down (or sped up) as required.
        PollFrequencyInSeconds = 0.05,
        // Old versions of service bus defaulted to string content.  Now it's default is stream.  There's a bit of overhead
        // when dealing with the string types, so this is defaulted to false unless you know you want extra compatibility when
        // setting up your listener (like listening off a subscription an old function sends to).
        SupportStringBodyType = false
    },
    // Information about the queue or topic to send messages to.
    Sender = new SenderSetup
    {
        // use queue instead of topic.
        UseTopic = false,
        // Create the queue if it doesnt already exist.
        CreateEntityIfNotExists = true,
        // Name of queue to send to.
        EntityName = "RobertsTestQueue",
        // Property "Version" for the message.  Always set, can be used for filtering.
        MessageVersion = 2.1
    }
});

var manager = messenger.EntityManager;

// The `ToString()` method shows full information about the service bus instance, sender and receiver.
Console.WriteLine(manager.ToString());

// The entity manager also contains useful methods for getting message count and entity usage percentage.
var senderMessageCount = await manager.GetSenderMessageCount();
var senderEntityUsagePercent = await manager.GetSenderEntityUsagePercentage();
var receiverEntityUsagePercent = await manager.GetReceiverEntityUsagePercentage();
var receiverMessageCount = await manager.GetReceiverMessageCount();
var isReceiverEntityDisabled = await manager.IsReceiverEntityDisabled();
var isSenderEntityDisabled = await manager.IsSenderEntityDisabled();
                
// Example of sending a single message to the configured queue.
await messenger.Send<string>("test");

// Receive one message from the configured topic (runs synchronously).
var messageItem = messenger.ReceiveOne<string>();

// Setup a subscribable to constantly listent for new messages arrive.
// Application needs to stay alive to keep this running.  Use AppHost `RunAndBlock()` method to support this.
// Alternative for testing is `Console.ReadLine()`.
messenger.StartReceive<string>(10).Subscribe(async message =>
{
    // Process messages here....

    // Complete the message when finished.
    await messenger.Abandon(message);   // return message to queue/topic without completing (will be picked up again).
    await messenger.Error(message);     // deadletter message.
    await messenger.Complete(message);  // complete and remove the message.
});

// When we no longer need to listen for messages, we can cancel using this:
messenger.CancelReceive<string>();

// When finished with the messenger, you can dispose the instance.
messenger.Dispose();
```

## Back-off Mechanism 

This API comes with an optional backoff-mechanism.  It only works when the wrapper is both receiving messages from one queue/topic and then sending on to another.  

When enabled (use the `EnableAutobackOff = true` config option), the code will monitor the topic its sending messages to and if it becomes greater than 90%, it will "Backoff" by stopping the receiver temporarily for 2 minutes.  After the 2 minutes are up, it will check the sender entity to see if it's fallen below the threshold of 90% and if so, will resume the receive and begin sending messages again.

This will apply back pressure - i.e. the receiver entity will fill up then, so be careful when using.  Works better if the previous component (sending messages to the receiver entity also has back-off enabled).


## *NOTE* Ones to watch out for...
### Body Type
There are three body types supported when sending messages - Stream, String and WCF.  By default ALL messages are sent and (expected to be) received 
with body type *Stream".  Content as a stream is more performant, so by default String support *IS NOT* enabled.  To enable this, when instantiating
the AzureServiceBus client and passing in the configuration, make sure to set the *SupportStringBodyType* property to _true_.

This property has been retrospectively added for backwards support with existing Topics and messages.

### Topic Setup - Don't enable partitioning
Preimum and should have "EnablePartitioning" set to false.  This can only be set when the topic is being created
so make sure that is the case when the infrastructure deployment scripts are being setup.


## Test Coverage
A threshold will be added to this package to ensure the test coverage is above 80% for branches, functions and lines.  If it's not above the required threshold 
(threshold that will be implemented on ALL of the core repositories to gurantee a satisfactory level of testing), then the build will fail.

## Compatibility
This package has has been written in .net Standard and can be therefore be referenced from a .net Core or .net Framework application. The advantage of utilising from a .net Core application, 
is that it can be deployed and run on a number of host operating systems, such as Windows, Linux or OSX.  Unlike referencing from the a .net Framework application, which can only run on 
Windows (or Linux using Mono).
 
## Setup
This package is built using .net Standard 2.1 and requires the .net Core 3.1 SDK, it can be downloaded here: 
https://www.microsoft.com/net/download/dotnet-core/

IDE of Visual Studio or Visual Studio Code, can be downloaded here:
https://visualstudio.microsoft.com/downloads/

## How to access this package
All of the Cloud.Core.* packages are published to a internal NuGet feed.  To consume this on your local development machine, please add the following feed to your feed sources in Visual Studio:
https://pkgs.dev.azure.com/cloudcoreproject/CloudCore/_packaging/Cloud.Core/nuget/v3/index.json
 
For help setting up, follow this article: https://docs.microsoft.com/en-us/vsts/package/nuget/consume?view=vsts


<img src="https://cloud1core.blob.core.windows.net/icons/cloud_core_small.PNG" />
