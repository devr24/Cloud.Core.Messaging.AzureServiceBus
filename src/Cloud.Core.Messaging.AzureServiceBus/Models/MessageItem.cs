using System;

namespace Cloud.Core.Messaging.AzureServiceBus.Models
{
    public class MessageItem<T> : IMessageItem<T>
    {
        public Exception Error { get; set; }
        public T Body { get; set; }
    }
}
