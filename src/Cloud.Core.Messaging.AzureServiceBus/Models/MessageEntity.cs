using System.Collections.Generic;

namespace Cloud.Core.Messaging.AzureServiceBus.Models
{
    public class MessageEntity<T> : IMessageEntity<T>
        where T: class
    {
        public T Body { get; set; }

        public IDictionary<string, object> Properties { get; set; }

        public TO GetPropertiesTyped<TO>() where TO : class, new()
        {
            return Properties.ToObject<TO>();
        }

        public static implicit operator MessageEntity<T>(T obj)
        {
            return new MessageEntity<T>() { Body = obj };
        }
    }
}
