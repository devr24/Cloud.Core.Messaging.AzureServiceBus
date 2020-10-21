namespace Cloud.Core.Messaging.AzureServiceBus.Models
{
    using System.Collections.Generic;

    /// <summary>
    /// Class ServiceBusMessageEntity.
    /// Implements the <see cref="Cloud.Core.IMessageEntity{T}" />
    /// </summary>
    /// <typeparam name="T"></typeparam>
    /// <seealso cref="Cloud.Core.IMessageEntity{T}" />
    public class ServiceBusMessageEntity<T> : IMessageEntity<T>
        where T: class
    {
        /// <summary>
        /// Entity message body.
        /// </summary>
        /// <value>The body.</value>
        public T Body { get; set; }

        /// <summary>
        /// Collection of properties for the entity message.
        /// </summary>
        /// <value>The properties.</value>
        public IDictionary<string, object> Properties { get; set; }

        /// <summary>
        /// Gets the properties as a typed object.
        /// </summary>
        /// <typeparam name="TO">The type of to.</typeparam>
        /// <returns>TO.</returns>
        public TO GetPropertiesTyped<TO>() where TO : class, new()
        {
            return Properties.ToObject<TO>();
        }

        /// <summary>
        /// Performs an implicit conversion from type T to <see cref="ServiceBusMessageEntity{T}"/>.
        /// </summary>
        /// <param name="obj">The object.</param>
        /// <returns>The result of the conversion.</returns>
        public static implicit operator ServiceBusMessageEntity<T>(T obj)
        {
            return new ServiceBusMessageEntity<T>() { Body = obj };
        }
    }
}
