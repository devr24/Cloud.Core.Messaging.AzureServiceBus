namespace Cloud.Core.Messaging.AzureServiceBus.Config
{
    using System;
    using System.Linq;
    using Cloud.Core.Extensions;

    /// <summary>
    /// Connection string configuration for connecting to an instance of Service Bus.
    /// Implements the <see cref="ConfigBase" />
    /// </summary>
    /// <seealso cref="ConfigBase" />
    public class ConnectionConfig : ConfigBase
    {
        /// <summary>
        /// Gets or sets the connection string to connect with.
        /// </summary>
        /// <value>The connection string.</value>
        public string ConnectionString { get; set; }

        /// <summary>
        /// Service Bus instance name taken from the connection string.
        /// </summary>
        public string InstanceName {
            get
            {
                if (ConnectionString.IsNullOrEmpty())
                    return null;

                const string replaceStr = "Endpoint=sb://";

                var parts = ConnectionString.Split('.');

                if (parts.Length <= 1) {
                    return null;
                }

                // Account name is used as the identifier.
                return parts.FirstOrDefault(p => p.StartsWith(replaceStr))?.Replace(replaceStr, string.Empty);
            }
        }

        /// <summary>
        /// Returns a <see cref="string" /> that represents this instance.
        /// </summary>
        /// <returns>A <see cref="string" /> that represents this instance.</returns>
        /// <inheritdoc />
        public override string ToString()
        {
            return $"ConnectionString: {(ConnectionString != null ? "[SET]" : "[NOT SET]")}{base.ToString()}";
        }

        /// <summary>
        /// Validates this instance.
        /// </summary>
        /// <exception cref="System.ArgumentException">ConnectionString must be set</exception>
        /// <exception cref="ArgumentException">ConnectionString must be set and ReceiverEntity OR SenderEntity must be set.</exception>
        public override void Validate()
        {
            if (ConnectionString.IsNullOrEmpty())
                throw new ArgumentException("ConnectionString must be set");

            base.Validate();
        }
    }
}
