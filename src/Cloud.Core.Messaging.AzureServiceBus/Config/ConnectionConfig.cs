namespace Cloud.Core.Messaging.AzureServiceBus.Config
{
    using System;

    /// <summary>Connection string configuration for connecting to an instance of Service Bus.</summary>
    public class ConnectionConfig : ServiceBusConfig
    {
        /// <summary>
        /// Gets or sets the connection string.
        /// </summary>
        /// <value>
        /// The connection string.
        /// </value>
        public string ConnectionString
        {
            get => Connection;
            set => Connection = value; 
        }

        /// <summary>
        /// Returns a <see cref="string" /> that represents this instance.
        /// </summary>
        /// <returns>
        /// A <see cref="string" /> that represents this instance.
        /// </returns>
        /// <inheritdoc />
        public override string ToString()
        {
            return $"ReceiverEntity: {ReceiverEntity}, ReceiverEntity: {ReceiverSubscriptionName},, SenderEntity: {SenderEntity}," +
                   $"IsTopic: {IsTopic}, PollFrequency: {PollFrequencyInSeconds}, EnableStringBodyTypeSupport: {EnableStringBodyTypeSupport}, MessageVersion: {MessageVersion}";
        }

        /// <summary>
        /// Validates this instance.
        /// </summary>
        /// <exception cref="ArgumentException">
        /// ConnectionString must be set and ReceiverEntity OR SenderEntity must be set.
        /// </exception>
        /// <inheritdoc />
        public override void Validate()
        {
            if (ConnectionString.IsNullOrEmpty())
                throw new ArgumentException("ConnectionString must be set");

            base.Validate();
        }
    }
}
