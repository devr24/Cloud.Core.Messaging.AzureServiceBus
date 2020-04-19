namespace Cloud.Core.Messaging.AzureServiceBus.Config
{
    using System;
    using Models;

    /// <summary>
    /// Configuration Base class, used with each of the individual config classes.
    /// </summary>
    public abstract class ConfigBase
    {
        private bool _checkAutobackOff;

        /// <summary>
        /// Gets or sets the receiver configuration.
        /// </summary>
        /// <value>The receiver config.</value>
        public ReceiverSetup Receiver { get; set; }

        /// <summary>
        /// Gets or sets the sender configuration.
        /// </summary>
        /// <value>The sender config.</value>
        public SenderSetup Sender { get; set; }

        /// <summary>
        /// Gets or sets a value indicating whether [autoback off] should take place (only works when Sender and Receiver is set).
        /// When the sender is getting full, the receiver will be temporarily stopped.
        /// </summary>
        /// <value><c>true</c> if [enable autoback off]; otherwise, <c>false</c>.</value>
        public bool EnableAutobackOff {
            get => (_checkAutobackOff && Sender != null && Receiver != null);
            set => _checkAutobackOff = value;
        }

        /// <summary>
        /// Validates this instance.
        /// </summary>
        public virtual void Validate()
        {
            // Validate receiver config if set.
            if (Receiver != null)
                Receiver.Validate();

            // Validate the sender config if its been set.
            if (Sender != null)
                Sender.Validate();
        }

        /// <summary>
        /// Returns a <see cref="string" /> that represents this instance.
        /// </summary>
        /// <returns>A <see cref="string" /> that represents this instance.</returns>
        /// <inheritdoc />
        public override string ToString()
        {
            return $"{Environment.NewLine}ReceiverInfo: {(Receiver == null ? "[NOT SET]" : Receiver.ToString())}"+
                $"{Environment.NewLine}SenderInfo: {(Sender == null ? "[NOT SET]" : Sender.ToString())}";
        }
    }
}
