using System.Linq;

namespace Cloud.Core.Messaging.AzureServiceBus.Extensions
{
    /// <summary>Object extensions.</summary>
    public static class ObjectExtensions
    {
        /// <summary>
        /// Gets the name of the property value by.
        /// </summary>
        /// <param name="item">The item.</param>
        /// <param name="propertyName">Name of the property.</param>
        /// <returns>System.Object.</returns>
        public static object GetPropertyValueByName(this object item, string propertyName)
        {
            var key = propertyName.ToLowerInvariant();
            var type = item.GetType();
            var prop = type.GetProperties().FirstOrDefault(x => x.Name.ToLowerInvariant() == key);

            return prop == null ? null : prop.GetValue(item);
        }
    }
}
