using System.Collections;

namespace Xtraq.Extensions;

/// <summary>
/// Provides reflection-driven helpers that merge configuration objects by overwriting non-empty values.
/// </summary>
public static class FileManagerExtensions
{
    /// <summary>
    /// Overwrites the writable properties of the target instance with non-null values from the source instance.
    /// </summary>
    /// <typeparam name="T">The reference type that exposes writable properties.</typeparam>
    /// <param name="target">The destination instance that receives new property values.</param>
    /// <param name="source">The source instance that supplies candidate property values.</param>
    /// <returns>The mutated target instance, or the source instance when the original target is null.</returns>
    public static T OverwriteWith<T>(this T target, T source) where T : class
    {
        if (source == null)
        {
            return target;
        }

        if (target == null)
        {
            return source;
        }

        var properties = target.GetType().GetProperties();

        foreach (var property in properties.Where(p => p.CanWrite))
        {
            var propertyType = property.PropertyType;
            var sourceValue = property.GetValue(source, null);

            if (sourceValue == null ||
                (propertyType == typeof(string) && string.IsNullOrWhiteSpace(sourceValue.ToString())))
            {
                continue;
            }

            if (propertyType.IsCollection())
            {
                if (sourceValue is IEnumerable sourceCollection && sourceCollection.Cast<object>().Any())
                {
                    property.SetValue(target, sourceValue, null);
                }

                continue;
            }

            if (propertyType.IsClass && !propertyType.IsSealed)
            {
                var targetValue = property.GetValue(target, null);
                if (targetValue != null)
                {
                    sourceValue = targetValue.OverwriteWith(sourceValue);
                }
            }

            property.SetValue(target, sourceValue, null);
        }

        return target;
    }

    /// <summary>
    /// Determines whether the type represents a collection (excluding <see cref="string"/>).
    /// </summary>
    /// <param name="propertyType">The type to evaluate.</param>
    /// <returns><c>true</c> when the type implements <see cref="IEnumerable"/> and is not a string; otherwise <c>false</c>.</returns>
    public static bool IsCollection(this Type propertyType)
    {
        return propertyType != typeof(string) &&
               (typeof(IEnumerable).IsAssignableFrom(propertyType) ||
                propertyType.GetInterfaces().Any(i => i == typeof(IEnumerable)));
    }
}
