using System.Collections.Concurrent;
using System.Runtime;

namespace NesterovskyBros.Utils;

/// <summary>
/// <para>
/// A store to keep objects by multiple weak keys.
/// Value is kept only if all keys are alive, otherwise value is reclaimed.
/// </para>
/// <para>
/// <b>Note:</b>  do not have strong references to all keys in an instance
/// of T kept in the store.
/// </para>
/// <para><b>Note:</b> class is thread safe.</para>
/// </summary>
public class WeakStore
{
  /// <summary>
  /// Gets an instance by keys.
  /// </summary>
  /// <typeparam name="T">An instance type to store.</typeparam>
  /// <param name="keys">An array of keys.</param>
  /// <returns>An instance, if available.</returns>
  public T? Get<T>(params object[] keys)
    where T : class =>
    store.TryGetValue(new(keys), out var next) ?
      (T?)next.value.Target : null;

  /// <summary>
  /// <para>Gets or creates an instance, if it was not in the store, by keys.</para>
  /// <para><b>Note:</b> If create is called then it runs within lock. 
  /// Its computation should be short and simple, and must not attempt to
  /// update any other mappings of this store.</para>
  /// </summary>
  /// <typeparam name="T">An instance type to store.</typeparam>
  /// <param name="create">An instance factory.</param>
  /// <param name="keys">An array of keys.</param>
  /// <returns>An instance.</returns>
  public T? GetOrCreate<T>(Func<T> create, params object[] keys)
    where T : class
  {
    var key = new Key(keys);

    var next = store.GetOrAdd(
      key,
      k =>
      {
        k.value = new(keys[0], create());
        k.store = this;
        k.MakeHandles();

        return k;
      });

    if(key != next)
    {
      key.store = null;
      key.Dispose();
    }

    return (T?)next.value.Dependent;
  }

  /// <summary>
  /// Sets or removes an instance by keys.
  /// </summary>
  /// <typeparam name="T">An instance type to store.</typeparam>
  /// <param name="value">A value to set, or <c>null</c> to remove.</param>
  /// <param name="keys">An array of keys.</param>
  /// <returns>A replaced instance, if any.</returns>
  public T? Set<T>(T? value, params object[] keys)
    where T : class
  {
    T? prevValue = default;
    var key = new Key(keys);

    if(value == null)
    {
      if(store.TryRemove(key, out var prev))
      {
        prevValue = (T?)prev.value.Dependent;
        prev.store = null;
        prev.Dispose();
      }
    }
    else
    {
      var next = store.AddOrUpdate(
        key,
        k =>
        {
          k.value = new(keys[0], value);
          k.store = this;
          k.MakeHandles();

          return k;
        },
        (k, v) =>
        {
          prevValue = (T?)v.value.Dependent;
          v.value = new(keys[0], value);

          return v;
        });

      if(next != key)
      {
        key.store = null;
        key.Dispose();
      }

      if((prevValue != null) && (prevValue != value))
      {
        Release(prevValue);
      }
    }

    return prevValue;
  }

  /// <summary>
  /// Called when an instance is released.
  /// </summary>
  /// <param name="value">An instance to release.</param>
  protected virtual void Release(object value)
  {
  }

  private class Key : IDisposable
  {
    public Key(object[] keys)
    {
      if((keys == null) || (keys.Length == 0))
      {
        throw new ArgumentException("Empty keys.", nameof(keys));
      }

      var hashCode = 0;

      for(var i = 0; i < keys.Length; ++i)
      {
        hashCode ^= keys[i].GetHashCode();
      }

      this.keys = keys;
      this.hashCode = hashCode;
    }

    ~Key()
    {
      Dispose();
    }

    public void Dispose()
    {
      var store = Interlocked.Exchange(ref this.store, null);

      if(store != null)
      {
        store.store.TryRemove(this, out var _);

        var value = this.value.Dependent;

        if(value != null)
        {
          this.value.Dependent = new();
          store.Release(value);
        }
      }

      keys = null;

      var handles = Interlocked.Exchange(ref this.handles, null);

      if(handles != null)
      {
        GC.SuppressFinalize(this);

        for(var i = 0; i < handles.Length; ++i)
        {
          handles[i].Dispose();
        }
      }
    }

    public override int GetHashCode() => hashCode;

    public override bool Equals(object? obj)
    {
      if(this == obj)
      {
        return true;
      }

      var that = (Key)obj!;
      var length = keys?.Length ?? handles?.Length ?? 0;
      var thatLength = that.keys?.Length ?? that.handles?.Length ?? 0;

      if(length != thatLength)
      {
        return false;
      }

      for(var i = 0; i < length; ++i)
      {
        var value = keys?[i] ?? handles?[i].Target;
        var thatValue = that.keys?[i] ?? that.handles?[i].Target;

        if((value != thatValue) || (value == null))
        {
          return false;
        }
      }

      return true;
    }

    public void MakeHandles()
    {
      var keys = Interlocked.Exchange(ref this.keys, null);

      if(keys != null)
      {
        var handles = new DependentHandle[keys.Length];

        this.handles = handles;

        for(var i = 0; i < keys.Length; ++i)
        {
          handles[i] = new DependentHandle(keys[i], new Notifier { key = this });
        }
      }
    }

    public readonly int hashCode;
    public WeakStore? store;
    public object[]? keys;
    public DependentHandle[]? handles;
    public DependentHandle value;
  }

  private class Notifier
  {
    public Key? key;

    ~Notifier()
    {
      key?.Dispose();
    }
  }

  /// <summary>
  /// A store itself.
  /// </summary>
  private readonly ConcurrentDictionary<Key, Key> store = new();
}
