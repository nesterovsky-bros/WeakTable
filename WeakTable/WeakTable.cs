namespace NesterovskyBros.Utils
{
  using System;
  using System.Collections;
  using System.Collections.Concurrent;
  using System.Collections.Generic;
  using System.Linq;
  using System.Runtime.CompilerServices;
  using System.Threading;

  /// <summary>
  /// A weak table supporting non-identity equality.
  /// WeakTable is modeled after ConditionalWeakTable&lt;K, V&gt; class.
  /// </summary>
  /// <typeparam name="K">A key type.</typeparam>
  /// <typeparam name="V">A value type.</typeparam>
  public class WeakTable<K, V>: IEnumerable<KeyValuePair<K, V>>
    where K: class
  {
    /// <summary>
    /// Creates a WeakTable instance.
    /// </summary>
    /// <param name="comparer">
    /// Optional comparer instance. If no or null value is passed then 
    /// EqualityComparer&lt;K>.Default is used to match keys.
    /// </param>
    public WeakTable(IEqualityComparer<K> comparer = null)
    {
      self = new WeakReference<WeakTable<K, V>>(this, true);
      this.comparer = comparer ?? (comparer = EqualityComparer<K>.Default);
      values = new ConcurrentDictionary<object, WeakReference<State>>(
        new EqualityComparer { comparer = comparer });
    }

    /// <summary>
    /// A key enumerator.
    /// </summary>
    public IEnumerable<K> Keys
    {
      get { return States.Select(s => s.key); }
    }

    /// <summary>
    /// Value enumerator.
    /// </summary>
    public IEnumerable<V> Values
    {
      get { return States.Select(s => s.value); }
    }

    /// <summary>
    /// Gets a enumerator of entities contained in this weak table.
    /// </summary>
    /// <returns>
    /// A enumerator of entities contained in this weak table.
    /// </returns>
    public IEnumerator<KeyValuePair<K, V>> GetEnumerator()
    {
      return States.Select(s => new KeyValuePair<K, V>(s.key, s.value)).
        GetEnumerator();
    }

    /// <summary>
    /// Gets a enumerator of entities contained in this weak table.
    /// </summary>
    /// <returns>
    /// A enumerator of entities contained in this weak table.
    /// </returns>
    IEnumerator IEnumerable.GetEnumerator()
    {
      return GetEnumerator();
    }

    /// <summary>
    /// Adds an item to the weak table.
    /// </summary>
    /// <param name="key">A key.</param>
    /// <param name="value">A value.</param>
    /// <exception cref="ArgumentException">
    /// If item is already exists in the weak table.
    /// </exception>
    public void Add(K key, V value)
    {
      if (!TryAdd(key, value))
      {
        throw new ArgumentException("Duplicate argument.");
      }
    }

    /// <summary>
    /// Clears content of the weak table.
    /// </summary>
    public void Clear()
    {
      foreach(var state in States)
      {
        Remove(state.key);
      }
    }

    /// <summary>
    /// Gets or creates a value for a key.
    /// If value does not exist for a key then it's created using
    /// Activator.CreateInstance<V>().
    /// </summary>
    /// <param name="key">A key to get or create value for.</param>
    /// <returns>A value for the key.</returns>
    public V GetOrCreateValue(K key)
    {
      return GetValue(key, k => Activator.CreateInstance<V>());
    }

    /// <summary>
    /// Gets or creates a value for a key.
    /// </summary>
    /// <param name="key">A key to get or create value for.</param>
    /// <param name="createValue">A factory to create a value.</param>
    /// <returns>A value for the key.</returns>
    public V GetValue(K key, Func<K, V> createValue)
    {
      if (key == null)
      {
        throw new ArgumentException("key");
      }

      State state;

      GetOrAdd(key, createValue, out state);

      return state.value;
    }

    /// <summary>
    /// Removes a key from a weak table.
    /// </summary>
    /// <param name="key">A key to remove.</param>
    /// <returns>
    /// true if key is removed form the table, and false if no key was present.
    /// </returns>
    public bool Remove(K key)
    {
      if (key == null)
      {
        throw new ArgumentException("key");
      }

      WeakReference<State> stateRef;

      if (!values.TryGetValue(key, out stateRef))
      {
        return false;
      }

      var state = Get(stateRef);

      if (state != null)
      {
        Volatile.Write(ref state.initialized, 2);
        states.Remove(state.key);
        GC.SuppressFinalize(state);
      }

      var result = values.TryRemove(stateRef, out stateRef);

      GC.KeepAlive(state);

      return result;
    }

    /// <summary>
    /// Tries to add a key and value to the weak table.
    /// </summary>
    /// <param name="key">A key.</param>
    /// <param name="value">A value.</param>
    /// <returns>true if value is successfully added, and false otherwise.</returns>
    public bool TryAdd(K key, V value)
    {
      if (key == null)
      {
        throw new ArgumentException("key");
      }

      State state;

      return GetOrAdd(key, k => value, out state);
    }

    /// <summary>
    /// Tries to get a value for the key.
    /// </summary>
    /// <param name="key">A key.</param>
    /// <param name="value">An output value.</param>
    /// <returns>
    /// true if key is stored in the weak table, and false otherwise.
    /// </returns>
    public bool TryGetValue(K key, out V value)
    {
      WeakReference<State> stateRef;

      if (values.TryGetValue(key, out stateRef))
      {
        var state = Get(stateRef);

        if (state != null)
        {
          value = state.value;

          return true;
        }
      }

      value = default(V);

      return false;
    }

    /// <summary>
    /// Gets or adds a value.
    /// </summary>
    /// <param name="key">A key for the value.</param>
    /// <param name="createValue">A value factory.</param>
    /// <param name="value">Output value.</param>
    /// <returns>true if this is a new value, and false otherwise.</returns>
    private bool GetOrAdd(K key, Func<K, V> createValue, out State value)
    {
      var state = new State(self, key, comparer.GetHashCode(key));

      do
      {
        value = Get(values.GetOrAdd(
          state.self,
          k =>
          {
            state.value = createValue(key);

            return state.self;
          }));
      }
      while(value == null);

      if (state != value)
      {
        GC.SuppressFinalize(state);

        return false;
      }

      if (Interlocked.CompareExchange(ref state.initialized, 1, 0) == 0)
      {
        states.Add(key, state);

        if (Volatile.Read(ref state.initialized) == 2)
        {
          states.Remove(key);
          GC.SuppressFinalize(state);
        }
      }

      return true;
    }

    /// <summary>
    /// States enumerator.
    /// </summary>
    private IEnumerable<State> States
    {
      get { return values.Values.Select(v => Get(v)).Where(s => s != null); }
    }

    /// <summary>
    /// Gets a target of a weak reference.
    /// </summary>
    /// <typeparam name="R">A target type.</typeparam>
    /// <param name="value">A weak reference.</param>
    /// <returns>A target instance, or null.</returns>
    private static R Get<R>(WeakReference<R> value)
      where R: class
    {
      R target;

      value.TryGetTarget(out target);

      return target;
    }

    /// <summary>
    /// A state.
    /// </summary>
    private class State
    {
      /// <summary>
      /// Creates a state instance.
      /// </summary>
      /// <param name="weakTableRef">A WeakTable live weak reference.</param>
      /// <param name="key">A key instance.</param>
      /// <param name="hashCode">A hash code.</param>
      public State(WeakReference<WeakTable<K, V>> weakTableRef, K key, int hashCode)
      {
        this.weakTableRef = weakTableRef;
        this.self = new WeakReference<State>(this, true);
        this.key = key;
        this.hashCode = hashCode;
      }

      /// <summary>
      /// A finalizer.
      /// </summary>
      ~State()
      {
        var weakTable = Get(this.weakTableRef);

        if (weakTable != null)
        {
          WeakReference<State> state;

          weakTable.values.TryRemove(self, out state);
        }
      }

      /// <summary>
      /// A weak reference to the self.
      /// </summary>
      public readonly WeakReference<State> self;

      /// <summary>
      /// A weak table weak reference.
      /// </summary>
      public readonly WeakReference<WeakTable<K, V>> weakTableRef;

      /// <summary>
      /// A key hashcode.
      /// </summary>
      public readonly int hashCode;

      /// <summary>
      /// A key.
      /// </summary>
      public readonly K key;

      /// <summary>
      /// A value for the key.
      /// </summary>
      public V value;

      /// <summary>
      /// Indicator of initialized state.
      /// </summary>
      public int initialized;
    }

    /// <summary>
    /// Internal comparer.
    /// </summary>
    private class EqualityComparer: IEqualityComparer<object>
    {
      /// <summary>
      /// A comparer instance used to match keys.
      /// </summary>
      public IEqualityComparer<K> comparer;

      /// <summary>
      /// Compares two weak references.
      /// </summary>
      /// <param name="x">first reference.</param>
      /// <param name="y">second reference.</param>
      /// <returns>true if references are equal, and false other wise.</returns>
      public new bool Equals(object x, object y)
      {
        if (x == y)
        {
          return true;
        }

        var xinitialized = 0;
        K xkey;
        var xref = x as WeakReference<State>;

        if (xref != null)
        {
          var xstate = Get(xref);

          if (xstate == null)
          {
            return false;
          }

          xinitialized = xstate.initialized;
          xkey = xstate.key;
        }
        else
        { 
          xkey = x as K;
        }

        if (xkey == null)
        {
          return false;
        }

        K ykey;
        var yref = y as WeakReference<State>;

        if (yref != null)
        {
          var ystate = Get(yref);

          if (ystate == null)
          {
            return false;
          }

          if ((xinitialized != 0) && (ystate.initialized != 0))
          {
            return false;
          }

          ykey = ystate.key;
        }
        else
        {
          ykey = y as K;
        }

        if (ykey == null)
        {
          return false;
        }

        return comparer.Equals(xkey, ykey);
      }

      /// <summary>
      /// Gets a hash code for the weak reference.
      /// </summary>
      /// <param name="key">A key.</param>
      /// <returns>A hash code value.</returns>
      public int GetHashCode(object obj)
      {
        var weakRef = obj as WeakReference<State>;

        if (weakRef != null)
        {
          var state = Get(weakRef);

          return state == null ? 0 : state.hashCode;
        }

        return obj == null ? 0 : obj.GetHashCode();
      }
    }

    // Theory:
    // 1. Keys in "values" are the same instances as values.
    // 2. The type of key in "values" is declared as object.
    //    This is to allow search keys of type K using custom comparer.
    // 3. WeakReference<State> instances track resurection. 
    //    This means that they keep object references during finalization.
    // 4. State removes its reference from "values" during finalization.
    // 5. States are collected after their K instances, as they are bound
    //    using "states" weak table.
    // 6. Both keys and values are not stongly kept by this instance.
    //    This means that a value can strongly refer to a key, which won't 
    //    keep them from GC, if there are no more strong references to the key.

    /// <summary>
    /// A comparer instance used to match keys.
    /// </summary>
    private readonly IEqualityComparer<K> comparer;

    /// <summary>
    /// A dictionary storage.
    /// </summary>
    private readonly ConcurrentDictionary<object, WeakReference<State>> values;

    /// <summary>
    /// A list of states.
    /// </summary>
    private readonly ConditionalWeakTable<K, State> states = 
      new ConditionalWeakTable<K, State>();

    /// <summary>
    /// A weak reference to the self.
    /// </summary>
    public readonly WeakReference<WeakTable<K, V>> self;
  }
}
