package org.apache.hadoop.fs.azurenative;

import java.lang.reflect.Array;
import java.util.Collection;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Queue;

/**
 * @param <E> : Ring buffer contains buffers this data type.
 */
public class AzureRingBuffer<E> implements Queue<E> {
  private boolean m_autoGrow;
  private E[] m_data;
  private int m_head;
  private int m_size;
  private int m_tail;

  static private int s_DEFAULT_CAPACITY = 8; // Default capacity of ring buffer.

  /**
   * Default constructor creating a ring buffer with a default capacity. The
   * ring buffer does not auto-grow.
   */
  public AzureRingBuffer() {
    this(s_DEFAULT_CAPACITY, false);
  }

  /**
   * Creates a ring buffer with a non-default capacity. The ring buffer does not
   * auto-grow.
   * 
   * @param aCapacity Non-default capacity of the ring buffer.
   */
  public AzureRingBuffer(int aCapacity) {
    this(aCapacity, false);
  }

  /**
   * Creates a ring buffer with a non-default capacity. The ring buffer is
   * allowed to auto-grow.
   * 
   * @param aCapacity Non-default capacity of the ring buffer.
   * @param canAutoGrow Set to true if the ring buffer is allowed to grow,
   *          otherwise it is false.
   */
  @SuppressWarnings("unchecked")
  public AzureRingBuffer(int aCapacity, boolean canAutoGrow) {
    m_data = (E[]) new Object[aCapacity];
    m_head = 0;
    m_tail = 0;
    m_autoGrow = canAutoGrow;
  }

  /**
   * Add a new element to the ring buffer.
   * 
   * @param anElement Add this data element to the ring buffer.
   * @return boolean True if the element was successfully added to the ring
   *         buffer.
   */
  @Override
  public boolean add(E anElement) {
    if (!offer(anElement)) {
      throw new IllegalStateException();
    }

    // Addition of the new element to the ring buffer was successful.
    //
    return true;
  }

  /**
   * Add a collection of elements to the ring buffer.
   * 
   * @param aCollection A collection of data elements to add to the ring buffer.
   * @return boolean True if the entire collection is added to the ring buffer.
   */
  @Override
  public boolean addAll(Collection<? extends E> aCollection) {
    // Iterate through the collection adding each element to the ring buffer.
    //
    Iterator<? extends E> iter = aCollection.iterator();
    while (iter.hasNext()) {
      // Add current element to the ring buffer.
      //
      if (!add(iter.next())) {
        // Could not successfully add an element to the ring buffer.
        //
        return false;
      }
    }

    // Successfully added elements to the ring buffer. Adding empty collections
    // are
    // is also considered successful.
    //
    return true;
  }

  /**
   * Query the current capacity of the ring buffer.
   * 
   * @return Current capacity of the ring buffer.
   */
  public int capacity() {
    return m_data.length;
  }

  /**
   * Resets the ring buffer.
   */
  @SuppressWarnings("unchecked")
  @Override
  public void clear() {
    m_data = (E[]) new Object[m_data.length];
    m_head = 0;
    m_tail = 0;
  }

  /**
   * Check if an element is in the ring buffer.
   * 
   * @param anObject Check for the existence of this object in the ring buffer.
   * @return boolean True if the element is found in the ring buffer, and false
   *         otherwise.
   */
  @Override
  public boolean contains(Object anObject) {
    // Iterate through the ring buffer attempting to fine the element.
    //
    Iterator<? extends E> iter = iterator();
    while (iter.hasNext()) {
      E currentElement = iter.next();
      if (null == anObject ? null == currentElement : anObject
          .equals(currentElement)) {
        return true;
      }
    }
    // The object was not found in the ring buffer.
    //
    return false;
  }

  /**
   * Check if all elements of a collection are contained in the ring buffer.
   * 
   * @param aCollection Collection of objects to be checked.
   * @return boolean True if all objects in the collection are in the ring
   *         buffer.
   */
  @Override
  public boolean containsAll(Collection<?> aCollection) {
    Iterator<?> iter = aCollection.iterator();
    while (iter.hasNext()) {
      // Check if the current element in the collection is in the ring buffer.
      //
      if (!contains(iter.next())) {
        // Current element is not in the collection.
        //
        return false;
      }
    }

    // At this point all elements of the collection are part of the ring buffer.
    //
    return true;
  }

  /**
   * Query the current head of the ring buffer.
   * 
   * @return E Current head element of the ring buffer.
   */
  @Override
  public E element() {
    // Return the current head element if the ring buffer contains any elements
    // at all.
    //
    if (0 < m_size) {
      // Peek at the current head element of the ring buffer without popping it
      // and return it to the caller.
      //
      return peek();
    }

    // The ring buffer is empty, throw an exception indicating no such element
    // exists.
    //
    throw new NoSuchElementException();
  }

  /* *
   * Query if the ring buffer auto-grows.
   * 
   * @return boolean True if the ring buffer auto-grows on reaching its
   * capacity.
   */
  public boolean canAutoGrow() {
    // Return value of member variable with the autogrowth attribute.
    //
    return m_autoGrow;
  }

  /**
   * Query if the ring buffer is empty.
   * 
   * @return boolean True if the ring buffer contains no elements.
   */
  @Override
  public boolean isEmpty() {
    // The ring buffer is empty if the number of elements it contains is zero.
    //
    return 0 == m_size;
  }

  /**
   * Iterator for walking through the ring buffer collection.
   * 
   * @return Iterator<E> Iterator over elements in the ring buffer.
   */
  @Override
  public Iterator<E> iterator() {
    return new Iterator<E>() {
      @SuppressWarnings("synthetic-access")
      private int _index = m_head;

      @SuppressWarnings("synthetic-access")
      @Override
      public boolean hasNext() {
        return _index != m_tail;
      }

      @SuppressWarnings("synthetic-access")
      @Override
      public E next() {
        if (!hasNext()) {
          // No elements in the ring buffer.
          //
          throw new NoSuchElementException();
        }

        E rawObject = m_data[_index];
        ++_index;
        if (m_data.length == _index) {
          _index = 0;
        }
        return rawObject;
      }

      @Override
      public void remove() {
        // TODO Remove operations are not yet supported.
        throw new UnsupportedOperationException();
      }
    };
  }

  /**
   * Inserts the specified element into this ring buffer, if possible.
   * 
   * @param anElement The element to be inserted into the ring buffer.
   * @return boolean True when the element is successfully inserted into the
   *         ring buffer.
   */
  @SuppressWarnings("unchecked")
  @Override
  public boolean offer(E anElement) {
    if (m_data.length == m_size) {
      // The ring buffer is full, see if it could be automatically grown.
      //
      if (m_autoGrow) {
        // Grow the ring buffer by reallocating with double the current ring
        // buffer
        // capacity.
        //
        E[] temp = (E[]) new Object[m_data.length * 2];

        // Copy the elements of the current ring buffer into the new one.
        //
        Iterator<? extends E> iter = iterator();
        for (int i = 0; i < m_size; ++i) {
          // Assertion: The there should be an element present at this position
          // of the ring buffer.
          //
          if (!iter.hasNext()) {
            throw new AssertionError(
                "Ring buffer element expected but not found.");
          }

          temp[i] = iter.next();
        }

        // Reset the member variables.
        //
        m_head = 0;
        m_tail = m_size;
        m_data = temp;
      } else {
        // The ring buffer is full and it cannot be automatically grown.
        //
        return false;
      }
    }

    // Insert the incoming element at the tail of the ring buffer.
    //
    m_data[m_tail] = anElement;
    ++m_tail;
    ++m_size;
    if (m_data.length == m_tail) {
      // Wrap the tail since it is at the maximum length of the ring buffer.
      //
      m_tail = 0;
    }

    // The element was successfully inserted into the ring buffer.
    //
    return true;
  }

  /**
   * Look get the head of the ring buffer without removing it.
   * 
   * @return E The head of the ring buffer.
   */
  @Override
  public E peek() {
    if (0 == m_size) {
      // The ring buffer is empty, return a null element.
      //
      return null;
    }

    // Assertion: Ring buffer is not empty, so the head should be non-null.
    //
    if (m_data[m_head] == null) {
      throw new AssertionError(
          "Ring buffer not empty but head does not exist.");
    }

    // Return the head of the ring buffer to the caller.
    //
    return m_data[m_head];
  }

  /**
   * Retrieves and removes the head of this queue, or null if this queue is
   * empty.
   * 
   * @return E The head of the ring buffer.
   */
  @Override
  public E poll() {
    E headElement = peek();
    if (null != headElement) {
      // Assertion: The ring buffer is not empty so its size should be greater
      // than 0.
      //
      if (m_size == 0) {
        throw new AssertionError("Non-empty ring buffer has zero size.");
      }

      // The ring buffer is not empty, remove the head element.
      //
      m_data[m_head] = null;
      ++m_head;
      --m_size;
      if (m_data.length == m_head) {
        // Wrap the head since the head index is at the capacity of the ring
        // buffer.
        //
        m_head = 0;
      }
    }

    // Return to caller with the head element--may be null.
    //
    return headElement;
  }

  /**
   * Retrieves and removes the head of this queue.
   * 
   * @return E The head of the ring buffer.
   */
  @Override
  public E remove() {
    E headElement = element();
    m_data[m_head] = null;
    ++m_head;
    --m_size;
    if (m_data.length == m_head) {
      // Wrap the head index since it is at the capacity of the ring buffer.
      //
      m_head = 0;
    }
    return headElement;
  }

  /**
   * Removes a single instance of the specified element from this collection, if
   * it is present.
   * 
   * @return boolean True if object is successfully removed from the ring
   *         buffer.
   */
  @Override
  public boolean remove(Object anObject) {
    // Unsupported: This collection method is not supported by the ring buffer.
    //
    throw new UnsupportedOperationException();
  }

  /**
   * Removes all this collection's elements that are also contained in the
   * specified collection (optional operation). After this call returns, this
   * collection will contain no elements in common with the specified
   * collection.
   * 
   * @return boolean True if object is successfully removed from the ring
   *         buffer.
   */
  @Override
  public boolean removeAll(Collection<?> aCollection) {
    // Unsupported: This collection method is not supported by the ring buffer.
    //
    throw new UnsupportedOperationException();
  }

  /**
   * Retains only the elements in this collection that are contained in the
   * specified collection (optional operation). In other words, removes from
   * this collection all of its elements that are not contained in the specified
   * collection.
   * 
   * @return boolean True if object is successfully removed from the ring
   *         buffer.
   */
  @Override
  public boolean retainAll(Collection<?> aCollection) {
    // Unsupported: This collection method is not supported by the ring buffer.
    //
    throw new UnsupportedOperationException();
  }

  /**
   * Dynamically set whether or not the ring buffer automatically grows when it
   * reaches capacity.
   * 
   * @param canAutoGrow Has a value of true for auto-growth and a value of false
   *          for fixed capacity.
   */
  public void setAutoGrowth(boolean canAutoGrow) {
    m_autoGrow = canAutoGrow;
  }

  /**
   * Query the number of elements in the ring buffer.
   * 
   * @return int Size of the ring buffer.
   */
  @Override
  public int size() {
    // Simply return the value of the size member variable.
    //
    return m_size;
  }

  /**
   * Returns an array containing all of the elements in this ring buffer. Since
   * the ring buffer makes any guarantees as to what order its elements are
   * returned by its iterator, this method must return the elements in the same
   * order.
   * 
   * @return Object [] Array of all elements in the ring buffer.
   */
  @Override
  public Object[] toArray() {
    Object[] elementArray = new Object[m_size];

    // Iterate through the ring buffer elements and capture them in the array.
    //
    Iterator<? extends E> iter = iterator();
    for (int i = 0; i < m_size; ++i) {
      // Capture the ring buffer elements into the array.
      //
      elementArray[i] = iter.next();
    }

    // Return to caller with the array.
    //
    return elementArray;
  }

  /**
   * Returns an array containing all of the elements in this ring buffer; the
   * runtime type of the array returned is that of the specified array. If the
   * ring buffer fits in the specified array, it is returned therein. Otherwise,
   * a new array is allocated with the runtime type of the specified array and
   * the size of this collection.
   * 
   * @return Object [] Array of all elements in the ring buffer.
   */
  @SuppressWarnings("unchecked")
  @Override
  public <T> T[] toArray(T[] anArray) {
    if (anArray.length < m_size) {
      // The incoming array does not accommodate the number of elements in the
      // ring buffer. Reflect on array type to create a new array type that is
      // the
      // correct size.
      //
      anArray = (T[]) Array.newInstance(anArray.getClass().getComponentType(),
          m_size);
    }

    // Iterate through the ring buffer elements and capture them in the array.
    //
    Iterator<? extends E> iter = iterator();
    for (int i = 0; i < m_size; ++i) {
      // Capture the ring buffer elements into the array.
      //
      anArray[i] = (T) iter.next();
    }

    // Return the array.
    //
    return anArray;
  }
}
