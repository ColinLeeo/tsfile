/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.tsfile.block.column;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.TsPrimitiveType;

import java.util.Arrays;

public interface Column {

  /** Get the data type. */
  TSDataType getDataType();

  /** Get the encoding for this column. */
  ColumnEncoding getEncoding();

  /** Gets a boolean at {@code position}. */
  default boolean getBoolean(int position) {
    throw new UnsupportedOperationException(getClass().getName());
  }

  /** Gets a little endian int at {@code position}. */
  default int getInt(int position) {
    throw new UnsupportedOperationException(getClass().getName());
  }

  /** Gets a little endian long at {@code position}. */
  default long getLong(int position) {
    throw new UnsupportedOperationException(getClass().getName());
  }

  /** Gets a float at {@code position}. */
  default float getFloat(int position) {
    throw new UnsupportedOperationException(getClass().getName());
  }

  /** Gets a double at {@code position}. */
  default double getDouble(int position) {
    throw new UnsupportedOperationException(getClass().getName());
  }

  /** Gets a Binary at {@code position}. */
  default Binary getBinary(int position) {
    throw new UnsupportedOperationException(getClass().getName());
  }

  /** Gets an Object at {@code position}. */
  default Object getObject(int position) {
    throw new UnsupportedOperationException(getClass().getName());
  }

  /** Gets the boolean array. */
  default boolean[] getBooleans() {
    throw new UnsupportedOperationException(getClass().getName());
  }

  /** Gets the little endian int array. */
  default int[] getInts() {
    throw new UnsupportedOperationException(getClass().getName());
  }

  /** Gets the little endian long array. */
  default long[] getLongs() {
    throw new UnsupportedOperationException(getClass().getName());
  }

  /** Gets the float array. */
  default float[] getFloats() {
    throw new UnsupportedOperationException(getClass().getName());
  }

  /** Gets the double array. */
  default double[] getDoubles() {
    throw new UnsupportedOperationException(getClass().getName());
  }

  /** Gets the Binary list. */
  default Binary[] getBinaries() {
    throw new UnsupportedOperationException(getClass().getName());
  }

  /** Gets the Object array. */
  default Object[] getObjects() {
    throw new UnsupportedOperationException(getClass().getName());
  }

  /** Gets a TsPrimitiveType at {@code position}. */
  default TsPrimitiveType getTsPrimitiveType(int position) {
    throw new UnsupportedOperationException(getClass().getName());
  }

  /**
   * Is it possible the column may have a null value? If false, the column cannot contain a null,
   * but if true, the column may or may not have a null.
   */
  boolean mayHaveNull();

  /**
   * Is the specified position null?
   *
   * @throws IllegalArgumentException if this position is not valid. The method may return false
   *     without throwing exception when there are no nulls in the block, even if the position is
   *     invalid
   */
  boolean isNull(int position);

  /** Returns the array to determine whether each position of the column is null or not. */
  boolean[] isNull();

  /**
   * Set the given range as null.
   *
   * @param start start position (inclusive)
   * @param end end position (exclusive)
   */
  void setNull(int start, int end);

  /** Returns the number of positions in this block. */
  int getPositionCount();

  /**
   * Returns the retained size of this column in memory, including over-allocations. This method is
   * called from the inner most execution loop and must be fast.
   */
  long getRetainedSizeInBytes();

  /**
   * Returns the size of this Column as if it was compacted, ignoring any over-allocations and any
   * unloaded nested Columns. For example, in dictionary blocks, this only counts each dictionary
   * entry once, rather than each time a value is referenced.
   */
  long getSizeInBytes();

  /**
   * Returns a column starting at the specified position and extends for the specified length. The
   * specified region must be entirely contained within this column.
   *
   * <p>The region can be a view over this column. If this column is released, the region column may
   * also be released. If the region column is released, this block may also be released.
   */
  Column getRegion(int positionOffset, int length);

  /**
   * Returns a column starting at the specified position and extends for the specified length. The
   * specified region must be entirely contained within this column.
   *
   * <p>The region is a copy over this column. If this column is released, the region column won't
   * be affected.
   */
  Column getRegionCopy(int positionOffset, int length);

  /**
   * This method will create a temporary view of origin column, which will reuse the array of column
   * but with different array offset.
   */
  Column subColumn(int fromIndex);

  /** This method will create a copy of origin column with different array offset. */
  Column subColumnCopy(int fromIndex);

  /**
   * Create a new colum from the current colum by keeping the same elements only with respect to
   * {@code positions} that starts at {@code offset} and has length of {@code length}. The
   * implementation may return a view over the data in this colum or may return a copy, and the
   * implementation is allowed to retain the positions array for use in the view.
   */
  Column getPositions(int[] positions, int offset, int length);

  /**
   * Returns a column containing the specified positions. Positions to copy are stored in a subarray
   * within {@code positions} array that starts at {@code offset} and has length of {@code length}.
   * All specified positions must be valid for this block.
   *
   * <p>The returned column must be a compact representation of the original column.
   */
  Column copyPositions(int[] positions, int offset, int length);

  /** reverse the column */
  void reverse();

  int getInstanceSize();

  void setPositionCount(int count);

  default void reset() {
    setPositionCount(0);
    final boolean[] isNulls = isNull();
    if (isNulls != null) {
      Arrays.fill(isNulls, false);
    }
  }
}
