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

package org.apache.tsfile.utils;

import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.read.reader.TsFileInput;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import static org.apache.tsfile.utils.ReadWriteIOUtils.ClassSerializeId.BINARY;
import static org.apache.tsfile.utils.ReadWriteIOUtils.ClassSerializeId.BOOLEAN;
import static org.apache.tsfile.utils.ReadWriteIOUtils.ClassSerializeId.DOUBLE;
import static org.apache.tsfile.utils.ReadWriteIOUtils.ClassSerializeId.FLOAT;
import static org.apache.tsfile.utils.ReadWriteIOUtils.ClassSerializeId.INTEGER;
import static org.apache.tsfile.utils.ReadWriteIOUtils.ClassSerializeId.LONG;
import static org.apache.tsfile.utils.ReadWriteIOUtils.ClassSerializeId.NULL;
import static org.apache.tsfile.utils.ReadWriteIOUtils.ClassSerializeId.STRING;

/**
 * ConverterUtils is a utility class. It provides conversion between normal datatype and byte array.
 */
public class ReadWriteIOUtils {

  public static final int BOOLEAN_LEN = 1;
  public static final int SHORT_LEN = 2;
  public static final int INT_LEN = 4;
  public static final int LONG_LEN = 8;
  public static final int DOUBLE_LEN = 8;
  public static final int FLOAT_LEN = 4;
  public static final float BIT_LEN = 0.125F;

  public static final int NO_BYTE_TO_READ = -1;

  private static final byte[] magicStringBytes;

  private static final String RETURN_ERROR = "Intend to read %d bytes but %d are actually returned";

  static {
    magicStringBytes = BytesUtils.stringToBytes(TSFileConfig.MAGIC_STRING);
  }

  private ReadWriteIOUtils() {}

  /** read a bool from inputStream. */
  public static boolean readBool(InputStream inputStream) throws IOException {
    int flag = inputStream.read();
    return flag == 1;
  }

  // used by generated code
  @SuppressWarnings("unused")
  public static boolean readBoolean(InputStream inputStream) throws IOException {
    int flag = inputStream.read();
    return flag == 1;
  }

  /** read a bool from byteBuffer. */
  public static boolean readBool(ByteBuffer buffer) {
    byte a = buffer.get();
    return a == 1;
  }

  /** read a bool from byteBuffer. */
  public static boolean readBoolean(ByteBuffer buffer) {
    return readBool(buffer);
  }

  /** read a Boolean from byteBuffer. */
  public static Boolean readBoolObject(ByteBuffer buffer) {
    byte a = buffer.get();
    if (a == 1) {
      return true;
    } else if (a == 0) {
      return false;
    }
    return null;
  }

  /** Read a Boolean from byteBuffer. */
  public static Boolean readBoolObject(InputStream inputStream) throws IOException {
    int flag = inputStream.read();
    if (flag == 1) {
      return true;
    } else if (flag == 0) {
      return false;
    }
    return null;
  }

  /** Read a byte from byteBuffer. */
  public static byte readByte(ByteBuffer buffer) {
    return buffer.get();
  }

  /**
   * Read bytes array in given size
   *
   * @param buffer buffer
   * @param size size
   * @return bytes array
   */
  public static byte[] readBytes(ByteBuffer buffer, int size) {
    byte[] res = new byte[size];
    buffer.get(res);
    return res;
  }

  /** read a bool from byteBuffer. */
  public static boolean readIsNull(InputStream inputStream) throws IOException {
    return readBool(inputStream);
  }

  /** read a bool from byteBuffer. */
  public static boolean readIsNull(ByteBuffer buffer) {
    return readBool(buffer);
  }

  public static int write(Map<String, String> map, OutputStream stream) throws IOException {
    if (map == null) {
      return write(NO_BYTE_TO_READ, stream);
    }

    int length = 0;
    length += write(map.size(), stream);
    for (Entry<String, String> entry : map.entrySet()) {
      length += write(entry.getKey(), stream);
      length += write(entry.getValue(), stream);
    }
    return length;
  }

  public static void write(List<Map<String, String>> maps, OutputStream stream) throws IOException {
    for (Map<String, String> map : maps) {
      write(map, stream);
    }
  }

  public static int write(Map<String, String> map, ByteBuffer buffer) {
    if (map == null) {
      return write(NO_BYTE_TO_READ, buffer);
    }

    int length = 0;
    byte[] bytes;
    buffer.putInt(map.size());
    length += 4;
    for (Entry<String, String> entry : map.entrySet()) {
      if (entry.getKey() == null) {
        buffer.putInt(-1);
      } else {
        bytes = entry.getKey().getBytes();
        buffer.putInt(bytes.length);
        buffer.put(bytes);
        length += bytes.length;
      }
      length += 4;
      if (entry.getValue() == null) {
        buffer.putInt(-1);
      } else {
        bytes = entry.getValue().getBytes();
        buffer.putInt(bytes.length);
        buffer.put(bytes);
        length += bytes.length;
      }
      length += 4;
    }
    return length;
  }

  public static void write(List<Map<String, String>> maps, ByteBuffer buffer) {
    for (Map<String, String> map : maps) {
      write(map, buffer);
    }
  }

  /**
   * write a int value to outputStream according to flag. If flag is true, write 1, else write 0.
   */
  public static int write(Boolean flag, OutputStream outputStream) throws IOException {
    if (flag == null) {
      outputStream.write(-1);
    } else if (Boolean.TRUE.equals(flag)) {
      outputStream.write(1);
    } else {
      outputStream.write(0);
    }
    return BOOLEAN_LEN;
  }

  /** write a byte to byteBuffer according to flag. If flag is true, write 1, else write 0. */
  public static int write(Boolean flag, ByteBuffer buffer) {
    byte a;
    if (flag == null) {
      a = -1;
    } else if (Boolean.TRUE.equals(flag)) {
      a = 1;
    } else {
      a = 0;
    }

    buffer.put(a);
    return BOOLEAN_LEN;
  }

  /**
   * write a byte n.
   *
   * @return The number of bytes used to represent a {@code byte} value in two's complement binary
   *     form.
   */
  public static int write(byte n, OutputStream outputStream) throws IOException {
    outputStream.write(n);
    return Byte.BYTES;
  }

  /**
   * write a short n.
   *
   * @return The number of bytes used to represent n.
   */
  public static int write(short n, OutputStream outputStream) throws IOException {
    byte[] bytes = BytesUtils.shortToBytes(n);
    outputStream.write(bytes);
    return bytes.length;
  }

  /**
   * write a byte n to byteBuffer.
   *
   * @return The number of bytes used to represent a {@code byte} value in two's complement binary
   *     form.
   */
  public static int write(byte n, ByteBuffer buffer) {
    buffer.put(n);
    return Byte.BYTES;
  }

  /**
   * write a short n to byteBuffer.
   *
   * @return The number of bytes used to represent n.
   */
  public static int write(short n, ByteBuffer buffer) {
    buffer.putShort(n);
    return SHORT_LEN;
  }

  /**
   * write a short n to byteBuffer.
   *
   * @return The number of bytes used to represent n.
   */
  public static int write(Binary n, ByteBuffer buffer) {
    buffer.putInt(n.getLength());
    buffer.put(n.getValues());
    return INT_LEN + n.getLength();
  }

  /**
   * write a int n to outputStream.
   *
   * @return The number of bytes used to represent n.
   */
  public static int write(int n, OutputStream outputStream) throws IOException {
    byte[] bytes = BytesUtils.intToBytes(n);
    outputStream.write(bytes);
    return INT_LEN;
  }

  /** write the size (int) of the binary and then the bytes in binary */
  public static int write(Binary binary, OutputStream outputStream) throws IOException {
    byte[] size = BytesUtils.intToBytes(binary.getValues().length);
    outputStream.write(size);
    outputStream.write(binary.getValues());
    return size.length + binary.getValues().length;
  }

  /**
   * write a int n to byteBuffer.
   *
   * @return The number of bytes used to represent n.
   */
  public static int write(int n, ByteBuffer buffer) {
    buffer.putInt(n);
    return INT_LEN;
  }

  /**
   * Write a float n to outputStream.
   *
   * @return The number of bytes used to represent n.
   */
  public static int write(float n, OutputStream outputStream) throws IOException {
    byte[] bytes = BytesUtils.floatToBytes(n);
    outputStream.write(bytes);
    return FLOAT_LEN;
  }

  /**
   * Write a double n to outputStream.
   *
   * @return The number of bytes used to represent n.
   */
  public static int write(double n, OutputStream outputStream) throws IOException {
    byte[] bytes = BytesUtils.doubleToBytes(n);
    outputStream.write(bytes);
    return DOUBLE_LEN;
  }

  /**
   * write a long n to outputStream.
   *
   * @return The number of bytes used to represent n.
   */
  public static int write(long n, OutputStream outputStream) throws IOException {
    byte[] bytes = BytesUtils.longToBytes(n);
    outputStream.write(bytes);
    return LONG_LEN;
  }

  /** write a long n to byteBuffer. */
  public static int write(long n, ByteBuffer buffer) {
    buffer.putLong(n);
    return LONG_LEN;
  }

  /** write a float n to byteBuffer. */
  public static int write(float n, ByteBuffer buffer) {
    buffer.putFloat(n);
    return FLOAT_LEN;
  }

  /** write a double n to byteBuffer. */
  public static int write(double n, ByteBuffer buffer) {
    buffer.putDouble(n);
    return DOUBLE_LEN;
  }

  /**
   * Write string to outputStream.
   *
   * @return the length of string represented by byte[].
   */
  public static int write(String s, OutputStream outputStream) throws IOException {
    int len = 0;
    if (s == null) {
      len += write(NO_BYTE_TO_READ, outputStream);
      return len;
    }

    byte[] bytes = s.getBytes(TSFileConfig.STRING_CHARSET);
    len += write(bytes.length, outputStream);
    outputStream.write(bytes);
    len += bytes.length;
    return len;
  }

  /**
   * Write string to outputStream.
   *
   * @return the length of string represented by byte[].
   */
  public static int writeVar(String s, OutputStream outputStream) throws IOException {
    int len = 0;
    if (s == null) {
      len += ReadWriteForEncodingUtils.writeVarInt(NO_BYTE_TO_READ, outputStream);
      return len;
    }

    byte[] bytes = s.getBytes(TSFileConfig.STRING_CHARSET);
    len += ReadWriteForEncodingUtils.writeVarInt(bytes.length, outputStream);
    outputStream.write(bytes);
    len += bytes.length;
    return len;
  }

  /**
   * write string to byteBuffer.
   *
   * @return the length of string represented by byte[].
   */
  public static int write(String s, ByteBuffer buffer) {
    if (s == null) {
      return write(NO_BYTE_TO_READ, buffer);
    }
    int len = 0;
    byte[] bytes = s.getBytes(TSFileConfig.STRING_CHARSET);
    len += write(bytes.length, buffer);
    buffer.put(bytes);
    len += bytes.length;
    return len;
  }

  public static int writeVar(String s, ByteBuffer buffer) {
    if (s == null) {
      return ReadWriteForEncodingUtils.writeVarInt(NO_BYTE_TO_READ, buffer);
    }
    int len = 0;
    byte[] bytes = s.getBytes(TSFileConfig.STRING_CHARSET);
    len += ReadWriteForEncodingUtils.writeVarInt(bytes.length, buffer);
    buffer.put(bytes);
    len += bytes.length;
    return len;
  }

  /** write byteBuffer.capacity and byteBuffer.array to outputStream. */
  public static int write(ByteBuffer byteBuffer, OutputStream outputStream) throws IOException {
    int len = 0;
    len += write(byteBuffer.capacity(), outputStream);
    byte[] bytes = byteBuffer.array();
    outputStream.write(bytes);
    len += bytes.length;
    return len;
  }

  public static void writeWithoutSize(
      ByteBuffer byteBuffer, int offset, int len, OutputStream outputStream) throws IOException {
    byte[] bytes = byteBuffer.array();
    outputStream.write(bytes, offset, len);
  }

  /** write byteBuffer.capacity and byteBuffer.array to byteBuffer. */
  public static int write(ByteBuffer byteBuffer, ByteBuffer buffer) {
    int len = 0;
    len += write(byteBuffer.capacity(), buffer);
    byte[] bytes = byteBuffer.array();
    buffer.put(bytes);
    len += bytes.length;
    return len;
  }

  /** CompressionType. */
  public static int write(CompressionType compressionType, OutputStream outputStream)
      throws IOException {
    byte n = compressionType.serialize();
    return write(n, outputStream);
  }

  /** write compressionType to byteBuffer. */
  public static int write(CompressionType compressionType, ByteBuffer buffer) {
    byte n = compressionType.serialize();
    return write(n, buffer);
  }

  /** TSDataType. */
  public static int write(TSDataType dataType, OutputStream outputStream) throws IOException {
    byte n = dataType.serialize();
    return write(n, outputStream);
  }

  public static int write(TSDataType dataType, ByteBuffer buffer) {
    byte n = dataType.serialize();
    return write(n, buffer);
  }

  /** TSEncoding. */
  public static int write(TSEncoding encoding, OutputStream outputStream) throws IOException {
    byte n = encoding.serialize();
    return write(n, outputStream);
  }

  public static int write(TSEncoding encoding, ByteBuffer buffer) {
    byte n = encoding.serialize();
    return write(n, buffer);
  }

  public static int sizeToWrite(Binary n) {
    return INT_LEN + n.getLength();
  }

  public static int sizeToWrite(String s) {
    if (s == null) {
      return INT_LEN;
    }
    return INT_LEN + s.getBytes().length;
  }

  /** read a byte var from inputStream. */
  public static byte readByte(InputStream inputStream) throws IOException {
    return (byte) inputStream.read();
  }

  /** read a short var from inputStream. */
  public static short readShort(InputStream inputStream) throws IOException {
    byte[] bytes = new byte[SHORT_LEN];
    int readLen = inputStream.read(bytes);
    if (readLen != SHORT_LEN) {
      throw new IOException(String.format(RETURN_ERROR, SHORT_LEN, readLen));
    }
    return BytesUtils.bytesToShort(bytes);
  }

  /** Read a short var from byteBuffer. */
  public static short readShort(ByteBuffer buffer) {
    return buffer.getShort();
  }

  /** Read a float var from inputStream. */
  public static float readFloat(InputStream inputStream) throws IOException {
    byte[] bytes = new byte[FLOAT_LEN];
    int readLen = inputStream.read(bytes);
    if (readLen != FLOAT_LEN) {
      throw new IOException(String.format(RETURN_ERROR, FLOAT_LEN, readLen));
    }
    return BytesUtils.bytesToFloat(bytes);
  }

  /** Read a float var from byteBuffer. */
  public static float readFloat(ByteBuffer byteBuffer) {
    byte[] bytes = new byte[FLOAT_LEN];
    byteBuffer.get(bytes);
    return BytesUtils.bytesToFloat(bytes);
  }

  /** Read a double var from inputStream. */
  public static double readDouble(InputStream inputStream) throws IOException {
    byte[] bytes = new byte[DOUBLE_LEN];
    int readLen = inputStream.read(bytes);
    if (readLen != DOUBLE_LEN) {
      throw new IOException(String.format(RETURN_ERROR, DOUBLE_LEN, readLen));
    }
    return BytesUtils.bytesToDouble(bytes);
  }

  /** Read a double var from byteBuffer. */
  public static double readDouble(ByteBuffer byteBuffer) {
    byte[] bytes = new byte[DOUBLE_LEN];
    byteBuffer.get(bytes);
    return BytesUtils.bytesToDouble(bytes);
  }

  /** Read an int var from inputStream. */
  public static int readInt(InputStream inputStream) throws IOException {
    byte[] bytes = new byte[INT_LEN];
    int readLen = inputStream.read(bytes);
    if (readLen != INT_LEN) {
      throw new IOException(String.format(RETURN_ERROR, INT_LEN, readLen));
    }
    return BytesUtils.bytesToInt(bytes);
  }

  /** Read a int var from byteBuffer. */
  public static int readInt(ByteBuffer buffer) {
    return buffer.getInt();
  }

  /**
   * Read an unsigned byte(0 ~ 255) as InputStream does.
   *
   * @return the byte or -1(means there is no byte to read)
   */
  public static int read(ByteBuffer buffer) {
    if (!buffer.hasRemaining()) {
      return NO_BYTE_TO_READ;
    }
    return buffer.get() & 0xFF;
  }

  /** Read a long var from inputStream. */
  public static long readLong(InputStream inputStream) throws IOException {
    byte[] bytes = new byte[LONG_LEN];
    int readLen = inputStream.read(bytes);
    if (readLen != LONG_LEN) {
      throw new IOException(String.format(RETURN_ERROR, LONG_LEN, readLen));
    }
    return BytesUtils.bytesToLong(bytes);
  }

  /** Read a long var from byteBuffer. */
  public static long readLong(ByteBuffer buffer) {
    return buffer.getLong();
  }

  /** Read string from inputStream. */
  public static String readString(InputStream inputStream) throws IOException {
    int strLength = readInt(inputStream);
    if (strLength <= 0) {
      return null;
    }
    byte[] bytes = new byte[strLength];
    int readLen = inputStream.read(bytes, 0, strLength);
    if (readLen != strLength) {
      throw new IOException(String.format(RETURN_ERROR, strLength, readLen));
    }
    return new String(bytes, 0, strLength, TSFileConfig.STRING_CHARSET);
  }

  /** String length's type is varInt */
  public static String readVarIntString(InputStream inputStream) throws IOException {
    int strLength = ReadWriteForEncodingUtils.readVarInt(inputStream);
    if (strLength < 0) {
      return null;
    } else if (strLength == 0) {
      return "";
    }
    byte[] bytes = new byte[strLength];
    int readLen = inputStream.read(bytes, 0, strLength);
    if (readLen != strLength) {
      throw new IOException(String.format(RETURN_ERROR, strLength, readLen));
    }
    return new String(bytes, 0, strLength, TSFileConfig.STRING_CHARSET);
  }

  /** Read string from byteBuffer. */
  public static String readString(ByteBuffer buffer) {
    int strLength = readInt(buffer);
    if (strLength < 0) {
      return null;
    } else if (strLength == 0) {
      return "";
    }
    byte[] bytes = new byte[strLength];
    buffer.get(bytes, 0, strLength);
    return new String(bytes, 0, strLength, TSFileConfig.STRING_CHARSET);
  }

  /** String length's type is varInt */
  public static String readVarIntString(ByteBuffer buffer) {
    int strLength = ReadWriteForEncodingUtils.readVarInt(buffer);
    if (strLength < 0) {
      return null;
    } else if (strLength == 0) {
      return "";
    }
    byte[] bytes = new byte[strLength];
    buffer.get(bytes, 0, strLength);
    return new String(bytes, 0, strLength, TSFileConfig.STRING_CHARSET);
  }

  /** Read string from byteBuffer with user define length. */
  public static String readStringWithLength(ByteBuffer buffer, int length) {
    if (length < 0) {
      return null;
    } else if (length == 0) {
      return "";
    }
    byte[] bytes = new byte[length];
    buffer.get(bytes, 0, length);
    return new String(bytes, 0, length, TSFileConfig.STRING_CHARSET);
  }

  public static ByteBuffer getByteBuffer(String s) {
    return ByteBuffer.wrap(s.getBytes(java.nio.charset.StandardCharsets.UTF_8));
  }

  public static ByteBuffer getByteBuffer(int i) {
    return ByteBuffer.allocate(4).putInt(0, i);
  }

  public static ByteBuffer getByteBuffer(long n) {
    return ByteBuffer.allocate(8).putLong(0, n);
  }

  public static ByteBuffer getByteBuffer(float f) {
    return ByteBuffer.allocate(4).putFloat(0, f);
  }

  public static ByteBuffer getByteBuffer(double d) {
    return ByteBuffer.allocate(8).putDouble(0, d);
  }

  public static ByteBuffer getByteBuffer(boolean i) {
    return ByteBuffer.allocate(1).put(i ? (byte) 1 : (byte) 0);
  }

  public static String readStringFromDirectByteBuffer(ByteBuffer buffer)
      throws CharacterCodingException {
    return java.nio.charset.StandardCharsets.UTF_8
        .newDecoder()
        .decode(buffer.duplicate())
        .toString();
  }

  /**
   * unlike InputStream.read(bytes), this method makes sure that you can read length bytes or reach
   * to the end of the stream.
   */
  public static byte[] readBytes(InputStream inputStream, int length) throws IOException {
    byte[] bytes = new byte[length];
    int offset = 0;
    int len;
    while (bytes.length - offset > 0
        && (len = inputStream.read(bytes, offset, bytes.length - offset)) != NO_BYTE_TO_READ) {
      offset += len;
    }
    return bytes;
  }

  public static Map<String, String> readMap(ByteBuffer buffer) {
    int length = readInt(buffer);
    if (length == NO_BYTE_TO_READ) {
      return null;
    }
    Map<String, String> map = new HashMap<>(length);
    for (int i = 0; i < length; i++) {
      // key
      String key = readString(buffer);
      // value
      String value = readString(buffer);
      map.put(key, value);
    }
    return map;
  }

  public static Map<String, String> readMap(InputStream inputStream) throws IOException {
    int length = readInt(inputStream);
    if (length == NO_BYTE_TO_READ) {
      return null;
    }
    Map<String, String> map = new HashMap<>(length);
    for (int i = 0; i < length; i++) {
      // key
      String key = readString(inputStream);
      // value
      String value = readString(inputStream);
      map.put(key, value);
    }
    return map;
  }

  public static LinkedHashMap<String, String> readLinkedHashMap(ByteBuffer buffer) {
    int length = readInt(buffer);
    if (length == NO_BYTE_TO_READ) {
      return null;
    }
    LinkedHashMap<String, String> map = new LinkedHashMap<>(length);
    for (int i = 0; i < length; i++) {
      // key
      String key = readString(buffer);
      // value
      String value = readString(buffer);
      map.put(key, value);
    }
    return map;
  }

  public static List<Map<String, String>> readMaps(ByteBuffer buffer, int totalSize) {
    List<Map<String, String>> results = new ArrayList<>(totalSize);
    for (int i = 0; i < totalSize; i++) {
      results.add(ReadWriteIOUtils.readMap(buffer));
    }
    return results;
  }

  /**
   * unlike InputStream.read(bytes), this method makes sure that you can read length bytes or reach
   * to the end of the stream.
   */
  public static byte[] readBytesWithSelfDescriptionLength(InputStream inputStream)
      throws IOException {
    int length = readInt(inputStream);
    return readBytes(inputStream, length);
  }

  public static Binary readBinary(ByteBuffer buffer) {
    int length = readInt(buffer);
    byte[] bytes = readBytes(buffer, length);
    return new Binary(bytes);
  }

  public static Binary readBinary(InputStream inputStream) throws IOException {
    int length = readInt(inputStream);
    byte[] bytes = readBytes(inputStream, length);
    return new Binary(bytes);
  }

  /**
   * read bytes from byteBuffer, this method makes sure that you can read length bytes or reach to
   * the end of the buffer.
   *
   * <p>read a int + buffer
   */
  public static byte[] readByteBufferWithSelfDescriptionLength(ByteBuffer buffer) {
    int byteLength = ReadWriteForEncodingUtils.readUnsignedVarInt(buffer);
    byte[] bytes = new byte[byteLength];
    buffer.get(bytes);
    return bytes;
  }

  /** read bytes from buffer with offset position to the end of buffer. */
  public static int readAsPossible(TsFileInput input, long position, ByteBuffer buffer)
      throws IOException {
    int length = 0;
    int read;
    while (buffer.hasRemaining() && (read = input.read(buffer, position)) != NO_BYTE_TO_READ) {
      length += read;
      position += read;
      input.read(buffer, position);
    }
    return length;
  }

  /** read util to the end of buffer. */
  public static int readAsPossible(TsFileInput input, ByteBuffer buffer) throws IOException {
    int length = 0;
    int read;
    while (buffer.hasRemaining() && (read = input.read(buffer)) != NO_BYTE_TO_READ) {
      length += read;
    }
    return length;
  }

  /** read bytes from buffer with offset position to the end of buffer or up to len. */
  public static int readAsPossible(TsFileInput input, ByteBuffer target, long offset, int len)
      throws IOException {
    int length = 0;
    int limit = target.limit();
    if (target.remaining() > len) {
      target.limit(target.position() + len);
    }
    int read;
    while (length < len
        && target.hasRemaining()
        && (read = input.read(target, offset)) != NO_BYTE_TO_READ) {
      length += read;
      offset += read;
    }
    target.limit(limit);
    return length;
  }

  /** read string list with self define length. */
  public static List<String> readStringList(InputStream inputStream) throws IOException {
    List<String> list = new ArrayList<>();
    int size = readInt(inputStream);

    for (int i = 0; i < size; i++) {
      list.add(readString(inputStream));
    }

    return list;
  }

  /** read string list with self define length. */
  public static List<String> readStringList(ByteBuffer buffer) {
    int size = readInt(buffer);
    if (size <= 0) {
      return Collections.emptyList();
    }

    List<String> list = new ArrayList<>();
    for (int i = 0; i < size; i++) {
      list.add(readString(buffer));
    }

    return list;
  }

  /** write string list with self define length. */
  public static void writeStringList(List<String> list, ByteBuffer buffer) {
    if (list == null) {
      throw new IllegalArgumentException("stringList must not be null!");
    }
    int size = list.size();
    buffer.putInt(size);
    for (String s : list) {
      write(s, buffer);
    }
  }

  /** write string list with self define length. */
  public static void writeStringList(List<String> list, OutputStream outputStream)
      throws IOException {
    if (list == null) {
      throw new IllegalArgumentException("stringList must not be null!");
    }
    int size = list.size();
    write(size, outputStream);
    for (String s : list) {
      write(s, outputStream);
    }
  }

  /** read integer set with self define length. */
  public static Set<Integer> readIntegerSet(ByteBuffer buffer) {
    int size = readInt(buffer);
    if (size <= 0) {
      return Collections.emptySet();
    }
    Set<Integer> set = new HashSet<>();
    for (int i = 0; i < size; i++) {
      set.add(readInt(buffer));
    }
    return set;
  }

  private static final String SET_NOT_NULL_MSG = "set must not be null!";

  // write integer set with self define length
  public static void writeIntegerSet(Set<Integer> set, OutputStream outputStream)
      throws IOException {
    if (set == null) {
      throw new IllegalArgumentException(SET_NOT_NULL_MSG);
    }
    int size = set.size();
    write(size, outputStream);
    for (int i : set) {
      write(i, outputStream);
    }
  }

  public static Set<Boolean> readBooleanSet(ByteBuffer buffer) {
    int size = readInt(buffer);
    if (size <= 0) {
      return Collections.emptySet();
    }
    Set<Boolean> set = new HashSet<>();
    for (int i = 0; i < size; i++) {
      set.add(readBoolean(buffer));
    }
    return set;
  }

  // Read long set with self define length
  public static Set<Long> readLongSet(ByteBuffer buffer) {
    int size = readInt(buffer);
    if (size <= 0) {
      return Collections.emptySet();
    }
    Set<Long> set = new HashSet<>();
    for (int i = 0; i < size; i++) {
      set.add(readLong(buffer));
    }
    return set;
  }

  public static Set<Float> readFloatSet(ByteBuffer buffer) {
    int size = readInt(buffer);
    if (size <= 0) {
      return Collections.emptySet();
    }
    Set<Float> set = new HashSet<>();
    for (int i = 0; i < size; i++) {
      set.add(readFloat(buffer));
    }
    return set;
  }

  public static Set<Double> readDoubleSet(ByteBuffer buffer) {
    int size = readInt(buffer);
    if (size <= 0) {
      return Collections.emptySet();
    }
    Set<Double> set = new HashSet<>();
    for (int i = 0; i < size; i++) {
      set.add(readDouble(buffer));
    }
    return set;
  }

  public static Set<Binary> readBinarySet(ByteBuffer buffer) {
    int size = readInt(buffer);
    if (size <= 0) {
      return Collections.emptySet();
    }
    Set<Binary> set = new HashSet<>();
    for (int i = 0; i < size; i++) {
      set.add(readBinary(buffer));
    }
    return set;
  }

  // read object set with self define length
  public static <T> Set<T> readObjectSet(ByteBuffer buffer) {
    int size = readInt(buffer);
    if (size <= 0) {
      return Collections.emptySet();
    }
    Set<T> set = new HashSet<>();
    for (int i = 0; i < size; i++) {
      set.add((T) readObject(buffer));
    }
    return set;
  }

  // write object set with self define length
  public static <T> void writeObjectSet(Set<T> set, DataOutputStream outputStream)
      throws IOException {
    if (set == null) {
      throw new IllegalArgumentException(SET_NOT_NULL_MSG);
    }
    write(set.size(), outputStream);
    for (T e : set) {
      writeObject(e, outputStream);
    }
  }

  public static void writeBooleanSet(Set<Boolean> set, DataOutputStream outputStream)
      throws IOException {
    write(set.contains(null) ? set.size() - 1 : set.size(), outputStream);
    for (Boolean e : set) {
      if (e != null) {
        write(e, outputStream);
      }
    }
  }

  public static void writeIntegerSet(Set<Integer> set, DataOutputStream outputStream)
      throws IOException {
    write(set.contains(null) ? set.size() - 1 : set.size(), outputStream);
    for (Integer e : set) {
      if (e != null) {
        write(e, outputStream);
      }
    }
  }

  // write long set with self define length
  public static void writeLongSet(Set<Long> set, DataOutputStream outputStream) throws IOException {
    write(set.contains(null) ? set.size() - 1 : set.size(), outputStream);
    for (Long e : set) {
      if (e != null) {
        write(e, outputStream);
      }
    }
  }

  public static void writeFloatSet(Set<Float> set, DataOutputStream outputStream)
      throws IOException {
    write(set.contains(null) ? set.size() - 1 : set.size(), outputStream);
    for (Float e : set) {
      if (e != null) {
        write(e, outputStream);
      }
    }
  }

  public static void writeDoubleSet(Set<Double> set, DataOutputStream outputStream)
      throws IOException {
    write(set.contains(null) ? set.size() - 1 : set.size(), outputStream);
    for (Double e : set) {
      if (e != null) {
        write(e, outputStream);
      }
    }
  }

  public static void writeBinarySet(Set<Binary> set, DataOutputStream outputStream)
      throws IOException {
    write(set.contains(null) ? set.size() - 1 : set.size(), outputStream);
    for (Binary e : set) {
      if (e != null) {
        write(e, outputStream);
      }
    }
  }

  public static CompressionType readCompressionType(InputStream inputStream) throws IOException {
    byte n = readByte(inputStream);
    return CompressionType.deserialize(n);
  }

  public static CompressionType readCompressionType(ByteBuffer buffer) {
    byte n = buffer.get();
    return CompressionType.deserialize(n);
  }

  public static TSDataType readDataType(InputStream inputStream) throws IOException {
    byte n = readByte(inputStream);
    return TSDataType.deserialize(n);
  }

  public static TSDataType readDataType(ByteBuffer buffer) {
    byte n = buffer.get();
    return TSDataType.deserialize(n);
  }

  public static TSEncoding readEncoding(InputStream inputStream) throws IOException {
    byte n = readByte(inputStream);
    return TSEncoding.deserialize(n);
  }

  public static TSEncoding readEncoding(ByteBuffer buffer) {
    byte n = buffer.get();
    return TSEncoding.deserialize(n);
  }

  /**
   * To check whether the byte buffer is reach the magic string this method doesn't change the
   * position of the byte buffer
   *
   * @param byteBuffer byte buffer
   * @return whether the byte buffer is reach the magic string
   */
  public static boolean checkIfMagicString(ByteBuffer byteBuffer) {
    byteBuffer.mark();
    boolean res = Arrays.equals(readBytes(byteBuffer, magicStringBytes.length), magicStringBytes);
    byteBuffer.reset();
    return res;
  }

  /**
   * to check whether the inputStream is reach the magic string this method doesn't change the
   * position of the inputStream
   *
   * @param inputStream inputStream
   * @return whether the inputStream is reach the magic string
   */
  public static boolean checkIfMagicString(InputStream inputStream) throws IOException {
    return inputStream.available() <= magicStringBytes.length;
  }

  public enum ClassSerializeId {
    LONG,
    DOUBLE,
    INTEGER,
    FLOAT,
    BINARY,
    BOOLEAN,
    STRING,
    NULL
  }

  public static void writeObject(Object value, DataOutputStream outputStream) {
    try {
      if (value instanceof Long) {
        outputStream.write(LONG.ordinal());
        outputStream.writeLong((Long) value);
      } else if (value instanceof Double) {
        outputStream.write(DOUBLE.ordinal());
        outputStream.writeDouble((Double) value);
      } else if (value instanceof Integer) {
        outputStream.write(INTEGER.ordinal());
        outputStream.writeInt((Integer) value);
      } else if (value instanceof Float) {
        outputStream.write(FLOAT.ordinal());
        outputStream.writeFloat((Float) value);
      } else if (value instanceof Binary) {
        outputStream.write(BINARY.ordinal());
        byte[] bytes = ((Binary) value).getValues();
        outputStream.writeInt(bytes.length);
        outputStream.write(bytes);
      } else if (value instanceof Boolean) {
        outputStream.write(BOOLEAN.ordinal());
        outputStream.write(Boolean.TRUE.equals(value) ? 1 : 0);
      } else if (value == null) {
        outputStream.write(NULL.ordinal());
      } else {
        outputStream.write(STRING.ordinal());
        byte[] bytes = value.toString().getBytes();
        outputStream.writeInt(bytes.length);
        outputStream.write(bytes);
      }
    } catch (IOException ignored) {
      // ignored
    }
  }

  public static void writeObject(Object value, ByteBuffer byteBuffer) {
    if (value instanceof Long) {
      byteBuffer.putInt(LONG.ordinal());
      byteBuffer.putLong((Long) value);
    } else if (value instanceof Double) {
      byteBuffer.putInt(DOUBLE.ordinal());
      byteBuffer.putDouble((Double) value);
    } else if (value instanceof Integer) {
      byteBuffer.putInt(INTEGER.ordinal());
      byteBuffer.putInt((Integer) value);
    } else if (value instanceof Float) {
      byteBuffer.putInt(FLOAT.ordinal());
      byteBuffer.putFloat((Float) value);
    } else if (value instanceof Binary) {
      byteBuffer.putInt(BINARY.ordinal());
      byte[] bytes = ((Binary) value).getValues();
      byteBuffer.putInt(bytes.length);
      byteBuffer.put(bytes);
    } else if (value instanceof Boolean) {
      byteBuffer.putInt(BOOLEAN.ordinal());
      byteBuffer.put(Boolean.TRUE.equals(value) ? (byte) 1 : (byte) 0);
    } else if (value == null) {
      byteBuffer.putInt(NULL.ordinal());
    } else {
      byteBuffer.putInt(STRING.ordinal());
      byte[] bytes = value.toString().getBytes();
      byteBuffer.putInt(bytes.length);
      byteBuffer.put(bytes);
    }
  }

  public static Object readObject(ByteBuffer buffer) {
    ClassSerializeId serializeId = ClassSerializeId.values()[buffer.get()];
    switch (serializeId) {
      case BOOLEAN:
        return buffer.get() == 1;
      case FLOAT:
        return buffer.getFloat();
      case DOUBLE:
        return buffer.getDouble();
      case LONG:
        return buffer.getLong();
      case INTEGER:
        return buffer.getInt();
      case BINARY:
        int length = buffer.getInt();
        byte[] bytes = new byte[length];
        buffer.get(bytes);
        return new Binary(bytes);
      case NULL:
        return null;
      case STRING:
      default:
        length = buffer.getInt();
        bytes = new byte[length];
        buffer.get(bytes);
        return new String(bytes);
    }
  }

  public static void writeInts(int[] ints, int offset, int length, OutputStream outputStream)
      throws IOException {
    write(length, outputStream);
    for (int i = 0; i < length; i++) {
      write(ints[offset + i], outputStream);
    }
  }

  public static int[] readInts(ByteBuffer buffer) {
    int length = readInt(buffer);
    int[] ints = new int[length];
    for (int i = 0; i < length; i++) {
      ints[i] = readInt(buffer);
    }
    return ints;
  }

  public static ByteBuffer clone(ByteBuffer original) {
    ByteBuffer clone = ByteBuffer.allocate(original.remaining());
    while (original.hasRemaining()) {
      clone.put(original.get());
    }

    clone.flip();
    return clone;
  }

  /**
   * The skip method of will return only if skipping n bytes or reaching end of file.
   *
   * @param inputStream the inputSteam to be skipped.
   * @param n the number of bytes to be skipped.
   * @throws IOException if the stream does not support seek, or if some other I/O error occurs.
   */
  public static void skip(InputStream inputStream, long n) throws IOException {
    while (n > 0) {
      long skipN = inputStream.skip(n);
      if (skipN > 0) {
        n -= skipN;
      } else {
        // read one byte to decide should we retry
        if (inputStream.read() == -1) {
          // EOF
          break;
        } else {
          n--;
        }
      }
    }
  }
}
