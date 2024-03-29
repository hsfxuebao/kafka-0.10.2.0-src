/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.log

import java.io.File
import java.nio.ByteBuffer

import kafka.utils.CoreUtils.inLock
import kafka.common.InvalidOffsetException

/**
 * An index that maps offsets to physical file locations for a particular log segment. This index may be sparse:
 * that is it may not hold an entry for all messages in the log.
 * 
 * The index is stored in a file that is pre-allocated to hold a fixed maximum number of 8-byte entries.
 * 
 * The index supports lookups against a memory-map of this file. These lookups are done using a simple binary search variant
 * to locate the offset/location pair for the greatest offset less than or equal to the target offset.
 * 
 * Index files can be opened in two ways: either as an empty, mutable index that allows appends or
 * an immutable read-only index file that has previously been populated. The makeReadOnly method will turn a mutable file into an 
 * immutable one and truncate off any extra bytes. This is done when the index file is rolled over.
 * 
 * No attempt is made to checksum the contents of this file, in the event of a crash it is rebuilt.
 * 
 * The file format is a series of entries. The physical format is a 4 byte "relative" offset and a 4 byte file location for the 
 * message with that offset. The offset stored is relative to the base offset of the index file. So, for example,
 * if the base offset was 50, then the offset 55 would be stored as 5. Using relative offsets in this way let's us use
 * only 4 bytes for the offset.
 * 
 * The frequency of entries is up to the user of this class.
 * 
 * All external APIs translate from relative offsets to full offsets, so users of this class do not interact with the internal 
 * storage format.
 */
// 索引文件
class OffsetIndex(file: File, baseOffset: Long, maxIndexSize: Int = -1)
    extends AbstractIndex[Long, Int](file, baseOffset, maxIndexSize) {

  override def entrySize = 8
  
  /* the last offset in the index */
  private[this] var _lastOffset = lastEntry.offset
  
  debug("Loaded index file %s with maxEntries = %d, maxIndexSize = %d, entries = %d, lastOffset = %d, file position = %d"
    .format(file.getAbsolutePath, maxEntries, maxIndexSize, _entries, _lastOffset, mmap.position))

  /**
   * The last entry in the index
   */
  private def lastEntry: OffsetPosition = {
    inLock(lock) {
      _entries match {
        case 0 => OffsetPosition(baseOffset, 0)
        case s => parseEntry(mmap, s - 1).asInstanceOf[OffsetPosition]
      }
    }
  }

  def lastOffset: Long = _lastOffset

  /**
   * Find the largest offset less than or equal to the given targetOffset 
   * and return a pair holding this offset and its corresponding physical file position.
   * 
   * @param targetOffset The offset to look up.
   * @return The offset found and the corresponding file position for this offset
   *         If the target offset is smaller than the least entry in the index (or the index is empty),
   *         the pair (baseOffset, 0) is returned.
   */
    // 查询索引文件
  def lookup(targetOffset: Long): OffsetPosition = {
    maybeLock(lock) {
      // 查询mmap会发生变化，先复制一个出来
      val idx = mmap.duplicate
      // 二分查找
      val slot = indexSlotFor(idx, targetOffset, IndexSearchType.KEY)
      if(slot == -1)
        OffsetPosition(baseOffset, 0)
      else
        parseEntry(idx, slot).asInstanceOf[OffsetPosition]
    }
  }

  // 获取索引文件第n个索引条目的相对偏移量
  private def relativeOffset(buffer: ByteBuffer, n: Int): Int = buffer.getInt(n * entrySize)

  // 获取索引文件第n个索引条目的物理位置值
  private def physical(buffer: ByteBuffer, n: Int): Int = buffer.getInt(n * entrySize + 4)

  override def parseEntry(buffer: ByteBuffer, n: Int): IndexEntry = {
      // baseOffset + relativeOffset(buffer, n) 基准偏移量加上相对偏移量
      // physical(buffer, n) 绝对偏移量在数据文件中对应的物理位置
      OffsetPosition(baseOffset + relativeOffset(buffer, n), physical(buffer, n))
  }
  
  /**
   * Get the nth offset mapping from the index
   * @param n The entry number in the index
   * @return The offset/position pair at that entry
   */
  def entry(n: Int): OffsetPosition = {
    maybeLock(lock) {
      if(n >= _entries)
        throw new IllegalArgumentException("Attempt to fetch the %dth entry from an index of size %d.".format(n, _entries))
      val idx = mmap.duplicate
      OffsetPosition(relativeOffset(idx, n), physical(idx, n))
    }
  }

  /**
   * Append an entry for the given offset/location pair to the index. This entry must have a larger offset than all subsequent entries.
   * offset： 消息的绝对偏移量
   * position：未追加消息前数据文件的大小，即物理位置
   */
  def append(offset: Long, position: Int) {
    inLock(lock) {
      require(!isFull, "Attempt to append to a full index (size = " + _entries + ").")
      if (_entries == 0 || offset > _lastOffset) {
        debug("Adding index entry %d => %d to %s.".format(offset, position, file.getName))
        // NIO offset 逻辑上的位置  0 1 2
        mmap.putInt((offset - baseOffset).toInt)  // 存储相对偏移量
        // 物理上的位置：写这条数据在磁盘的哪个位置
        mmap.putInt(position) // 存储消息的物理位置

        /**
         * 结论：我们写索引的时候，会记录两个位置：一个是逻辑上位置，平时说的offset (偏移量)
         * 还有一个物理位置 指的就是这个消息在磁盘的哪个位置
         */
        _entries += 1
        _lastOffset = offset
        require(_entries * entrySize == mmap.position, entries + " entries but file position in index is " + mmap.position + ".")
      } else {
        throw new InvalidOffsetException("Attempt to append an offset (%d) to position %d no larger than the last offset appended (%d) to %s."
          .format(offset, entries, _lastOffset, file.getAbsolutePath))
      }
    }
  }

  override def truncate() = truncateToEntries(0)

  override def truncateTo(offset: Long) {
    inLock(lock) {
      val idx = mmap.duplicate
      val slot = indexSlotFor(idx, offset, IndexSearchType.KEY)

      /* There are 3 cases for choosing the new size
       * 1) if there is no entry in the index <= the offset, delete everything
       * 2) if there is an entry for this exact offset, delete it and everything larger than it
       * 3) if there is no entry for this offset, delete everything larger than the next smallest
       */
      val newEntries = 
        if(slot < 0)
          0
        else if(relativeOffset(idx, slot) == offset - baseOffset)
          slot
        else
          slot + 1
      truncateToEntries(newEntries)
    }
  }

  /**
   * Truncates index to a known number of entries.
   */
  private def truncateToEntries(entries: Int) {
    inLock(lock) {
      _entries = entries
      mmap.position(_entries * entrySize)
      _lastOffset = lastEntry.offset
    }
  }

  override def sanityCheck() {
    require(_entries == 0 || _lastOffset > baseOffset,
            s"Corrupt index found, index file (${file.getAbsolutePath}) has non-zero size but the last offset " +
                s"is ${_lastOffset} which is no larger than the base offset $baseOffset.")
    val len = file.length()
    require(len % entrySize == 0,
            "Index file " + file.getAbsolutePath + " is corrupt, found " + len +
            " bytes which is not positive or not a multiple of 8.")
  }

}
