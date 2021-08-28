/*************************************************************************************************
 * RPC API of Tkrzwb
 *
 * Copyright 2020 Google LLC
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License.  You may obtain a copy of the License at
 *     https://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied.  See the License for the specific language governing permissions
 * and limitations under the License.
 *************************************************************************************************/

#ifndef _TKRZW_RPC_H
#define _TKRZW_RPC_H

#include <map>
#include <string>
#include <string_view>
#include <tkrzw_lib_common.h>
#include <utility>
#include <vector>

namespace tkrzw {

class DBMClientImpl;
class DBMClientIteratorImpl;

/**
 * RPC interface to access the database service via gRPC protocol.
 */
class DBMClient final {
 public:
  /**
   * Iterator for each record.
   * @details When the database is updated, some iterators may or may not be invalided.
   * Operations with invalidated iterators fails gracefully with NOT_FOUND_ERROR.
   */
  class Iterator {
    friend class tkrzw::DBMClient;
   public:
    /**
     * Initializes the iterator to indicate the first record.
     * @return The result status.
     * @details Even if there's no record, the operation doesn't fail.
     */
    Status First();

    /**
     * Gets the key and the value of the current record of the iterator.
     * @param key The pointer to a string object to contain the record key.  If it is nullptr,
     * the key data is ignored.
     * @param value The pointer to a string object to contain the record value.  If it is nullptr,
     * the value data is ignored.
     * @return The result status.
     */
    Status Get(std::string* key = nullptr, std::string* value = nullptr);

    /**
     * Moves the iterator to the next record.
     * @return The result status.
     * @details If the current record is missing, the operation fails.  Even if there's no next
     * record, the operation doesn't fail.
     */
    Status Next();

   private:
    /**
     * Constructor.
     * @param dbm_impl The database implementation object.
     */
    explicit Iterator(DBMClientImpl* dbm_impl);

    /** Pointer to the actual implementation. */
    DBMClientIteratorImpl* impl_;
  };
  
  /**
   * Constructor.
   */
  DBMClient();

  /**
   * Destructor.
   */
  ~DBMClient();

  /**
   * Injects a stub for testing.
   * @param stub The pointer to the DBMService::StubInterface object.  The ownership is taken.
   */
  void InjectStub(void* stub);

  /**
   * Connects to the server.
   * @param host The host name of the server.
   * @param port The port number of the server.
   * @return The result status.
   */
  Status Connect(const std::string& host, int32_t port);

  /**
   * Disconnects the connection to the server.
   */
  void Disconnect();

  /**
   * Sets the index of the DBM to access.
   * @param dbm_index The index of the DBM to access.
   */
  void SetDBMIndex(int32_t dbm_index);

  /**
   * Sends a message and gets back the echo message.
   * @param echo The pointer to a string object to contain the echo message.
   * @return The result status.
   */
  Status Echo(std::string_view message, std::string* echo);

  /**
   * Inspects a database.
   * @param records The pointer to a map to store retrieved records.
   * @return The result status.
   * @details If the DBM index is negative, basic metadata of all DBMs are obtained.
   */
  Status Inspect(std::vector<std::pair<std::string, std::string>>* records);

  /**
   * Gets the value of a record of a key.
   * @param key The key of the record.
   * @param value The pointer to a string object to contain the result value.  If it is nullptr,
   * the value data is ignored.
   * @return The result status.  If there's no matching record, NOT_FOUND_ERROR is returned.
   */
  Status Get(std::string_view key, std::string* value = nullptr);

  /**
   * Gets the value of a record of a key, in a simple way.
   * @param key The key of the record.
   * @param default_value The value to be returned on failure.
   * @return The value of the matching record on success, or the default value on failure.
   */
  std::string GetSimple(std::string_view key, std::string_view default_value = "") {
    std::string value;
    return Get(key, &value) == Status::SUCCESS ? value : std::string(default_value);
  }

  /**
   * Sets a record of a key and a value.
   * @param key The key of the record.
   * @param value The value of the record.
   * @param overwrite Whether to overwrite the existing value if there's a record with the same
   * key.  If true, the existing value is overwritten by the new value.  If false, the operation
   * is given up and an error status is returned.
   * @return The result status.  If overwriting is abandoned, DUPLICATION_ERROR is returned.
   */
  Status Set(std::string_view key, std::string_view value, bool overwrite = true);

  /**
   * Removes a record of a key.
   * @param key The key of the record.
   * @return The result status.  If there's no matching record, NOT_FOUND_ERROR is returned.
   */
  Status Remove(std::string_view key);

  /**
   * Appends data at the end of a record of a key.
   * @param key The key of the record.
   * @param value The value to append.
   * @param delim The delimiter to put after the existing record.
   * @return The result status.
   * @details If there's no existing record, the value is set without the delimiter.
   */
  Status Append(std::string_view key, std::string_view value, std::string_view delim = "");

  /**
   * Increments the numeric value of a record.
   * @param key The key of the record.
   * @param increment The incremental value.  If it is INT64MIN, the current value is not changed
   * and a new record is not created.
   * @param current The pointer to an integer to contain the current value.  If it is nullptr,
   * it is ignored.
   * @param initial The initial value.
   * @return The result status.
   * @details The record value is stored as an 8-byte big-endian integer.  Negative is also
   * supported.
   */
  Status Increment(std::string_view key, int64_t increment = 1,
                   int64_t* current = nullptr, int64_t initial = 0);

  /**
   * Increments the numeric value of a record, in a simple way.
   * @param key The key of the record.
   * @param increment The incremental value.
   * @param initial The initial value.
   * @return The current value or INT64MIN on failure.
   * @details The record value is treated as a decimal integer.  Negative is also supported.
   */
  int64_t IncrementSimple(std::string_view key, int64_t increment = 1, int64_t initial = 0) {
    int64_t current = 0;
    return Increment(key, increment, &current, initial) == Status::SUCCESS ? current : INT64MIN;
  }

  /**
   * Gets the number of records.
   * @param count The pointer to an integer object to contain the result count.
   * @return The result status.
   */
  Status Count(int64_t* count);

  /**
   * Gets the number of records, in a simple way.
   * @return The number of records on success, or -1 on failure.
   */
  int64_t CountSimple() {
    int64_t count = 0;
    return Count(&count) == Status::SUCCESS ? count : -1;
  }

  /**
   * Gets the current file size of the database.
   * @param size The pointer to an integer object to contain the result size.
   * @return The result status.
   */
  Status GetFileSize(int64_t* size);

  /**
   * Gets the current file size of the database, in a simple way.
   * @return The current file size of the database, or -1 on failure.
   */
  int64_t GetFileSizeSimple() {
    int64_t size = 0;
    return GetFileSize(&size) == Status::SUCCESS ? size : -1;
  }

  /**
   * Removes all records.
   * @return The result status.
   */
  Status Clear();

  /**
   * Rebuilds the entire database.
   * @param params Optional parameters.
   * @return The result status.
   */
  Status Rebuild(const std::map<std::string, std::string>& params = {});

  /**
   * Checks whether the database should be rebuilt.
   * @param tobe The pointer to a boolean object to contain the result decision.
   * @return The result status.
   */
  Status ShouldBeRebuilt(bool* tobe);

  /**
   * Synchronizes the content of the database to the file system.
   * @param hard True to do physical synchronization with the hardware or false to do only
   * logical synchronization with the file system.
   * @param params Optional parameters.
   * @return The result status.
   */
  Status Synchronize(bool hard, const std::map<std::string, std::string>& params = {});

 private:
  /** Pointer to the actual implementation. */
  DBMClientImpl* impl_;
};

}  // namespace tkrzw

#endif  // _TKRZW_RPC_H

// END OF FILE
