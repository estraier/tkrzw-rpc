/*************************************************************************************************
 * Remote database manager implementation based on gRPC
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

#ifndef _TKRZW_DBM_REMOTE_H
#define _TKRZW_DBM_REMOTE_H

#include <map>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "tkrzw_dbm.h"
#include "tkrzw_dbm_ulog.h"
#include "tkrzw_lib_common.h"
#include "tkrzw_str_util.h"

namespace tkrzw {

class RemoteDBMImpl;
class RemoteDBMStreamImpl;
class RemoteDBMIteratorImpl;
class RemoteDBMReplicatorImpl;

/**
 * RPC interface to access the database service via gRPC protocol.
 * @details All operations are thread-safe; Multiple threads can access the same connection
 * concurrently.  The SetDBMIndex affects all threads so it should be called before the object is
 * shared.
 */
class RemoteDBM final {
 public:
  /**
   * Stream for better performance of intensive operations.
   * @details An instance of this class dominates a thread on the server so you should
   * destroy when it is no longer in use.
   */
  class Stream {
    friend class tkrzw::RemoteDBM;
   public:
    /**
     * Destructor.
     */
    ~Stream();

    /**
     * Copy and assignment are disabled.
     */
    explicit Stream(const Stream& rhs) = delete;
    Stream& operator =(const Stream& rhs) = delete;

    /**
     * Cancels the current operation.
     * @details This is called by another thread than the thread doing the operation.
     */
    void Cancel();

    /**
     * Sends a message and gets back the echo message.
     * @param message The message to send.
     * @param echo The pointer to a string object to contain the echo message.
     * @return The result status.
     */
    Status Echo(std::string_view message, std::string* echo);

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
     * @param ignore_result If true, the result status is not checked.
     * @return The result status.  If overwriting is abandoned, DUPLICATION_ERROR is returned.
     */
    Status Set(std::string_view key, std::string_view value, bool overwrite = true,
               bool ignore_result = false);

    /**
     * Removes a record of a key.
     * @param key The key of the record.
     * @param ignore_result If true, the result status is not checked.
     * @return The result status.  If there's no matching record, NOT_FOUND_ERROR is returned.
     */
    Status Remove(std::string_view key, bool ignore_result = false);

    /**
     * Appends data at the end of a record of a key.
     * @param key The key of the record.
     * @param value The value to append.
     * @param delim The delimiter to put after the existing record.
     * @param ignore_result If true, the result status is not checked.
     * @return The result status.
     * @details If there's no existing record, the value is set without the delimiter.
     */
    Status Append(std::string_view key, std::string_view value, std::string_view delim = "",
                  bool ignore_result = false);

    /**
     * Compares the value of a record and exchanges if the condition meets.
     * @param key The key of the record.
     * @param expected The expected value.  If the data is nullptr, no existing record is expected.
     * @param desired The desired value.  If the data is nullptr, the record is to be removed.
     * @return The result status.  If the condition doesn't meet, INFEASIBLE_ERROR is returned.
     */
    Status CompareExchange(std::string_view key, std::string_view expected,
                           std::string_view desired);

    /**
     * Increments the numeric value of a record.
     * @param key The key of the record.
     * @param increment The incremental value.  If it is INT64MIN, the current value is not changed
     * and a new record is not created.
     * @param current The pointer to an integer to contain the current value.  If it is nullptr,
     * it is ignored.
     * @param initial The initial value.
     * @param ignore_result If true, the result status is not checked.
     * @return The result status.
     * @details The record value is stored as an 8-byte big-endian integer.  Negative is also
     * supported.
     */
    Status Increment(std::string_view key, int64_t increment = 1,
                     int64_t* current = nullptr, int64_t initial = 0, bool ignore_result = false);

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

   private:
    /**
     * Constructor.
     * @param dbm_impl The database implementation object.
     */
    explicit Stream(RemoteDBMImpl* dbm_impl);

    /** Pointer to the actual implementation. */
    RemoteDBMStreamImpl* impl_;
  };

  /**
   * Iterator for each record.
   * @details When the database is updated, some iterators may or may not be invalided.
   * Operations with invalidated iterators fails gracefully with NOT_FOUND_ERROR.
   * @details An instance of this class dominates a thread on the server so you should
   * destroy when it is no longer in use.
   */
  class Iterator {
    friend class tkrzw::RemoteDBM;
   public:
    /**
     * Destructor.
     */
    ~Iterator();

    /**
     * Copy and assignment are disabled.
     */
    explicit Iterator(const Iterator& rhs) = delete;
    Iterator& operator =(const Iterator& rhs) = delete;

    /**
     * Cancels the current operation.
     * @details This is called by another thread than the thread doing the operation.
     */
    void Cancel();

    /**
     * Initializes the iterator to indicate the first record.
     * @return The result status.
     * @details Even if there's no record, the operation doesn't fail.
     */
    Status First();

    /**
     * Initializes the iterator to indicate the last record.
     * @return The result status.
     * @details Even if there's no record, the operation doesn't fail.  This method is suppoerted
     * only by ordered databases.
     */
    Status Last();

    /**
     * Initializes the iterator to indicate a specific record.
     * @param key The key of the record to look for.
     * @return The result status.
     * @details Ordered databases can support "lower bound" jump; If there's no record with the
     * same key, the iterator refers to the first record whose key is greater than the given key.
     * The operation fails with unordered databases if there's no record with the same key.
     */
    Status Jump(std::string_view key);

    /**
     * Initializes the iterator to indicate the last record whose key is lower than a given key.
     * @param key The key to compare with.
     * @param inclusive If true, the condition is inclusive: equal to or lower than the key.
     * @return The result status.
     * @details Even if there's no matching record, the operation doesn't fail.  This method is
     * suppoerted only by ordered databases.
     */
    Status JumpLower(std::string_view key, bool inclusive = false);

    /**
     * Initializes the iterator to indicate the first record whose key is upper than a given key.
     * @param key The key to compare with.
     * @param inclusive If true, the condition is inclusive: equal to or upper than the key.
     * @return The result status.
     * @details Even if there's no matching record, the operation doesn't fail.  This method is
     * suppoerted only by ordered databases.
     */
    Status JumpUpper(std::string_view key, bool inclusive = false);

    /**
     * Moves the iterator to the next record.
     * @return The result status.
     * @details If the current record is missing, the operation fails.  Even if there's no next
     * record, the operation doesn't fail.
     */
    Status Next();

    /**
     * Moves the iterator to the previous record.
     * @return The result status.
     * @details If the current record is missing, the operation fails.  Even if there's no previous
     * record, the operation doesn't fail.  This method is suppoerted only by ordered databases.
     */
    Status Previous();

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
     * Gets the key of the current record, in a simple way.
     * @param default_value The value to be returned on failure.
     * @return The key of the current record on success, or the default value on failure.
     */
    std::string GetKey(std::string_view default_value = "") {
      std::string key;
      return Get(&key, nullptr) == Status::SUCCESS ? key : std::string(default_value);
    }

    /**
     * Gets the value of the current record, in a simple way.
     * @param default_value The value to be returned on failure.
     * @return The value of the current record on success, or the default value on failure.
     */
    std::string GetValue(std::string_view default_value = "") {
      std::string value;
      return Get(nullptr, &value) == Status::SUCCESS ? value : std::string(default_value);
    }

    /**
     * Sets the value of the current record.
     * @param value The value of the record.
     * @return The result status.
     */
    Status Set(std::string_view value);

    /**
     * Removes the current record.
     * @return The result status.
     * @details If possible, the iterator moves to the next record.
     */
    Status Remove();

    /**
     * Gets the current record and moves the iterator to the next record.
     * @param key The pointer to a string object to contain the record key.  If it is nullptr,
     * the key data is ignored.
     * @param value The pointer to a string object to contain the record value.  If it is nullptr,
     * the value data is ignored.
     * @return The result status.
     */
    Status Step(std::string* key = nullptr, std::string* value = nullptr);

   private:
    /**
     * Constructor.
     * @param dbm_impl The database implementation object.
     */
    explicit Iterator(RemoteDBMImpl* dbm_impl);

    /** Pointer to the actual implementation. */
    RemoteDBMIteratorImpl* impl_;
  };

  /**
   * Wrapper of an update log for replication.
   */
  struct ReplicateLog : public DBMUpdateLoggerMQ::UpdateLog {
    /**
     * Default constructor.
     */
    ReplicateLog();

    /**
     * Destructor.
     */
    ~ReplicateLog();

   private:
    friend class RemoteDBMReplicatorImpl;
    /** The placeholder for the key and the value. */
    char* buffer_;
  };

  /**
   * Reader for update logs for asynchronous replicatoin.
   * @details An instance of this class dominates a thread on the server so you should
   * destroy when it is no longer in use.
   */
  class Replicator {
    friend class tkrzw::RemoteDBM;
   public:
    /**
     * Destructor.
     */
    ~Replicator();

    /**
     * Copy and assignment are disabled.
     */
    explicit Replicator(const Replicator& rhs) = delete;
    Replicator& operator =(const Replicator& rhs) = delete;

    /**
     * Cancels the current operation.
     * @details This is called by another thread than the thread doing the operation.
     */
    void Cancel();

    /**
     * Get the server ID of the master server.
     * @return The server ID of the master server or -1 on failure.
     * @details Ths can be called only after the Start method returns success.
     */
    int32_t GetMasterServerID();

    /**
     * Starts replication.
     * @param min_timestamp The minimum timestamp in milliseconds of messages to read.
     * @param server_id The server ID of the process.  Logs with the same server ID are skipped
     * to avoid infinite loop.
     * @param wait_time The time in seconds to wait for the next log.  Zero means no wait.
     * Negative means unlimited.
     * @return The result status.
     */
    Status Start(int64_t min_timestamp, int32_t server_id = 0, double wait_time = -1);

    /**
     * Reads the next update log.
     * @param timestamp The pointer to a variable to store the timestamp in milliseconds of the
     * message.  This is set if the result is SUCCESS or INFEASIBLE_ERROR.
     * @param op The pointer to the update log object to store the result.  The life duration of
     * the key and the value fields is the same as the given message.
     * @return The result status.  If the wait time passes, INFEASIBLE_ERROR is returned.
     * If the writer closes the file while waiting, CANCELED_ERROR is returned.
     */
    Status Read(int64_t* timestamp, ReplicateLog* op);

    /**
     * Constructor.
     * @param dbm_impl The database implementation object.
     */
    explicit Replicator(RemoteDBMImpl* dbm_impl);

    /** Pointer to the actual implementation. */
    RemoteDBMReplicatorImpl* impl_;
  };

  /**
   * Constructor.
   */
  RemoteDBM();

  /**
   * Destructor.
   */
  ~RemoteDBM();

  /**
   * Copy and assignment are disabled.
   */
  explicit RemoteDBM(const RemoteDBM& rhs) = delete;
  RemoteDBM& operator =(const RemoteDBM& rhs) = delete;

  /**
   * Injects a stub for testing.
   * @param stub The pointer to the DBMService::StubInterface object.  The ownership is taken.
   * @details This method is used instead of the Connect method.  With this, you can inject
   * mock stubs for testing and any kind of stubs with arbitrary auth/network settings.
   */
  void InjectStub(void* stub);

  /**
   * Connects to the server.
   * @param address The address or the host name of the server and its port number.  For IPv4
   * address, it's like "127.0.0.1:1978".  For IPv6, it's like "[::1]:1978".  For UNIX domain
   * sockets, it's like "unix:/path/to/file".
   * @param timeout The timeout in seconds for connection and each operation.  Negative means
   * unlimited.
   * @param auth_config The authentication configuration.  It it is empty, no authentication is
   * done.  If it begins with "ssl:", the SSL authentication is done.  Key-value parameters in
   * "key=value,key=value,..." format comes next.  For SSL, "key" and "cert" parameters  specify
   * the paths of the private key file and the certificate file respectively.
   * @return The result status.
   */
  Status Connect(const std::string& address, double timeout = -1,
                 const std::string& auth_config = "");

  /**
   * Disconnects the connection to the server.
   * @return The result status.
   */
  Status Disconnect();

  /**
   * Sets the index of the DBM to access.
   * @param dbm_index The index of the DBM to access.
   * @return The result status.
   */
  Status SetDBMIndex(int32_t dbm_index);

  /**
   * Sends a message and gets back the echo message.
   * @param message The message to send.
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
   * Gets the values of multiple records of keys, with a string view vector.
   * @param keys The keys of records to retrieve.
   * @param records The pointer to a map to store retrieved records.  Keys which don't match
   * existing records are ignored.
   * @return The result status.  If all records of the given keys are found, SUCCESS is returned.
   * If one or more records are missing, NOT_FOUND_ERROR is returned.  Thus, even with an error
   * code, the result map can have elements.
   */
  Status GetMulti(
      const std::vector<std::string_view>& keys, std::map<std::string, std::string>* records);

  /**
   * Gets the values of multiple records of keys, with an initializer list.
   * @param keys The keys of records to retrieve.
   * @param records The pointer to a map to store retrieved records.  Keys which don't match
   * existing records are ignored.
   * @return The result status.  If all records of the given keys are found, SUCCESS is returned.
   * If one or more records are missing, NOT_FOUND_ERROR is returned.  Thus, even with an error
   * code, the result map can have elements.
   */
  Status GetMulti(const std::initializer_list<std::string_view>& keys,
                  std::map<std::string, std::string>* records) {
    std::vector<std::string_view> vector_keys(keys.begin(), keys.end());
    return GetMulti(vector_keys, records);
  }

  /**
   * Gets the values of multiple records of keys, with a string vector.
   * @param keys The keys of records to retrieve.
   * @param records The pointer to a map to store retrieved records.  Keys which don't match
   * existing records are ignored.
   * @return The result status.  If all records of the given keys are found, SUCCESS is returned.
   * If one or more records are missing, NOT_FOUND_ERROR is returned.  Thus, even with an error
   * code, the result map can have elements.
   */
  Status GetMulti(
      const std::vector<std::string>& keys, std::map<std::string, std::string>* records) {
    return GetMulti(MakeStrViewVectorFromValues(keys), records);
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
   * Sets multiple records, with a map of string views.
   * @param records The records to store.
   * @param overwrite Whether to overwrite the existing value if there's a record with the same
   * key.  If true, the existing value is overwritten by the new value.  If false, the operation
   * is given up and an error status is returned.
   * @return The result status.  If there are records avoiding overwriting, DUPLICATION_ERROR
   * is returned.
   */
  Status SetMulti(
      const std::map<std::string_view, std::string_view>& records, bool overwrite = true);

  /**
   * Sets multiple records, with an initializer list.
   * @param records The records to store.
   * @param overwrite Whether to overwrite the existing value if there's a record with the same
   * key.  If true, the existing value is overwritten by the new value.  If false, the operation
   * is given up and an error status is returned.
   * @return The result status.  If there are records avoiding overwriting, DUPLICATION_ERROR
   * is returned.
   */
  Status SetMulti(
      const std::initializer_list<std::pair<std::string_view, std::string_view>>& records,
      bool overwrite = true) {
    std::map<std::string_view, std::string_view> map_records;
    for (const auto& record : records) {
      map_records.emplace(std::pair(
          std::string_view(record.first), std::string_view(record.second)));
    }
    return SetMulti(map_records, overwrite);
  }

  /**
   * Sets multiple records, with a map of strings.
   * @param records The records to store.
   * @param overwrite Whether to overwrite the existing value if there's a record with the same
   * key.  If true, the existing value is overwritten by the new value.  If false, the operation
   * is given up and an error status is returned.
   * @return The result status.  If there are records avoiding overwriting, DUPLICATION_ERROR
   * is returned.
   */
  Status SetMulti(
      const std::map<std::string, std::string>& records, bool overwrite = true) {
    return SetMulti(MakeStrViewMapFromRecords(records), overwrite);
  }

  /**
   * Removes a record of a key.
   * @param key The key of the record.
   * @return The result status.  If there's no matching record, NOT_FOUND_ERROR is returned.
   */
  Status Remove(std::string_view key);

  /**
   * Removes records of keys, with a string view vector.
   * @param keys The keys of records to remove.
   * @return The result status.  If there are missing records, NOT_FOUND_ERROR is returned.
   */
  Status RemoveMulti(const std::vector<std::string_view>& keys);

  /**
   * Removes records of keys, with an initializer list.
   * @param keys The keys of records to remove.
   * @return The result status.
   */
  Status RemoveMulti(const std::initializer_list<std::string_view>& keys) {
    std::vector<std::string_view> vector_keys(keys.begin(), keys.end());
    return RemoveMulti(vector_keys);
  }

  /**
   * Removes records of keys, with a string vector.
   * @param keys The keys of records to remove.
   * @return The result status.  If there are missing records, NOT_FOUND_ERROR is returned.
   */
  Status RemoveMulti(const std::vector<std::string>& keys) {
    return RemoveMulti(MakeStrViewVectorFromValues(keys));
  }

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
   * Appends data to multiple records, with a map of string views.
   * @param records The records to append.
   * @param delim The delimiter to put after the existing record.
   * @return The result status.
   * @details If there's no existing record, the value is set without the delimiter.
   */
  Status AppendMulti(
      const std::map<std::string_view, std::string_view>& records, std::string_view delim = "");

  /**
   * Appends data to multiple records, with an initializer list.
   * @param records The records to store.
   * @param delim The delimiter to put after the existing record.
   * @return The result status.
   * @details If there's no existing record, the value is set without the delimiter.
   */
  Status AppendMulti(
      const std::initializer_list<std::pair<std::string_view, std::string_view>>& records,
      std::string_view delim = "") {
    std::map<std::string_view, std::string_view> map_records;
    for (const auto& record : records) {
      map_records.emplace(std::pair(
          std::string_view(record.first), std::string_view(record.second)));
    }
    return AppendMulti(map_records, delim);
  }

  /**
   * Appends data to multiple records, with a map of strings.
   * @param records The records to append.
   * @param delim The delimiter to put after the existing record.
   * @return The result status.
   * @details If there's no existing record, the value is set without the delimiter.
   */
  Status AppendMulti(
      const std::map<std::string, std::string>& records, std::string_view delim = "") {
    return AppendMulti(MakeStrViewMapFromRecords(records), delim);
  }

  /**
   * Compares the value of a record and exchanges if the condition meets.
   * @param key The key of the record.
   * @param expected The expected value.  If the data is nullptr, no existing record is expected.
   * @param desired The desired value.  If the data is nullptr, the record is to be removed.
   * @return The result status.  If the condition doesn't meet, INFEASIBLE_ERROR is returned.
   */
  Status CompareExchange(std::string_view key, std::string_view expected,
                         std::string_view desired);

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
   * Compares the values of records and exchanges if the condition meets.
   * @param expected The record keys and their expected values.  If the value is nullptr, no
   * existing record is expected.
   * @param desired The record keys and their desired values.  If the value is nullptr, the
   * record is to be removed.
   * @return The result status.  If the condition doesn't meet, INFEASIBLE_ERROR is returned.
   */
  Status CompareExchangeMulti(
      const std::vector<std::pair<std::string_view, std::string_view>>& expected,
      const std::vector<std::pair<std::string_view, std::string_view>>& desired);

  /**
   * Changes the key of a record.
   * @param old_key The old key of the record.
   * @param new_key The new key of the record.
   * @param overwrite Whether to overwrite the existing record of the new key.
   * @param copying Whether to retain the record of the old key.
   * @return The result status.  If there's no matching record to the old key, NOT_FOUND_ERROR
   * is returned.  If the overwrite flag is false and there is an existing record of the new key,
   * DUPLICATION ERROR is returned.
   */
  Status Rekey(std::string_view old_key, std::string_view new_key,
               bool overwrite = true, bool copying = false);

  /**
   * Gets the first record and removes it.
   * @param key The pointer to a string object to contain the key of the first record.  If it
   * is nullptr, it is ignored.
   * @param value The pointer to a string object to contain the value of the first record.  If
   * it is nullptr, it is ignored.
   * @param retry_wait The maximum wait time in seconds before retrying.  If it is zero, no retry
   * is done.  If it is positive, retry is done and wait for the notifications of the next update
   * for the time at most.
   * @return The result status.
   */
  Status PopFirst(
      std::string* key = nullptr, std::string* value = nullptr, double retry_wait = 0);

  /**
   * Adds a record with a key of the current timestamp.
   * @param value The value of the record.
   * @param wtime The current wall time used to generate the key.  If it is negative, the system
   * clock is used.
   * @param notify If true, notification signal is sent.
   * @return The result status.
   * @details The key is generated as an 8-bite big-endian binary string of the timestamp.  If
   * there is an existing record matching the generated key, the key is regenerated and the
   * attempt is repeated until it succeeds.
   */
  Status PushLast(std::string_view value, double wtime = -1, bool notify = false);

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
   * @details Tuning options can be given by the optional parameters, as with the Open method of
   * the PolyDBM class and the database configurations of the server command.  A unset parameter
   * means that the current setting is succeeded or calculated implicitly.
   * @details In addition, HashDBM, TreeDBM, and SkipDBM supports the following parameters.
   *   - skip_broken_records (bool): If true, the operation continues even if there are broken
   *     records which can be skipped.
   *   - sync_hard (bool): If true, physical synchronization with the hardware is done before
   *     finishing the rebuilt file.
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
   * @details The "reducer" parameter specifies the reducer for SkipDBM.  "ReduceToFirst",
   * "ReduceToSecond", "ReduceToLast", etc are supported.  If the parameter "make_backup" exists,
   * a backup file is created in the same directory as the database file.  The backup file name
   * has a date suffix in GMT, like ".backup.20210831213749".  If the value of "make_backup" not
   * empty, it is the value is used as the suffix.
   */
  Status Synchronize(bool hard, const std::map<std::string, std::string>& params = {});

  /**
   * Searches a database and get keys which match a pattern, according to a mode expression.
   * @param mode The search mode.  "contain" extracts keys containing the pattern.  "begin"
   * extracts keys beginning with the pattern.  "end" extracts keys ending with the pattern.
   * "regex" extracts keys partially matches the pattern of a regular expression.  "edit"
   * extracts keys whose edit distance to the UTF-8 pattern is the least.  "editbin" extracts
   * keys whose edit distance to the binary pattern is the least.  Ordered databases support
   * "upper" and "lower" which extract keys whose positions are upper/lower than the pattern.
   * "upperinc" and "lowerinc" are their inclusive versions.
   * @param pattern The pattern for matching.
   * @param matched A vector to contain the result.
   * @param capacity The maximum records to obtain.  0 means unlimited.
   * @return The result status.
   */
  Status Search(
      std::string_view mode, std::string_view pattern,
      std::vector<std::string>* matched, size_t capacity = 0);

  /**
   * Changes the master server of the replication.
   * @param master The address of the master server.  If it is empty, replication stops.
   * @param timestamp_skew The timestamp skew in milliseconds.  Negative makes the timestamp back
   * to the past.
   */
  Status ChangeMaster(std::string_view master, double timestamp_skew = 0);

  /**
   * Makes a stream for intensive operations.
   * @return The stream for intensive operations.
   */
  std::unique_ptr<Stream> MakeStream();

  /**
   * Makes an iterator for each record.
   * @return The iterator for each record.
   */
  std::unique_ptr<Iterator> MakeIterator();

  /**
   * Makes a replicator for asynchronous replication.
   * @return The replicator for asynchronous replication.
   */
  std::unique_ptr<Replicator> MakeReplicator();

 private:
  /** Pointer to the actual implementation. */
  RemoteDBMImpl* impl_;
};

}  // namespace tkrzw

#endif  // _TKRZW_DBM_REMOTE_H

// END OF FILE
