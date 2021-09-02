/*************************************************************************************************
 * Example for serious use cases of the remote database
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

#include "tkrzw_cmd_util.h"
#include "tkrzw_dbm_remote.h"

// Main routine.
int main(int argc, char** argv) {
  // All symbols of Tkrzw are under the namespace "tkrzw".
  using namespace tkrzw;

  // Creates the database manager.
  RemoteDBM dbm;

  // Connects to the database service.
  // The timeout is set to 1.5 seconds.
  Status status = dbm.Connect("localhost:1978", 1.5);
  if (status != Status::SUCCESS) {
    // Failure of the Connect operation is critical so we stop.
    Die("Connect failed: ", status);
  }

  // Makes a stream for better performance of intensive operations.
  std::unique_ptr<RemoteDBM::Stream> stream = dbm.MakeStream();

  // Stores records.
  // Bit-or assignment to the status updates the status if the original
  // state is SUCCESS and the new state is an error.
  status |= stream->Set("foo", "hop");
  status |= stream->Set("bar", "step");
  status |= stream->Set("baz", "jump");
  if (status != Status::SUCCESS) {
    // The Set operation shouldn't fail.  So we stop if it happens.
    Die("Set failed: ", status);
  }

  // Store records, ignoring the result status.
  stream->Set("quux", "land", true, true);
  stream->Set("xyzzy", "rest", true, true);

  // Retrieves records.
  // If there was no record, NOT_FOUND_ERROR would be returned.
  std::string value;
  status = stream->Get("foo", &value);
  if (status == Status::SUCCESS) {
    std::cout << value << std::endl;
  } else {
    std::cerr << "missing: " << status << std::endl;
  }

  // Destroys the stream if it is not used any longer.
  stream.reset(nullptr);

  // Makes an iterator for traversal operations.
  std::unique_ptr<RemoteDBM::Iterator> iter = dbm.MakeIterator();

  // Traverses records.
  if (iter->First() != Status::SUCCESS) {
    // Failure of the First operation is critical so we stop.
    Die("First failed: ", status);
  }
  while (true) {
    // Retrieves the current record data.
    std::string iter_key, iter_value;
    status = iter->Get(&iter_key, &iter_value);
    if (status == Status::SUCCESS) {
      std::cout << iter_key << ":" << iter_value << std::endl;
    } else {
      // This happens at the end of iteration.
      if (status != Status::NOT_FOUND_ERROR) {
        // Error types other than NOT_FOUND_ERROR are critical.
        Die("Iterator::Get failed: ", status);
      }
      break;
    }
    // Moves the iterator to the next record.
    status = iter->Next();
    if (status != Status::SUCCESS) {
      // This could happen if another thread removed the current record.
      if (status != Status::NOT_FOUND_ERROR) {
        // Error types other than NOT_FOUND_ERROR are critical.
        Die("Iterator::Get failed: ", status);
      }
      std::cerr << "missing: " << status << std::endl;
      break;
    }
  }

  // Destroys the iterator if it is not used any longer.
  iter.reset(nullptr);
  
  // Disconnects the connection.
  // Even if you forgot to disconnect it, the destructor would do it.
  // However, checking the status is a good manner.
  status = dbm.Disconnect();
  if (status != Status::SUCCESS) {
    // The Disconnect operation shouldn't fail.  So we stop if it happens.
    Die("Disconnect failed: ", status);
  }

  return 0;
}

// END OF FILE
