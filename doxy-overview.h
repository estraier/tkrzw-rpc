/**

@mainpage Tkrzw-RPC: Remote procedure call interface of Tkrzw

@section Introduction

Tkrzw is a library of DBM and provides features to mange key-value storages in various algorithms.  This package provides a server program manage databases and a library to access the service via gRPC protocol.

@li tkrzw::RemoteDBM -- Remote database manager implementation.

@code
#include "tkrzw_dbm_remote.h"

// Main routine.
int main(int argc, char** argv) {
  // All symbols of Tkrzw are under the namespace "tkrzw".
  using namespace tkrzw;

  // Creates the database manager.
  RemoteDBM dbm;

  // Connects to the database service.
  dbm.Connect("localhost", 1978);
  
  // Stores records.
  dbm.Set("foo", "hop");
  dbm.Set("bar", "step");
  dbm.Set("baz", "jump");

  // Retrieves records.
  std::cout << dbm.GetSimple("foo", "*") << std::endl;
  std::cout << dbm.GetSimple("bar", "*") << std::endl;
  std::cout << dbm.GetSimple("baz", "*") << std::endl;
  std::cout << dbm.GetSimple("outlier", "*") << std::endl;

  // Traverses records.
  std::unique_ptr<RemoteDBM::Iterator> iter = dbm.MakeIterator();
  iter->First();
  std::string key, value;
  while (iter->Get(&key, &value) == Status::SUCCESS) {
    std::cout << key << ":" << value << std::endl;
    iter->Next();
  }
  
  // Disconnects the connection.
  dbm.Disconnect();

  return 0;
}
@endcode

*/

/**
 * Common namespace of Tkrzw.
 */
namespace tkrzw {}

/**
 * @file tkrzw_dbm_remote.h Remote database manager implementation based on gRPC.
 */

// END OF FILE
