/**

@mainpage Tkrzw-RPC: Remote-Procedure-Call API of Tkrzw

@section Introduction

Tkrzw is a library of DBM and provides features to mange key-value storages in various algorithms.  This package provides a server program manage databases and a library to access the service via gRPC protocol.

@li tkrzw::DBMClient -- RPC interface to access the service.

@code
#include "tkrzw_rpc.h"

// Main routine.
int main(int argc, char** argv) {
  // All symbols of Tkrzw are under the namespace "tkrzw".
  using namespace tkrzw;

  // Creates the client.
  DBMClient client;

  // Connects to the service.
  client.Connect("localhost", 1978);

  // Stores records.
  client.Set("foo", "hop");
  client.Set("bar", "step");
  client.Set("baz", "jump");

  // Retrieves records.
  std::cout << client.GetSimple("foo", "*") << std::endl;
  std::cout << client.GetSimple("bar", "*") << std::endl;
  std::cout << client.GetSimple("baz", "*") << std::endl;
  std::cout << client.GetSimple("outlier", "*") << std::endl;

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
 * @file tkrzw_rpc.h RPC API of Tkrzw.
 */

// END OF FILE
