/**

@mainpage Tkrzw-RPC: RPC interface of Tkrzw

@section Introduction

Tkrzw-RPC is provides a server program which manages databases of Tkrzw and a library to access the service via gRPC protocol.  Tkrzw is a library to mange key-value storages in various algorithms.  With Tkrzw, the application can handle database files efficiently in process without any network overhead.  However, it means that multiple processes cannot open the same database file simultaneously.  Tkrzw-RPC solves the issue by using a server program which manages database files and allowing other processes access the contents via RPC.  See the <a href="http://dbmx.net/tkrzw-rpc/">core document</a> for details.

@li tkrzw::RemoteDBM -- Remote database manager implementation.

The class RemoteDBM is a wrapper of gRPC stubs to access the database service.  It encapsulates details of gRPC so that you can access the databases as if they are handled locally.  Actually, the API of RemoteDBM is alomost the same as the local DBM classes of Tkrzw.  The noticeable differences are that you use the Connect method instead of the Open method and the Disconnect method instead of the Close method.

The server of Tkrzw-RPC can handle multiple databases at the same time.  An instance of RemoteDBM handles a connection to the servece and can access the multiple databases on the server.  By default, the connection is associated to the first database in the server.  You can switch it to another database by calling the SetDBMIndex method.  All methods of RemoteDBM are thread-safe so multiple threads can share the same instance as long as SetDBMIndex is not called in parallel.

Let's play with Tkrzw-RPC with the minimum settings.  First, run the database server.  An on-memory database is handled by default.  To stop the program, send a termination signal to the process by inputting Ctrl-C etc on the terminal.

@code
$ tkrzw_server --log_level debug
@endcode

While running the server program, run the program of the following code, which is the basic usage of Tkrzw-RPC.

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
