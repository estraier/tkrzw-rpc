/*************************************************************************************************
 * Server utilities
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

#ifndef _TKRZW_SERVER_UTIL_H
#define _TKRZW_SERVER_UTIL_H

#include "tkrzw_cmd_util.h"

namespace tkrzw {

/**
 * Switches the process into the background.
 * @return The result status.
 */
Status DaemonizeProcess();

}  // namespace tkrzw

#endif  // _TKRZW_SERVER_UTIL_H

// END OF FILE
