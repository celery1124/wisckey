/******* wisckey *******/
/* write_batch.h
* 07/23/2019
* by Mian Qin
*/

#ifndef _write_batch_h_
#define _write_batch_h_


#include <string>
#include <vector>
#include <utility>
#include "wisckey/status.h"

namespace wisckey {

class Slice;

class WriteBatch {
 public:
  WriteBatch();
  ~WriteBatch();

  // Store the mapping "key->value" in the database.
  void Put(const Slice& key, const Slice& value);

  // If the database contains a mapping for "key", erase it.  Else do nothing.
  void Delete(const Slice& key);

  // Clear all updates buffered in this batch.
  void Clear();

  // Batch size
  int Size();

 public:
  std::vector<std::pair<std::string, std::string>> batch_;
};

}  // namespace wisckey

#endif
