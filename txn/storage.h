// Author: Alexander Thomson (thomson@cs.yale.edu)
// Modified by: Kun Ren (kun.ren@yale.edu)

#ifndef _STORAGE_H_
#define _STORAGE_H_

#include <limits.h>
#include <tr1/unordered_map>
#include <deque>
#include <map>

#include "txn/common.h"
#include "txn/txn.h"
#include "utils/mutex.h"

using std::tr1::unordered_map;
using std::deque;
using std::map;


class Storage {
 public:
  // If there exists a record for the specified key, sets '*result' equal to
  // the value associated with the key and returns true, else returns false;
  // Note that the third parameter is only used for MVCC, the default vaule is 0.
  virtual bool Read(Key key, Value* result, int txn_unique_id = 0);

  // Inserts the record <key, value>, replacing any previous record with the
  // same key.
  // Note that the third parameter is only used for MVCC, the default vaule is 0.
  virtual void Write(Key key, Value value, int txn_unique_id = 0);

  // Returns the timestamp at which the record with the specified key was last
  // updated (returns 0 if the record has never been updated). This is used for OCC.
  virtual double Timestamp(Key key);
  
  // Init storage
  virtual void InitStorage();


  // modified code for images

  virtual bool ReadImage(Key key, Image* result, int txn_unique_id = 0);

  virtual void WriteImage(Key key, Image image, int txn_unique_id = 0);
  
  // Init image storage (1000 images that are 500KB each)
  virtual void InitImageStorage();



  // modified code for strings

  virtual bool ReadString(Key key, String* result, int txn_unique_id = 0);

  virtual void WriteString(Key key, String str, int txn_unique_id = 0);
  
  // Init image storage (1000 images that are 500KB each)
  virtual void InitStringStorage();


  // modified code for blog strings

  virtual bool ReadBlogString(Key key, BlogString* result, int txn_unique_id = 0);

  virtual void WriteBlogString(Key key, BlogString str, int txn_unique_id = 0);
  
  // Init image storage (1000 images that are 500KB each)
  virtual void InitBlogStringStorage();


  
  virtual ~Storage();
  
  // The following methods are only used for MVCC
  virtual void Lock(Key key) {}
  
  virtual void Unlock(Key key) {}
  
  virtual bool CheckWrite (Key key, int txn_unique_id) {return true;}
   
 private:
 
   friend class TxnProcessor;
   
   // Collection of <key, value> pairs. Use this for single-version storage
   unordered_map<Key, Value> data_;

   // Collection of <key, image> pairs. Use this for image storage
   unordered_map<Key, Image> images_;

   // Collection of <key, string> pairs. Use this for string storage
   unordered_map<Key, String> strings_;

   // Collection of <key, blog string> pairs. Use this for blog string storage
   unordered_map<Key, BlogString> blog_strings_;
  
   // Timestamps at which each key was last updated.
   unordered_map<Key, double> timestamps_;
};

#endif  // _STORAGE_H_
