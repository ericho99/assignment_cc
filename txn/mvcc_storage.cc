// Author: Kun Ren (kun.ren@yale.edu)
// Modified by Daniel Abadi

#include "txn/mvcc_storage.h"

// Init the storage
void MVCCStorage::InitStorage() {
  for (int i = 0; i < 1000000;i++) {
    Write(i, 0, 0);
    Mutex* key_mutex = new Mutex();
    mutexs_[i] = key_mutex;
  }
}

// Free memory.
MVCCStorage::~MVCCStorage() {
  for (unordered_map<Key, deque<Version*>*>::iterator it = mvcc_data_.begin();
       it != mvcc_data_.end(); ++it) {
    delete it->second;          
  }
  
  mvcc_data_.clear();
  
  for (unordered_map<Key, Mutex*>::iterator it = mutexs_.begin();
       it != mutexs_.end(); ++it) {
    delete it->second;          
  }
  
  mutexs_.clear();
}

// Lock the key to protect its version_list. Remember to lock the key when you read/update the version_list 
void MVCCStorage::Lock(Key key) {
  mutexs_[key]->Lock();
}

// Unlock the key.
void MVCCStorage::Unlock(Key key) {
  mutexs_[key]->Unlock();
}

// MVCC Read
bool MVCCStorage::Read(Key key, Value* result, int txn_unique_id) {
  // CPSC 438/538:
  //
  // Implement this method!
  
  // Hint: Iterate the version_lists and return the verion whose write timestamp
  // (version_id) is the largest write timestamp less than or equal to txn_unique_id.

  deque<Version *> *dq = mvcc_data_[key]; // get the proper list
  deque<Version *>::iterator it = dq->begin(); // set iterator to first record

  // if the iterator is empty return false
  if (it == dq->end()) {
    return false;
  }

  int i = 0;

  for (; it != dq->end(); ++it, ++i) {
    // If we have passed or reached the record we are looking for
    if (txn_unique_id >= (*it)->version_id_) {
      result = &((*it)->value_);

      // if the max_read_id_ is smaller than the current timestamp
      if ((*it)->max_read_id_ <= txn_unique_id) {
        //dq->at(i)->max_read_id_ = txn_unique_id;
        (*it)->max_read_id_ = txn_unique_id;
      }

      return true;
    }
  }

  // if the record we were looking for was the final one, the for loop fails
  --it;
  result = &((*it)->value_);

  // if the max_read_id_ is smaller than the current id
  if ((*it)->max_read_id_ <= txn_unique_id) {
    //dq->at(i)->max_read_id_ = txn_unique_id;
    (*it)->max_read_id_ = txn_unique_id;
  }

  return true; // valid record was the final one
}


// Check whether apply or abort the write
bool MVCCStorage::CheckWrite(Key key, int txn_unique_id) {
  // CPSC 438/538:
  //
  // Implement this method!
  
  // Hint: Before all writes are applied, we need to make sure that each write
  // can be safely applied based on MVCC timestamp ordering protocol. This method
  // only checks one key, so you should call this method for each key in the
  // write_set. Return true if this key passes the check, return false if not. 
  // Note that you don't have to call Lock(key) in this method, just
  // call Lock(key) before you call this method and call Unlock(key) afterward.
  
  // get the list of records for key
  deque<Version *> *dq = mvcc_data_[key];
  deque<Version *>::iterator it = dq->begin();

  // if the deque is empty
  if (it == dq->end())
    return true;

  while (it != dq->end()) {
    // if we've reached the insert point
    // check to see if the write is valid
    if ((*it)->version_id_ <= txn_unique_id) {
      // check to see if the timestamp is past previous max_read_id_
      if ((*it)->max_read_id_ <= txn_unique_id) {
        return true; // the last read was before txn_unique_id
      } else {
        return false; // read by txn after txn_unique_id
      }
    }

    ++it;
  }

  // reach here if all the records were newer than txn_unique_id
  --it; // get away from dq->end()
  if ((*it)->max_read_id_ <= txn_unique_id) {
    return true; // the final record was read before txn_unique_id
  }

  return false; // only reach here if final record was read after txn_unique_id
}

// MVCC Write, call this method only if CheckWrite return true.
void MVCCStorage::Write(Key key, Value value, int txn_unique_id) {
  // CPSC 438/538:
  //
  // Implement this method!
  
  // Hint: Insert a new version (malloc a Version and specify its value/version_id/max_read_id)
  // into the version_lists. Note that InitStorage() also calls this method to init storage. 
  // Note that you don't have to call Lock(key) in this method, just
  // call Lock(key) before you call this method and call Unlock(key) afterward.

  if (txn_unique_id == 0) {
    mvcc_data_[key] = new deque<Version *>();
  }
  
  // create the verion
  Version *new_version = (Version *) malloc(sizeof(Version));
  new_version->value_ = value;
  new_version->max_read_id_ = 0;
  new_version->version_id_ = txn_unique_id;

  // get the list of records for key
  deque<Version *> *dq = mvcc_data_[key];
  deque<Version *>::iterator it = dq->begin();

  // if the deque is empty
  if (it == dq->end()) {
    dq->push_front(new_version);
    return;
  }

  // iterate through the deque until passing all of the earlier records
  while (it != dq->end()) {
    // if we've reached a record that is older than txn_unique_id
    // then we should insert new_version before that record
    if ((*it)->version_id_ <= txn_unique_id) {
      //--it;
      new_version->max_read_id_ = (*it)->max_read_id_; // don't invalidate later reads
      //++it; // want to insert after previous record (not before)
      dq->insert(it, new_version);
      return;
    }

    ++it;
  }

  // if all of the records are larger than the new_version
  --it;
  new_version->max_read_id_ = (*it)->max_read_id_; // don't invalidate later reads
  ++it;
  dq->insert(it, new_version);
}

