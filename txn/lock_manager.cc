// Author: Alexander Thomson (thomson@cs.yale.edu)
// Modified by: Kun Ren (kun.ren@yale.edu)
//
// Lock manager implementing deterministic two-phase locking as described in
// 'The Case for Determinism in Database Systems'.

#include "txn/lock_manager.h"
#include "txn/txn.h"

LockManagerC::LockManagerC(deque<Txn*>* ready_txns) {
  ready_txns_ = ready_txns;
}

// now returns true if the txn is waiting on something with 
// lower priority, or has already acquired the lock
bool LockManagerC::WriteLock(Txn* txn, const Key& key) {
  // CPSC 438/538:
  //
  // Implement this method!

  // find the key in the lock table
  unordered_map<Key, deque<LockRequest>*>::const_iterator it = lock_table_.find(key);
  if (it == lock_table_.end()) {
    // create a deque at the key
    deque<LockRequest> *newTxnDeque = new deque<LockRequest>();
    LockRequest lr(EXCLUSIVE, txn);
    newTxnDeque->push_back(lr);
    lock_table_.insert({key, newTxnDeque});
    return true;
  } else {
    deque<LockRequest> *txnDeque = it->second;
    // push the transaction to the back of the deque
    LockRequest lr(EXCLUSIVE, txn);
    txnDeque->push_back(lr);

    // if it's the only transaction, then we grant the lock
    if (txnDeque->size() == 1) {
      return true;
    }

    // find transaction in txn_waits_
    unordered_map<Txn*, int>::const_iterator waitIt = txn_waits_.find(txn);
    // create new key if txn doesn't exist in txn_waits_
    if (waitIt == txn_waits_.end()) {
      txn_waits_.insert({txn, 1}); 
    } else {
      // increments value by 1 in txn_waits_
      txn_waits_[txn] += 1;
    }

    bool validWait = true;
    for (deque<LockRequest>::iterator dit = txnDeque->begin(); dit != txnDeque->end(); ++dit) {
      if (dit->txn_->unique_id_ <= txn->unique_id_) {
        validWait = false;
        break;
      }
    }

    return validWait;
  }
}

bool LockManagerC::ReadLock(Txn* txn, const Key& key) {
  // CPSC 438/538:
  //
  // Implement this method!
  
  // find the key in the lock table
  unordered_map<Key, deque<LockRequest>*>::const_iterator it = lock_table_.find(key);
  if (it == lock_table_.end()) {
    // create a deque at the key
    deque<LockRequest> *newTxnDeque = new deque<LockRequest>();
    LockRequest lr(SHARED, txn);
    newTxnDeque->push_back(lr);
    lock_table_.insert({key, newTxnDeque});
    return true;
  } else {
    deque<LockRequest> *txnDeque = it->second;

    // check if there has already been a read/write
    // also stores whether there are only shared lock requests in the deque
    bool onlyShared = true;
    for (deque<LockRequest>::iterator dit = txnDeque->begin(); dit != txnDeque->end(); ++dit) {
      if (dit->mode_ != SHARED) {
        onlyShared = false;
        break;
      }
    }

    // push the transaction to the back of the deque
    LockRequest lr(SHARED, txn);
    txnDeque->push_back(lr);

    // if it's the only transaction, then we grant the lock
    if (txnDeque->size() == 1 or onlyShared) {
      return true;
    }

    // find transaction in txn_waits_
    unordered_map<Txn*, int>::const_iterator waitIt = txn_waits_.find(txn);
    // create new key if txn doesn't exist in txn_waits_
    if (waitIt == txn_waits_.end()) {
      txn_waits_.insert({txn, 1}); 
    } else {
      // increments value by 1
      txn_waits_[txn] += 1;
    }

    bool validWait = true;
    for (deque<LockRequest>::iterator dit = txnDeque->begin(); dit != txnDeque->end(); ++dit) {
      if (dit->txn_->unique_id_ <= txn->unique_id_) {
        validWait = false;
        break;
      }
    }

    return validWait;
  }
}

void LockManagerC::Release(Txn* txn, const Key& key) {
  // CPSC 438/538:
  //
  // Implement this method!

  // find the deque
  unordered_map<Key, deque<LockRequest>*>::const_iterator it = lock_table_.find(key);
  if (it == lock_table_.end()){
    return; 
  } else {
    deque<LockRequest> *txnDeque = it->second;
    bool first = true;
    bool onlyShared = true;
    // iterate over elements of the deque to find the txn
    for (deque<LockRequest>::iterator it = txnDeque->begin(); it != txnDeque->end(); ++it) {
      if (it->txn_ == txn) {
        // remembers the next iterator
        deque<LockRequest>::iterator nextIt = it + 1;
        if (nextIt == txnDeque->end()) {
          // erases old entry
          unordered_map<Txn*, int>::const_iterator waitIt = txn_waits_.find(txn);
          if (waitIt != txn_waits_.end()) {
            // decrements in wait table
            txn_waits_[txn] -= 1;
          }

          txnDeque->erase(it);
          return;
        }
        
        if ((onlyShared and it->mode_ == EXCLUSIVE) or
            (first and it->mode_ == SHARED and nextIt->mode_ == EXCLUSIVE)) {
          // removes a prefix of shared locks from the wait table
          // or starts the next exclusive lock
          first = true;
          for (; nextIt != txnDeque->end(); ++nextIt) {
            if ((first and nextIt->mode_ == EXCLUSIVE) or
                (nextIt->mode_ == SHARED)) {
              unordered_map<Txn*, int>::const_iterator waitIt = txn_waits_.find(nextIt->txn_);
              if (waitIt != txn_waits_.end()) {
                // decrements in wait table
                if (waitIt->second == 1) {
                  txn_waits_.erase(waitIt);
                  ready_txns_->push_back(nextIt->txn_);
                } else {
                  txn_waits_[nextIt->txn_] -= 1;
                }
              }

              // ends prefix if we remove an exclusive lock from waiting
              if (nextIt->mode_ == EXCLUSIVE) {
                break;
              }
            } else if (nextIt->mode_ == EXCLUSIVE) {
              break;
            } else {
              printf("A HUGE ERROR HAS OCCURRED\n");
            }

            first = false;
          }
        }

        // erases old entry
        txnDeque->erase(it);
        return;
      }

      // check if prefixed by shared locks
      if (it->mode_ != SHARED) {
        onlyShared = false;
      }

      first = false;
    }
  }
}

LockMode LockManagerC::Status(const Key& key, vector<Txn*>* owners) {
  // CPSC 438/538:
  //
  // Implement this method!

  // clear owners first
  owners->clear();

  unordered_map<Key, deque<LockRequest>*>::const_iterator it = lock_table_.find(key);
  if (it == lock_table_.end()){
    return UNLOCKED;
  }

  // return the first one because we require exclusive locks
  // check if deque is empty first
  deque<LockRequest> *txnDeque = it->second;
  if (txnDeque->empty()) {
    return UNLOCKED;
  }

  // return prefix of shared locks or first exclusive lock
  bool sawShared = false;
  for (deque<LockRequest>::iterator it = txnDeque->begin(); it != txnDeque->end(); ++it) {
    if (it->mode_ == EXCLUSIVE and sawShared == false) {
      owners->push_back(it->txn_);
      return EXCLUSIVE;
    } else if (it->mode_ == SHARED) {
      owners->push_back(it->txn_);
      sawShared = true;
    } else if (sawShared) {
      return SHARED;
    } else {
      printf("SHOULDNT GET HERE: ERROR\n");
      return UNLOCKED;
    }
  }

  return SHARED;
}

// returns true if the transaction is not waiting on anything
bool LockManagerC::ReadyExecute(Txn *txn) {
  unordered_map<Txn*, int>::const_iterator waitIt = txn_waits_.find(txn);
  if (waitIt != txn_waits_.end()) {
    // decrements in wait table
    if (waitIt->second > 0) {
      return false;
    } else {
      txn_waits_.erase(waitIt);
    }
  }
  return true;
}

LockManagerA::LockManagerA(deque<Txn*>* ready_txns) {
  ready_txns_ = ready_txns;
}

bool LockManagerA::WriteLock(Txn* txn, const Key& key) {
  // CPSC 438/538:
  //
  // Implement this method!
  return true;
}

bool LockManagerA::ReadLock(Txn* txn, const Key& key) {
  // Since Part 1A implements ONLY exclusive locks, calls to ReadLock can
  // simply use the same logic as 'WriteLock'.
  return WriteLock(txn, key);
}

void LockManagerA::Release(Txn* txn, const Key& key) {
  // CPSC 438/538:
  //
  // Implement this method!
}

LockMode LockManagerA::Status(const Key& key, vector<Txn*>* owners) {
  // CPSC 438/538:
  //
  // Implement this method!
  return UNLOCKED;
}

bool LockManagerA::ReadyExecute(Txn *txn) {
  return true;
}

LockManagerB::LockManagerB(deque<Txn*>* ready_txns) {
  ready_txns_ = ready_txns;
}

bool LockManagerB::WriteLock(Txn* txn, const Key& key) {
  // CPSC 438/538:
  //
  // Implement this method!
  
  // find the key in the lock table
  unordered_map<Key, deque<LockRequest>*>::const_iterator it = lock_table_.find(key);
  if (it == lock_table_.end()) {
    // create a deque at the key
    deque<LockRequest> *newTxnDeque = new deque<LockRequest>();
    LockRequest lr(EXCLUSIVE, txn);
    newTxnDeque->push_back(lr);
    lock_table_.insert({key, newTxnDeque});
    return true;
  } else {
    deque<LockRequest> *txnDeque = it->second;
    // push the transaction to the back of the deque
    LockRequest lr(EXCLUSIVE, txn);
    txnDeque->push_back(lr);

    // if it's the only transaction, then we grant the lock
    if (txnDeque->size() == 1) {
      return true;
    }

    // find transaction in txn_waits_
    unordered_map<Txn*, int>::const_iterator waitIt = txn_waits_.find(txn);
    // create new key if txn doesn't exist in txn_waits_
    if (waitIt == txn_waits_.end()) {
      txn_waits_.insert({txn, 1}); 
    } else {
      // increments value by 1
      txn_waits_[txn] += 1;
    }
    return false;
  }
}

bool LockManagerB::ReadLock(Txn* txn, const Key& key) {
  // CPSC 438/538:
  //
  // Implement this method!
  
  // find the key in the lock table
  unordered_map<Key, deque<LockRequest>*>::const_iterator it = lock_table_.find(key);
  if (it == lock_table_.end()) {
    // create a deque at the key
    deque<LockRequest> *newTxnDeque = new deque<LockRequest>();
    LockRequest lr(SHARED, txn);
    newTxnDeque->push_back(lr);
    lock_table_.insert({key, newTxnDeque});
    return true;
  } else {
    deque<LockRequest> *txnDeque = it->second;

    // check if there has already been a read/write
    // also stores whether there are only shared lock requests in the deque
    bool onlyShared = true;
    for (deque<LockRequest>::iterator it = txnDeque->begin(); it != txnDeque->end(); ++it) {
      if (it->mode_ != SHARED) {
        onlyShared = false;
        break;
      }
    }

    // push the transaction to the back of the deque
    LockRequest lr(SHARED, txn);
    txnDeque->push_back(lr);

    // if it's the only transaction, then we grant the lock
    if (txnDeque->size() == 1 or onlyShared) {
      return true;
    }

    // find transaction in txn_waits_
    unordered_map<Txn*, int>::const_iterator waitIt = txn_waits_.find(txn);
    // create new key if txn doesn't exist in txn_waits_
    if (waitIt == txn_waits_.end()) {
      txn_waits_.insert({txn, 1}); 
    } else {
      // increments value by 1
      txn_waits_[txn] += 1;
    }
    return false;
  }
}

void LockManagerB::Release(Txn* txn, const Key& key) {
  // CPSC 438/538:
  //
  // Implement this method!
}

LockMode LockManagerB::Status(const Key& key, vector<Txn*>* owners) {
  // CPSC 438/538:
  //
  // Implement this method!
  return UNLOCKED;
}

bool LockManagerB::ReadyExecute(Txn *txn) {
  return true;
}
