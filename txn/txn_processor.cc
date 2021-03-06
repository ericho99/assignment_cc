// Author: Alexander Thomson (thomson@cs.yale.edu)
// Modified by: Christina Wallin (christina.wallin@yale.edu)
// Modified by: Kun Ren (kun.ren@yale.edu)

#include "txn/txn_processor.h"
#include <stdio.h>
#include <set>
#include <stdlib.h>

#include "txn/lock_manager.h"

// Thread & queue counts for StaticThreadPool initialization.
#define THREAD_COUNT 8

using namespace std;

TxnProcessor::TxnProcessor(CCMode mode)
    : mode_(mode), tp_(THREAD_COUNT), next_unique_id_(1) {
  if (mode_ == LOCKING_EXCLUSIVE_ONLY)
    lm_ = new LockManagerA(&ready_txns_);
  else if (mode_ == LOCKING)
    lm_ = new LockManagerB(&ready_txns_);
  else if (mode == TWOPL)
    lm_ = new LockManagerC(&ready_txns_);
  else if (mode == TWOPL2)
    lm_ = new LockManagerD(&ready_txns_);
  else if (mode == SILO)
    lm_ = new LockManagerD(&ready_txns_);
  
  // Create the storage
  if (mode_ == MVCC) {
    storage_ = new MVCCStorage();
  } else {
    storage_ = new Storage();
  }
  
  storage_->InitStorage();
  storage_->InitImageStorage();
  storage_->InitStringStorage();
  storage_->InitBlogStringStorage();

  // Start 'RunScheduler()' running.
  cpu_set_t cpuset;
  pthread_attr_t attr;
  pthread_attr_init(&attr);
  CPU_ZERO(&cpuset);
  CPU_SET(0, &cpuset);
  CPU_SET(1, &cpuset);       
  CPU_SET(2, &cpuset);
  CPU_SET(3, &cpuset);
  CPU_SET(4, &cpuset);
  CPU_SET(5, &cpuset);
  CPU_SET(6, &cpuset);  
  pthread_attr_setaffinity_np(&attr, sizeof(cpu_set_t), &cpuset);
  pthread_t scheduler_;
  pthread_create(&scheduler_, &attr, StartScheduler, reinterpret_cast<void*>(this));
  
}

void* TxnProcessor::StartScheduler(void * arg) {
  reinterpret_cast<TxnProcessor *>(arg)->RunScheduler();
  return NULL;
}

TxnProcessor::~TxnProcessor() {
  if (mode_ == LOCKING_EXCLUSIVE_ONLY || mode_ == LOCKING || mode_ == TWOPL || mode_ == TWOPL2)
    delete lm_;
    
  delete storage_;
}

void TxnProcessor::NewTxnRequest(Txn* txn) {
  // Atomically assign the txn a new number and add it to the incoming txn
  // requests queue.
  mutex_.Lock();
  txn->unique_id_ = next_unique_id_;
  next_unique_id_++;
  txn_requests_.Push(txn);
  mutex_.Unlock();
}

Txn* TxnProcessor::GetTxnResult() {
  Txn* txn;
  while (!txn_results_.Pop(&txn)) {
    // No result yet. Wait a bit before trying again (to reduce contention on
    // atomic queues).
    sleep(0.000001);
  }
  return txn;
}

void TxnProcessor::RunScheduler() {
  switch (mode_) {
    case SERIAL:                 RunSerialScheduler(); break;
    case LOCKING:                RunLockingScheduler(); break;
    case LOCKING_EXCLUSIVE_ONLY: RunLockingScheduler(); break;
    case OCC:                    RunOCCScheduler(); break;
    case P_OCC:                  RunOCCParallelScheduler(); break;
    case MVCC:                   RunMVCCScheduler(); break;
    case TWOPL:                  RunLockingSchedulerTwo(); break;
    case TWOPL2:                 RunTwoScheduler(); break;
    case SILO:                   RunOCCParallelScheduler(); break;
  }
}

void TxnProcessor::RunSerialScheduler() {
  Txn* txn;
  while (tp_.Active()) {
    // Get next txn request.
    if (txn_requests_.Pop(&txn)) {
      // Execute txn.
      if (txn->data_type_ == 1){
        ExecuteTxn(txn);
      }
      else if (txn->data_type_ == 2){
        ExecuteImageTxn(txn);
      }
      else if (txn->data_type_ == 3){
        ExecuteStringTxn(txn);
      }
      else if (txn->data_type_ == 4){
        ExecuteBlogStringTxn(txn);
      }

      // Commit/abort txn according to program logic's commit/abort decision.
      if (txn->Status() == COMPLETED_C) {
        // ApplyWrites(txn);
        if (txn->data_type_ == 1){
          ApplyWrites(txn);
        }
        else if (txn->data_type_ == 2){
          ApplyImageWrites(txn);
        }
        else if (txn->data_type_ == 3){
          ApplyStringWrites(txn);
        }
        else if (txn->data_type_ == 4){
          ApplyBlogStringWrites(txn);
        }
        txn->status_ = COMMITTED;
      } else if (txn->Status() == COMPLETED_A) {
        txn->status_ = ABORTED;
      } else {
        // Invalid TxnStatus!
        DIE("Completed Txn has invalid TxnStatus: " << txn->Status());
      }

      // Return result to client.
      txn_results_.Push(txn);
    }
  }
}

Key *TxnProcessor::KeySorter(set<Key>* set) {
  int len = set->size();
  if (len == 0) {
    return 0;
  }

  Key* sorted;
  sorted = (Key *) malloc(len * sizeof(Key));
  int i = 0;
  std::set<Key>::iterator it = set->begin();
  for (; it != set->end(); ++it) {
    sorted[i] = *it;
    i++;
  }

  int lowest = 0;
 
  for (int x = 0; x < len - 1; x++) {
    lowest = x;
    for (int y = x; y < len; y++) {
      if (sorted[y] < sorted[lowest]) {
        lowest = y;
      }
    }
    Key temp = sorted[x];
    sorted[x] = sorted[lowest];
    sorted[lowest] = temp;
  }

  return sorted;
}


vector<pair<Key, bool>> TxnProcessor::KeySorter2(set<Key>* rset, set<Key>* wset) {
  int rlen = rset->size();
  int wlen = wset->size();

  vector<pair<Key, bool>> setVector;

  if (rlen != 0) {
    std::set<Key>::iterator it = rset->begin();
    for (; it != rset->end(); ++it) {
      setVector.push_back(make_pair(*it, false));
    }
  }

  if (wlen != 0) {
    std::set<Key>::iterator it = wset->begin();
    for (; it != wset->end(); ++it) {
      setVector.push_back(make_pair(*it, true));
    }
  }

  uint64_t i, j, lowest;
  for (i = 0; i < setVector.size() - 1; ++i) {
    lowest = i;
    for (j = i; j < setVector.size(); ++j) {
      if (setVector[j].first < setVector[lowest].first) {
        lowest = j;
      }
    }
    pair<Key, bool> temp = setVector[i];
    setVector[i] = setVector[lowest];
    setVector[lowest] = temp;
  }

  return setVector;
}


void TxnProcessor::StartTwoExecuting(Txn *txn) {
  uint64_t i;
  vector<pair<Key, bool>> setVector = KeySorter2(&(txn->readset_), &(txn->writeset_));

  for (i = 0; i < setVector.size(); ++i) {
    Key current = setVector[i].first;
    bool isWrite = setVector[i].second;

    if (!isWrite) {
      while (!lm_->ReadLock(txn, current)) {
        //continue;
        sleep(1); // adjust this if necessary
      }

      if (txn->data_type_ == 1) {
        Value result;
        if (storage_->Read(current, &result))
          txn->reads_[current] = result;
      }
      else if (txn->data_type_ == 2) {
        Image result;
        if (storage_->ReadImage(current, &result))
          txn->readsIMG_[current] = result;
      }
      else if (txn->data_type_ == 3) {
        String result;
        if (storage_->ReadString(current, &result))
          txn->readsSTR_[current] = result;
      }
      else if (txn->data_type_ == 4) {
        BlogString result;
        if (storage_->ReadBlogString(current, &result))
          txn->readsBSTR_[current] = result;
      }
    } else {
      while (!lm_->WriteLock(txn, current)) {
        //continue;
        sleep(1); // adjust this if necessary
      }

      if (txn->data_type_ == 1) {
        Value result;
        if (storage_->Read(current, &result)) {
          txn->writes_[current] = result;
        }

        storage_->Write(current, result, txn->unique_id_);
      }
      else if (txn->data_type_ == 2) {
        Image result;
        if (storage_->ReadImage(current, &result)) {
          txn->readsIMG_[current] = result;
        }

        storage_->WriteImage(current, result, txn->unique_id_);
      }
      else if (txn->data_type_ == 3) {
        String result;
        if (storage_->ReadString(current, &result)) {
          txn->readsSTR_[current] = result;
        }

        storage_->WriteString(current, result, txn->unique_id_);
      }
      else if (txn->data_type_ == 4) {
        BlogString result;
        if (storage_->ReadBlogString(current, &result)) {
          txn->readsBSTR_[current] = result;
        }

        storage_->WriteBlogString(current, result, txn->unique_id_);
      }
    }
  }

  //Key *sortedReadset = KeySorter(&(txn->readset_));
  //for (i = 0; i < txn->readset_.size(); ++i) {
  //  Key current = sortedReadset[i];

  //  while (!lm_->ReadLock(txn, current)) {
  //    //continue;
  //    sleep(1); // adjust this if necessary
  //  }

  //  if (txn->data_type_ == 1) {
  //    Value result;
  //    if (storage_->Read(current, &result))
  //      txn->reads_[current] = result;
  //  }
  //  else if (txn->data_type_ == 2) {
  //    Image result;
  //    if (storage_->ReadImage(current, &result))
  //      txn->readsIMG_[current] = result;
  //  }
  //  else if (txn->data_type_ == 3) {
  //    String result;
  //    if (storage_->ReadString(current, &result))
  //      txn->readsSTR_[current] = result;
  //  }
  //  else if (txn->data_type_ == 4) {
  //    BlogString result;
  //    if (storage_->ReadBlogString(current, &result))
  //      txn->readsBSTR_[current] = result;
  //  }
  //}

  //free(sortedReadset);

  //Key *sortedWriteset = KeySorter(&(txn->writeset_));

  //for (i = 0; i < txn->writeset_.size(); ++i) {
  //  Key current = sortedWriteset[i];
  //  while (!lm_->WriteLock(txn, current)) {
  //    //continue;
  //    sleep(1); // adjust this if necessary
  //  }

  //  if (txn->data_type_ == 1) {
  //    Value result;
  //    if (storage_->Read(current, &result)) {
  //      txn->writes_[current] = result;
  //    }

  //    storage_->Write(current, result, txn->unique_id_);
  //  }
  //  else if (txn->data_type_ == 2) {
  //    Image result;
  //    if (storage_->ReadImage(current, &result)) {
  //      txn->readsIMG_[current] = result;
  //    }

  //    storage_->WriteImage(current, result, txn->unique_id_);
  //  }
  //  else if (txn->data_type_ == 3) {
  //    String result;
  //    if (storage_->ReadString(current, &result)) {
  //      txn->readsSTR_[current] = result;
  //    }

  //    storage_->WriteString(current, result, txn->unique_id_);
  //  }
  //  else if (txn->data_type_ == 4) {
  //    BlogString result;
  //    if (storage_->ReadBlogString(current, &result)) {
  //      txn->readsBSTR_[current] = result;
  //    }

  //    storage_->WriteBlogString(current, result, txn->unique_id_);
  //  }
  //}

  //free(sortedWriteset);

  // Execute txn's program logic.
  txn->Run();

  // shrinking phase
  for (i = 0; i < setVector.size(); ++i) {
    Key current = setVector[i].first;
    lm_->Release(txn, current);
  }

  // shrinking phase
  //for (i = 0; i < txn->readset_.size(); ++i) {
  //  Key current = sortedReadset[i];
  //  lm_->Release(txn, current);
  //}

  //// Release write locks.
  //for (set<Key>::iterator it = txn->writeset_.begin();
  //     it != txn->writeset_.end(); ++it) {
  //  lm_->Release(txn, *it);
  //}

  // Return result to client.
  txn_results_.Push(txn);
  return;
}

void TxnProcessor::RunTwoScheduler() {
  Txn* txn;
  while (tp_.Active()) {
    // Start processing the next incoming transaction request.
    if (txn_requests_.Pop(&txn)) {
      // Start txn running in its own thread.
      tp_.RunTask(new Method<TxnProcessor, void, Txn*>(
          this,
          &TxnProcessor::StartTwoExecuting,
          txn));
    }
  }
}

void TxnProcessor::RunLockingSchedulerTwo() {
  Txn* txn;
  while (tp_.Active()) {
    // Start processing the next incoming transaction request.
    if (txn_requests_.Pop(&txn)) {
      bool blocked = false;
      // Request read locks.
      for (set<Key>::iterator it = txn->readset_.begin();
           it != txn->readset_.end(); ++it) {
        if (!lm_->ReadLock(txn, *it)) {
          blocked = true;
          // If readset_.size() + writeset_.size() > 1, and blocked, just abort
          if (txn->readset_.size() + txn->writeset_.size() > 1) {
            // Release all locks that already acquired
            for (set<Key>::iterator it_reads = txn->readset_.begin(); true; ++it_reads) {
              lm_->Release(txn, *it_reads);
              if (it_reads == it) {
                break;
              }
            }
            break;
          }
        }
      }
          
      if (blocked == false) {
        // Request write locks.
        for (set<Key>::iterator it = txn->writeset_.begin();
             it != txn->writeset_.end(); ++it) {
          if (!lm_->WriteLock(txn, *it)) {
            blocked = true;
            // If readset_.size() + writeset_.size() > 1, and blocked, just abort
            if (txn->readset_.size() + txn->writeset_.size() > 1) {
              // Release all read locks that already acquired
              for (set<Key>::iterator it_reads = txn->readset_.begin(); it_reads != txn->readset_.end(); ++it_reads) {
                lm_->Release(txn, *it_reads);
              }
              // Release all write locks that already acquired
              for (set<Key>::iterator it_writes = txn->writeset_.begin(); true; ++it_writes) {
                lm_->Release(txn, *it_writes);
                if (it_writes == it) {
                  break;
                }
              }
              break;
            }
          }
        }
      }

      // If all read and write locks were immediately acquired, this txn is
      // ready to be executed. Else, just restart the txn
      if (blocked == false) {
        if (lm_->ReadyExecute(txn)) {
          ready_txns_.push_back(txn);
        }
      } else if (blocked == true && (txn->writeset_.size() + txn->readset_.size() > 1)){
        mutex_.Lock();
        //txn->unique_id_ = next_unique_id_;
        //next_unique_id_++;
        txn_requests_.Push(txn);
        mutex_.Unlock(); 
      }
    }

    // Process and commit all transactions that have finished running.
    while (completed_txns_.Pop(&txn)) {
      // Commit/abort txn according to program logic's commit/abort decision.
      if (txn->Status() == COMPLETED_C) {
        if (txn->data_type_ == 1){
          ApplyWrites(txn);
        }
        else if (txn->data_type_ == 2){
          ApplyImageWrites(txn);
        }
        else if (txn->data_type_ == 3){
          ApplyStringWrites(txn);
        }
        else if (txn->data_type_ == 4){
          ApplyBlogStringWrites(txn);
        }
        txn->status_ = COMMITTED;
      } else if (txn->Status() == COMPLETED_A) {
        txn->status_ = ABORTED;
      } else {
        // Invalid TxnStatus!
        DIE("Completed Txn has invalid TxnStatus: " << txn->Status());
      }
      
      // Release read locks.
      for (set<Key>::iterator it = txn->readset_.begin();
           it != txn->readset_.end(); ++it) {
        lm_->Release(txn, *it);
      }
      // Release write locks.
      for (set<Key>::iterator it = txn->writeset_.begin();
           it != txn->writeset_.end(); ++it) {
        lm_->Release(txn, *it);
      }

      // Return result to client.
      txn_results_.Push(txn);
    }

    // Start executing all transactions that have newly acquired all their
    // locks.
    while (ready_txns_.size()) {
      // Get next ready txn from the queue.
      txn = ready_txns_.front();
      ready_txns_.pop_front();

      if (txn->data_type_ == 1){
        // Start txn running in its own thread.
        tp_.RunTask(new Method<TxnProcessor, void, Txn*>(
            this,
            &TxnProcessor::ExecuteTxn,
            txn));
      }
      else if (txn->data_type_ == 2){
        // Start txn running in its own thread.
        tp_.RunTask(new Method<TxnProcessor, void, Txn*>(
            this,
            &TxnProcessor::ExecuteImageTxn,
            txn));
      }
      else if (txn->data_type_ == 3){
        // Start txn running in its own thread.
        tp_.RunTask(new Method<TxnProcessor, void, Txn*>(
            this,
            &TxnProcessor::ExecuteStringTxn,
            txn));
      }
      else if (txn->data_type_ == 4){
        // Start txn running in its own thread.
        tp_.RunTask(new Method<TxnProcessor, void, Txn*>(
            this,
            &TxnProcessor::ExecuteBlogStringTxn,
            txn));
      }

      // // Start txn running in its own thread.
      // tp_.RunTask(new Method<TxnProcessor, void, Txn*>(
      //       this,
      //       &TxnProcessor::ExecuteTxn,
      //       txn));

    }
  }
}

void TxnProcessor::RunLockingScheduler() {
  Txn* txn;
  while (tp_.Active()) {
    // Start processing the next incoming transaction request.
    if (txn_requests_.Pop(&txn)) {
      bool blocked = false;
      // Request read locks.
      for (set<Key>::iterator it = txn->readset_.begin();
           it != txn->readset_.end(); ++it) {
        if (!lm_->ReadLock(txn, *it)) {
          blocked = true;
          // If readset_.size() + writeset_.size() > 1, and blocked, just abort
          if (txn->readset_.size() + txn->writeset_.size() > 1) {
            // Release all locks that already acquired
            for (set<Key>::iterator it_reads = txn->readset_.begin(); true; ++it_reads) {
              lm_->Release(txn, *it_reads);
              if (it_reads == it) {
                break;
              }
            }
            break;
          }
        }
      }
          
      if (blocked == false) {
        // Request write locks.
        for (set<Key>::iterator it = txn->writeset_.begin();
             it != txn->writeset_.end(); ++it) {
          if (!lm_->WriteLock(txn, *it)) {
            blocked = true;
            // If readset_.size() + writeset_.size() > 1, and blocked, just abort
            if (txn->readset_.size() + txn->writeset_.size() > 1) {
              // Release all read locks that already acquired
              for (set<Key>::iterator it_reads = txn->readset_.begin(); it_reads != txn->readset_.end(); ++it_reads) {
                lm_->Release(txn, *it_reads);
              }
              // Release all write locks that already acquired
              for (set<Key>::iterator it_writes = txn->writeset_.begin(); true; ++it_writes) {
                lm_->Release(txn, *it_writes);
                if (it_writes == it) {
                  break;
                }
              }
              break;
            }
          }
        }
      }

      // If all read and write locks were immediately acquired, this txn is
      // ready to be executed. Else, just restart the txn
      if (blocked == false) {
        ready_txns_.push_back(txn);
      } else if (blocked == true && (txn->writeset_.size() + txn->readset_.size() > 1)){
        mutex_.Lock();
        txn->unique_id_ = next_unique_id_;
        next_unique_id_++;
        txn_requests_.Push(txn);
        mutex_.Unlock(); 
      }
    }

    // Process and commit all transactions that have finished running.
    while (completed_txns_.Pop(&txn)) {
      // Commit/abort txn according to program logic's commit/abort decision.
      if (txn->Status() == COMPLETED_C) {
        if (txn->data_type_ == 1){
          ApplyWrites(txn);
        }
        else if (txn->data_type_ == 2){
          ApplyImageWrites(txn);
        }
        else if (txn->data_type_ == 3){
          ApplyStringWrites(txn);
        }
        else if (txn->data_type_ == 4){
          ApplyBlogStringWrites(txn);
        }
        txn->status_ = COMMITTED;
      } else if (txn->Status() == COMPLETED_A) {
        txn->status_ = ABORTED;
      } else {
        // Invalid TxnStatus!
        DIE("Completed Txn has invalid TxnStatus: " << txn->Status());
      }
      
      // Release read locks.
      for (set<Key>::iterator it = txn->readset_.begin();
           it != txn->readset_.end(); ++it) {
        lm_->Release(txn, *it);
      }
      // Release write locks.
      for (set<Key>::iterator it = txn->writeset_.begin();
           it != txn->writeset_.end(); ++it) {
        lm_->Release(txn, *it);
      }

      // Return result to client.
      txn_results_.Push(txn);
    }

    // Start executing all transactions that have newly acquired all their
    // locks.
    while (ready_txns_.size()) {
      // Get next ready txn from the queue.
      txn = ready_txns_.front();
      ready_txns_.pop_front();

      if (txn->data_type_ == 1){
        // Start txn running in its own thread.
        tp_.RunTask(new Method<TxnProcessor, void, Txn*>(
            this,
            &TxnProcessor::ExecuteTxn,
            txn));
      }
      else if (txn->data_type_ == 2){
        // Start txn running in its own thread.
        tp_.RunTask(new Method<TxnProcessor, void, Txn*>(
            this,
            &TxnProcessor::ExecuteImageTxn,
            txn));
      }
      else if (txn->data_type_ == 3){
        // Start txn running in its own thread.
        tp_.RunTask(new Method<TxnProcessor, void, Txn*>(
            this,
            &TxnProcessor::ExecuteStringTxn,
            txn));
      }
      else if (txn->data_type_ == 4){
        // Start txn running in its own thread.
        tp_.RunTask(new Method<TxnProcessor, void, Txn*>(
            this,
            &TxnProcessor::ExecuteBlogStringTxn,
            txn));
      }
      // Start txn running in its own thread.
      // printf("this is the type: %d\n", txn->data_type_);
      // tp_.RunTask(new Method<TxnProcessor, void, Txn*>(
      //       this,
      //       &TxnProcessor::ExecuteTxn,
      //       txn));

    }
  }
}

void TxnProcessor::ExecuteTxn(Txn* txn) {

  // Get the start time
  txn->occ_start_time_ = GetTime();

  // Read everything in from readset.
  for (set<Key>::iterator it = txn->readset_.begin();
       it != txn->readset_.end(); ++it) {
    // Save each read result iff record exists in storage.
    Value result;
    if (storage_->Read(*it, &result))
      txn->reads_[*it] = result;
  }

  // Also read everything in from writeset.
  for (set<Key>::iterator it = txn->writeset_.begin();
       it != txn->writeset_.end(); ++it) {
    // Save each read result iff record exists in storage.
    Value result;
    if (storage_->Read(*it, &result))
      txn->reads_[*it] = result;
  }


  // Execute txn's program logic.
  txn->Run();

  // Hand the txn back to the RunScheduler thread.
  completed_txns_.Push(txn);
}

void TxnProcessor::ExecuteImageTxn(Txn* txn) {

  // Get the start time
  txn->occ_start_time_ = GetTime();

  // modified code for images

  // Read everything in from readset.
  for (set<Key>::iterator it = txn->readset_.begin();
       it != txn->readset_.end(); ++it) {
    // Save each read result iff record exists in storage.
    Image result;
    if (storage_->ReadImage(*it, &result))
      txn->readsIMG_[*it] = result;
  }

  // Also read everything in from writeset.
  for (set<Key>::iterator it = txn->writeset_.begin();
       it != txn->writeset_.end(); ++it) {
    // Save each read result iff record exists in storage.
    Image result;
    if (storage_->ReadImage(*it, &result))
      txn->readsIMG_[*it] = result;
  }


  // Execute txn's program logic.
  txn->Run();

  // Hand the txn back to the RunScheduler thread.
  completed_txns_.Push(txn);
}

void TxnProcessor::ExecuteStringTxn(Txn* txn) {

  // Get the start time
  txn->occ_start_time_ = GetTime();

  // modified code for strings

  // Read everything in from readset.
  for (set<Key>::iterator it = txn->readset_.begin();
       it != txn->readset_.end(); ++it) {
    // Save each read result iff record exists in storage.
    String result;
    if (storage_->ReadString(*it, &result))
      txn->readsSTR_[*it] = result;
  }

  // Also read everything in from writeset.
  for (set<Key>::iterator it = txn->writeset_.begin();
       it != txn->writeset_.end(); ++it) {
    // Save each read result iff record exists in storage.
    String result;
    if (storage_->ReadString(*it, &result))
      txn->readsSTR_[*it] = result;
  }


  // Execute txn's program logic.
  txn->Run();

  // Hand the txn back to the RunScheduler thread.
  completed_txns_.Push(txn);
}

void TxnProcessor::ExecuteBlogStringTxn(Txn* txn) {

  // Get the start time
  txn->occ_start_time_ = GetTime();

  // modified code for blog strings

  // Read everything in from readset.
  for (set<Key>::iterator it = txn->readset_.begin();
       it != txn->readset_.end(); ++it) {
    // Save each read result iff record exists in storage.
    BlogString result;
    if (storage_->ReadBlogString(*it, &result))
      txn->readsBSTR_[*it] = result;
  }

  // Also read everything in from writeset.
  for (set<Key>::iterator it = txn->writeset_.begin();
       it != txn->writeset_.end(); ++it) {
    // Save each read result iff record exists in storage.
    BlogString result;
    if (storage_->ReadBlogString(*it, &result))
      txn->readsBSTR_[*it] = result;
  }


  // Execute txn's program logic.
  txn->Run();

  // Hand the txn back to the RunScheduler thread.
  completed_txns_.Push(txn);
}

void TxnProcessor::ApplyWrites(Txn* txn) {
  // Write buffered writes out to storage.
  for (map<Key, Value>::iterator it = txn->writes_.begin();
       it != txn->writes_.end(); ++it) {
    storage_->Write(it->first, it->second, txn->unique_id_);
  }

}

void TxnProcessor::ApplyImageWrites(Txn* txn) {

  //modified code for images

  // Write buffered writes out to storage.
  for (map<Key, Image>::iterator it = txn->writesIMG_.begin();
       it != txn->writesIMG_.end(); ++it) {
    storage_->WriteImage(it->first, it->second, txn->unique_id_);
  }

}

void TxnProcessor::ApplyStringWrites(Txn* txn) {
  // Write buffered writes out to storage.
  for (map<Key, String>::iterator it = txn->writesSTR_.begin();
       it != txn->writesSTR_.end(); ++it) {
      storage_->WriteString(it->first, it->second, txn->unique_id_);
  }

}

void TxnProcessor::ApplyBlogStringWrites(Txn* txn) {
  // Write buffered writes out to storage.
  for (map<Key, BlogString>::iterator it = txn->writesBSTR_.begin();
       it != txn->writesBSTR_.end(); ++it) {
    storage_->WriteBlogString(it->first, it->second, txn->unique_id_);
  }

}

void TxnProcessor::ExecuteOCCTxn(Txn* txn) {
  if (txn->data_type_ == 1){
    ExecuteTxn(txn);
  }
  else if (txn->data_type_ == 2){
    ExecuteImageTxn(txn);
  }
  else if (txn->data_type_ == 3){
    ExecuteStringTxn(txn);
  }
  else if (txn->data_type_ == 4){
    ExecuteBlogStringTxn(txn);
  }
}

void TxnProcessor::RunOCCScheduler() {
  // CPSC 438/538:
  //
  // Implement this method!
  //
  // [For now, run serial scheduler in order to make it through the test
  // suite]

  Txn* txn;
  bool validated;

  while (tp_.Active()) {

    //Get the next new transaction request (if one is pending) and pass it to an execution thread.
    if (txn_requests_.Pop(&txn)) {
      tp_.RunTask(new Method<TxnProcessor, void, Txn*>(
            this,
            &TxnProcessor::ExecuteOCCTxn,
            txn));
    }
    
    //Validation Phase:
    // Process and commit all transactions that have finished running.
    while (completed_txns_.Pop(&txn)) {
      validated = true;

      // check all writes of the record to make sure it doesn't conflict with the current txn's readset/writeset
      for (set<Key>::iterator it = txn->writeset_.begin();
        it != txn->writeset_.end(); ++it) {
        if ((storage_->Timestamp(*it)) > (txn->occ_start_time_)) {
          validated  = false;
        }
      }
      for (set<Key>::iterator it = txn->readset_.begin();
        it != txn->readset_.end(); ++it) {
        if ((storage_->Timestamp(*it)) > (txn->occ_start_time_)) {
          validated  = false;
        }
      }
      // Commit/abort txn according to program logic's commit/abort decision.
      if (validated) {
        // ApplyWrites(txn); 
        if (txn->data_type_ == 1){
          ApplyWrites(txn);
        }
        else if (txn->data_type_ == 2){
          ApplyImageWrites(txn);
        }
        else if (txn->data_type_ == 3){
          ApplyStringWrites(txn);
        }
        else if (txn->data_type_ == 4){
          ApplyBlogStringWrites(txn);
        }
        txn->status_ = COMMITTED;
        txn_results_.Push(txn);
      } else  {
        // Cleanup txn
        txn->reads_.clear();
        txn->writes_.clear();
        txn->status_ = INCOMPLETE;
        // Completely restart the transaction.
        mutex_.Lock();
        txn->unique_id_ = next_unique_id_;
        next_unique_id_++;
        txn_requests_.Push(txn);
        mutex_.Unlock(); 
      }
    }
  }
}

void TxnProcessor::ExecuteTxnParallel(Txn *txn) {
    // Get the start time
  txn->occ_start_time_ = GetTime();
 
 
  bool blocked = false;
  // Request write locks.
    for (set<Key>::iterator it = txn->writeset_.begin();
             it != txn->writeset_.end(); ++it) {
      if (!lm_->WriteLock(txn, *it)) {
        blocked = true;
        // If writeset_.size() > 1, and blocked, just abort
        if (txn->writeset_.size() > 1) {
          // Release all write locks that already acquired
          for (set<Key>::iterator it_writes = txn->writeset_.begin(); true; ++it_writes) {
            lm_->Release(txn, *it_writes);
            if (it_writes == it) {
              break;
            }
          }
          break;
        }
      }
    }
 
      // If all write locks were immediately acquired, this txn is
      // ready to be executed. Else, just restart the txn
      /*if (blocked == false) {
    if (lm_->ReadyExecute(txn)) {
      ready_txns_.push_back(txn);
    }
      } else */if (blocked == true && (txn->writeset_.size() > 1)){
    mutex_.Lock();
    txn->unique_id_ = next_unique_id_;
    next_unique_id_++;
    txn_requests_.Push(txn);
    mutex_.Unlock();
    return;
      }

 
  // Key *sortedWriteset = KeySorter(&(txn->writeset_));
 
  // for (uint64_t i = 0; i < txn->writeset_.size(); ++i) {
  //   Key current = sortedWriteset[i];
  //   while (!lm_->WriteLock(txn, current)) {
  //     //continue;
  //     sleep(1); // adjust this if necessary
  //   }
  // }

  // free(sortedWriteset);
 
  // Read everything in from readset.
  for (set<Key>::iterator it = txn->readset_.begin();
       it != txn->readset_.end(); ++it) {
    // Save each read result iff record exists in storage.
    Value result;
    if (storage_->Read(*it, &result))
      txn->reads_[*it] = result;
  }
 
  // Read everything in from writeset
  for (set<Key>::iterator it = txn->writeset_.begin();
       it != txn->writeset_.end(); ++it) {
    // Save each read result iff record exists in storage.
    Value result;
    if (storage_->Read(*it, &result))
      txn->reads_[*it] = result;
  }
 
  // Execute txn's program logic.
  txn->Run();
 
  // critical section
  // creates copy of active set and inserts the transaction
  active_set_mutex_.Lock();
  set<Txn *> active_set_copy_ = active_set_.GetSet();
  active_set_.Insert(txn);
  active_set_mutex_.Unlock();
 
  bool validTxn = true;
  // Check times readset.
  for (set<Key>::iterator it = txn->readset_.begin();
      it != txn->readset_.end(); ++it) {
    if (storage_->Timestamp(*it) > txn->occ_start_time_) {
      validTxn = false;
      break;
    }
  }
 
  /********************
  ** Don't need this part because writes are locked
 
  // Also read everything in from writeset.
  if (validTxn) {
    for (set<Key>::iterator it = txn->writeset_.begin();
        it != txn->writeset_.end(); ++it) {
      if (storage_->Timestamp(*it) > txn->occ_start_time_) {
        validTxn = false;
        break;
      }
    }
  }
 
 *********************/
 
  // check all transactions in active set for overlaps
  if (validTxn) {
    for (set<Txn *>::iterator it = active_set_copy_.begin();
        it != active_set_copy_.end(); ++it) {
      if (txn->data_type_ == (*it)->data_type_) {
        // check readset of each transaction
        for (set<Key>::iterator txnIt = (*it)->writeset_.begin();
            txnIt != (*it)->writeset_.end(); ++txnIt) {
          if (txn->readset_.count(*txnIt) > 0) {
            validTxn = false;
            break;
          }
        }
      }
    }
  }
 
  if (validTxn) {
    // apply writes
    ApplyWrites(txn);
 
    // remove from active set (check if I need to obtain a lock on the entire active set table)
    active_set_.Erase(txn);
   
    // update commit status
    txn->status_ = COMMITTED;
 
    // Return result to client.
    txn_results_.Push(txn);
  } else {
    // remove from active set
    active_set_.Erase(txn);
 
    // cleanup txn
    txn->reads_.clear();
    txn->writes_.clear();
    txn->status_ = INCOMPLETE;
 
    // restart txn
    mutex_.Lock();
    txn->unique_id_ = next_unique_id_;
    next_unique_id_++;
    txn_requests_.Push(txn);
    mutex_.Unlock();
  }

  for (set<Key>::iterator it = txn->writeset_.begin();
       it != txn->writeset_.end(); ++it) {
    lm_->Release(txn, *it);
  }

}
 
void TxnProcessor::ExecuteTxnImageParallel(Txn *txn) {
    // Get the start time
  txn->occ_start_time_ = GetTime();
 

  bool blocked = false;
  // Request write locks.
    for (set<Key>::iterator it = txn->writeset_.begin();
             it != txn->writeset_.end(); ++it) {
      if (!lm_->WriteLock(txn, *it)) {
        blocked = true;
        // If writeset_.size() > 1, and blocked, just abort
        if (txn->writeset_.size() > 1) {
          // Release all write locks that already acquired
          for (set<Key>::iterator it_writes = txn->writeset_.begin(); true; ++it_writes) {
            lm_->Release(txn, *it_writes);
            if (it_writes == it) {
              break;
            }
          }
          break;
        }
      }
    }
 
      // If all write locks were immediately acquired, this txn is
      // ready to be executed. Else, just restart the txn
      /*if (blocked == false) {
    if (lm_->ReadyExecute(txn)) {
          std::cout << "check" << std::endl;
      ready_txns_.push_back(txn);
    }
      } else */if (blocked == true && (txn->writeset_.size() > 1)){
    mutex_.Lock();
    txn->unique_id_ = next_unique_id_;
    next_unique_id_++;
    txn_requests_.Push(txn);
    mutex_.Unlock();
    return;
      }

 
  // Key *sortedWriteset = KeySorter(&(txn->writeset_));
 
  // for (uint64_t i = 0; i < txn->writeset_.size(); ++i) {
  //   Key current = sortedWriteset[i];
  //   while (!lm_->WriteLock(txn, current)) {
  //     //continue;
  //     sleep(1); // adjust this if necessary
  //   }
 
  // }

  // free(sortedWriteset);
 
  // Read everything in from readset.
  for (set<Key>::iterator it = txn->readset_.begin();
       it != txn->readset_.end(); ++it) {
    // Save each read result iff record exists in storage.
    Image result;
    if (storage_->ReadImage(*it, &result))
      txn->readsIMG_[*it] = result;
  }
 
  // Read everything in from writeset
  for (set<Key>::iterator it = txn->writeset_.begin();
       it != txn->writeset_.end(); ++it) {
    // Save each read result iff record exists in storage.
    Image result;
    if (storage_->ReadImage(*it, &result))
      txn->readsIMG_[*it] = result;
  }
 
  // Execute txn's program logic.
  txn->Run();
 
  // critical section
  // creates copy of active set and inserts the transaction
  active_set_mutex_.Lock();
  set<Txn *> active_set_copy_ = active_set_.GetSet();
  active_set_.Insert(txn);
  active_set_mutex_.Unlock();
 
  bool validTxn = true;
  // Check times readset.
  for (set<Key>::iterator it = txn->readset_.begin();
      it != txn->readset_.end(); ++it) {
    if (storage_->Timestamp(*it) > txn->occ_start_time_) {
      validTxn = false;
      break;
    }
  }
 
  /********************
  ** Don't need this part because writes are locked
 
  // Also read everything in from writeset.
  if (validTxn) {
    for (set<Key>::iterator it = txn->writeset_.begin();
        it != txn->writeset_.end(); ++it) {
      if (storage_->Timestamp(*it) > txn->occ_start_time_) {
        validTxn = false;
        break;
      }
    }
  }
 
 *********************/
 
  // check all transactions in active set for overlaps
  if (validTxn) {
    for (set<Txn *>::iterator it = active_set_copy_.begin();
        it != active_set_copy_.end(); ++it) {
      if (txn->data_type_ == (*it)->data_type_) {
        // check readset of each transaction
        for (set<Key>::iterator txnIt = (*it)->writeset_.begin();
            txnIt != (*it)->writeset_.end(); ++txnIt) {
          if (txn->readset_.count(*txnIt) > 0) {
            validTxn = false;
            break;
          }
        }
      }
    }
  }
 
  if (validTxn) {
    // apply writes
    ApplyImageWrites(txn);
 
    // remove from active set (check if I need to obtain a lock on the entire active set table)
    active_set_.Erase(txn);
   
    // update commit status
    txn->status_ = COMMITTED;
 
    // Return result to client.
    txn_results_.Push(txn);
  } else {
    // remove from active set
    active_set_.Erase(txn);
 
    // cleanup txn
    txn->reads_.clear();
    txn->writes_.clear();
    txn->status_ = INCOMPLETE;
 
    // restart txn
    mutex_.Lock();
    txn->unique_id_ = next_unique_id_;
    next_unique_id_++;
    txn_requests_.Push(txn);
    mutex_.Unlock();
  }
  for (set<Key>::iterator it = txn->writeset_.begin();
       it != txn->writeset_.end(); ++it) {
    lm_->Release(txn, *it);
  }
}
 
void TxnProcessor::ExecuteTxnStringParallel(Txn *txn) {
    // Get the start time
  txn->occ_start_time_ = GetTime();
 

  bool blocked = false;
  // Request write locks.
    for (set<Key>::iterator it = txn->writeset_.begin();
             it != txn->writeset_.end(); ++it) {
      if (!lm_->WriteLock(txn, *it)) {
        blocked = true;
        // If writeset_.size() > 1, and blocked, just abort
        if (txn->writeset_.size() > 1) {
          // Release all write locks that already acquired
          for (set<Key>::iterator it_writes = txn->writeset_.begin(); true; ++it_writes) {
            lm_->Release(txn, *it_writes);
            if (it_writes == it) {
              break;
            }
          }
          break;
        }
      }
    }
 
      // If all write locks were immediately acquired, this txn is
      // ready to be executed. Else, just restart the txn
      /*if (blocked == false) {
    if (lm_->ReadyExecute(txn)) {
      ready_txns_.push_back(txn);
    }
      } else */if (blocked == true && (txn->writeset_.size() > 1)){
    mutex_.Lock();
    txn->unique_id_ = next_unique_id_;
    next_unique_id_++;
    txn_requests_.Push(txn);
    mutex_.Unlock();
    return;
      }

 
  // Key *sortedWriteset = KeySorter(&(txn->writeset_));
 
  // for (uint64_t i = 0; i < txn->writeset_.size(); ++i) {
  //   Key current = sortedWriteset[i];
  //   while (!lm_->WriteLock(txn, current)) {
  //     //continue;
  //     sleep(1); // adjust this if necessary
  //   }
  // }

  // free(sortedWriteset);
 
  // Read everything in from readset.
  for (set<Key>::iterator it = txn->readset_.begin();
       it != txn->readset_.end(); ++it) {
    // Save each read result iff record exists in storage.
    String result;
    if (storage_->ReadString(*it, &result))
      txn->readsSTR_[*it] = result;
  }
 
  // Read everything in from writeset
  for (set<Key>::iterator it = txn->writeset_.begin();
       it != txn->writeset_.end(); ++it) {
    // Save each read result iff record exists in storage.
    String result;
    if (storage_->ReadString(*it, &result))
      txn->readsSTR_[*it] = result;
  }
 
  // Execute txn's program logic.
  txn->Run();
 
  // critical section
  // creates copy of active set and inserts the transaction
  active_set_mutex_.Lock();
  set<Txn *> active_set_copy_ = active_set_.GetSet();
  active_set_.Insert(txn);
  active_set_mutex_.Unlock();
 
  bool validTxn = true;
  // Check times readset.
  for (set<Key>::iterator it = txn->readset_.begin();
      it != txn->readset_.end(); ++it) {
    if (storage_->Timestamp(*it) > txn->occ_start_time_) {
      validTxn = false;
      break;
    }
  }
 
  /********************
  ** Don't need this part because writes are locked
 
  // Also read everything in from writeset.
  if (validTxn) {
    for (set<Key>::iterator it = txn->writeset_.begin();
        it != txn->writeset_.end(); ++it) {
      if (storage_->Timestamp(*it) > txn->occ_start_time_) {
        validTxn = false;
        break;
      }
    }
  }
 
 *********************/
 
  // check all transactions in active set for overlaps
  if (validTxn) {
    for (set<Txn *>::iterator it = active_set_copy_.begin();
        it != active_set_copy_.end(); ++it) {
      if (txn->data_type_ == (*it)->data_type_) {
        // check readset of each transaction
        for (set<Key>::iterator txnIt = (*it)->writeset_.begin();
            txnIt != (*it)->writeset_.end(); ++txnIt) {
          if (txn->readset_.count(*txnIt) > 0) {
            validTxn = false;
            break;
          }
        }
      }
    }
  }
 
  if (validTxn) {
    // apply writes
    ApplyStringWrites(txn);
 
    // remove from active set (check if I need to obtain a lock on the entire active set table)
    active_set_.Erase(txn);
   
    // update commit status
    txn->status_ = COMMITTED;
 
    // Return result to client.
    txn_results_.Push(txn);
  } else {
    // remove from active set
    active_set_.Erase(txn);
 
    // cleanup txn
    txn->reads_.clear();
    txn->writes_.clear();
    txn->status_ = INCOMPLETE;
 
    // restart txn
    mutex_.Lock();
    txn->unique_id_ = next_unique_id_;
    next_unique_id_++;
    txn_requests_.Push(txn);
    mutex_.Unlock();
  }
  for (set<Key>::iterator it = txn->writeset_.begin();
       it != txn->writeset_.end(); ++it) {
    lm_->Release(txn, *it);
  }
}
 
void TxnProcessor::ExecuteTxnBlogStringParallel(Txn *txn) {
    // Get the start time
  txn->occ_start_time_ = GetTime();
 

  bool blocked = false;
  // Request write locks.
    for (set<Key>::iterator it = txn->writeset_.begin();
             it != txn->writeset_.end(); ++it) {
      if (!lm_->WriteLock(txn, *it)) {
        blocked = true;
        // If writeset_.size() > 1, and blocked, just abort
        if (txn->writeset_.size() > 1) {
          // Release all write locks that already acquired
          for (set<Key>::iterator it_writes = txn->writeset_.begin(); true; ++it_writes) {
            lm_->Release(txn, *it_writes);
            if (it_writes == it) {
              break;
            }
          }
          break;
        }
      }
    }
 
      // If all write locks were immediately acquired, this txn is
      // ready to be executed. Else, just restart the txn
      /*if (blocked == false) {
    if (lm_->ReadyExecute(txn)) {
      ready_txns_.push_back(txn);
    }
      } else */if (blocked == true && (txn->writeset_.size() > 1)){
    mutex_.Lock();
    txn->unique_id_ = next_unique_id_;
    next_unique_id_++;
    txn_requests_.Push(txn);
    mutex_.Unlock();
    return;
      }

 
  // Key *sortedWriteset = KeySorter(&(txn->writeset_));
 
  // for (uint64_t i = 0; i < txn->writeset_.size(); ++i) {
  //   Key current = sortedWriteset[i];
  //   while (!lm_->WriteLock(txn, current)) {
  //     //continue;
  //     sleep(1); // adjust this if necessary
  //   }
  // }

  // free(sortedWriteset);
 
  // Read everything in from readset.
  for (set<Key>::iterator it = txn->readset_.begin();
       it != txn->readset_.end(); ++it) {
    // Save each read result iff record exists in storage.
    BlogString result;
    if (storage_->ReadBlogString(*it, &result))
      txn->readsBSTR_[*it] = result;
  }
 
  // Read everything in from writeset
  for (set<Key>::iterator it = txn->writeset_.begin();
       it != txn->writeset_.end(); ++it) {
    // Save each read result iff record exists in storage.
    BlogString result;
    if (storage_->ReadBlogString(*it, &result))
      txn->readsBSTR_[*it] = result;
  }
 
  // Execute txn's program logic.
  txn->Run();
 
  // critical section
  // creates copy of active set and inserts the transaction
  active_set_mutex_.Lock();
  set<Txn *> active_set_copy_ = active_set_.GetSet();
  active_set_.Insert(txn);
  active_set_mutex_.Unlock();
 
  bool validTxn = true;
  // Check times readset.
  for (set<Key>::iterator it = txn->readset_.begin();
      it != txn->readset_.end(); ++it) {
    if (storage_->Timestamp(*it) > txn->occ_start_time_) {
      validTxn = false;
      break;
    }
  }
 
  /********************
  ** Don't need this part because writes are locked
 
  // Also read everything in from writeset.
  if (validTxn) {
    for (set<Key>::iterator it = txn->writeset_.begin();
        it != txn->writeset_.end(); ++it) {
      if (storage_->Timestamp(*it) > txn->occ_start_time_) {
        validTxn = false;
        break;
      }
    }
  }
 
 *********************/
 
  // check all transactions in active set for overlaps
  if (validTxn) {
    for (set<Txn *>::iterator it = active_set_copy_.begin();
        it != active_set_copy_.end(); ++it) {
      if (txn->data_type_ == (*it)->data_type_) {
        // check readset of each transaction
        for (set<Key>::iterator txnIt = (*it)->writeset_.begin();
            txnIt != (*it)->writeset_.end(); ++txnIt) {
          if (txn->readset_.count(*txnIt) > 0) {
            validTxn = false;
            break;
          }
        }
      }
    }
  }
 
  if (validTxn) {
    // apply writes
    ApplyBlogStringWrites(txn);
 
    // remove from active set (check if I need to obtain a lock on the entire active set table)
    active_set_.Erase(txn);
   
    // update commit status
    txn->status_ = COMMITTED;
 
    // Return result to client.
    txn_results_.Push(txn);
  } else {
    // remove from active set
    active_set_.Erase(txn);
 
    // cleanup txn
    txn->reads_.clear();
    txn->writes_.clear();
    txn->status_ = INCOMPLETE;
 
    // restart txn
    mutex_.Lock();
    txn->unique_id_ = next_unique_id_;
    next_unique_id_++;
    txn_requests_.Push(txn);
    mutex_.Unlock();
  }
  for (set<Key>::iterator it = txn->writeset_.begin();
       it != txn->writeset_.end(); ++it) {
    lm_->Release(txn, *it);
  }
}

 
void TxnProcessor::RunOCCParallelScheduler() {
  // CPSC 438/538:
  //
  // Implement this method! Note that implementing OCC with parallel
  // validation may need to create another method, like
  // TxnProcessor::ExecuteTxnParallel.
  // Note that you can use active_set_ and active_set_mutex_ we provided
  // for you in the txn_processor.h
  //
  // [For now, run serial scheduler in order to make it through the test
  // suite]
 
  Txn* txn;
  while (tp_.Active()) {
    // Get next txn request.
    if (txn_requests_.Pop(&txn)) {
      // Start txn running in its own thread.
      if(txn->data_type_ == 1) {
        tp_.RunTask(new Method<TxnProcessor, void, Txn*>(
              this,
              &TxnProcessor::ExecuteTxnParallel,
              txn));
      } else if(txn->data_type_ == 2) {
        tp_.RunTask(new Method<TxnProcessor, void, Txn*>(
              this,
              &TxnProcessor::ExecuteTxnImageParallel,
              txn));
      } else if(txn->data_type_ == 3) {
        tp_.RunTask(new Method<TxnProcessor, void, Txn*>(
              this,
              &TxnProcessor::ExecuteTxnStringParallel,
              txn));
      } else if(txn->data_type_ == 4) {
        tp_.RunTask(new Method<TxnProcessor, void, Txn*>(
              this,
              &TxnProcessor::ExecuteTxnBlogStringParallel,
              txn));
      }
    }
  }
}


void TxnProcessor::RunMVCCScheduler() {
  // CPSC 438/538:
  //
  // Implement this method!
  
  // Hint:Pop a txn from txn_requests_, and pass it to a thread to execute. 
  // Note that you may need to create another execute method, like TxnProcessor::MVCCExecuteTxn. 
  //
  // [For now, run serial scheduler in order to make it through the test
  // suite]
  RunSerialScheduler();
}



