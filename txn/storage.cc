// Author: Alexander Thomson (thomson@cs.yale.edu)
// Modified by: Kun Ren (kun.ren@yale.edu)

#include "txn/storage.h"

bool Storage::Read(Key key, Value* result, int txn_unique_id) {
  if (data_.count(key)) {
    *result = data_[key];
    return true;
  } else {
    return false;
  }
}

// Write value and timestamps
void Storage::Write(Key key, Value value, int txn_unique_id) {
  data_[key] = value;
  timestamps_[key] = GetTime();
}

double Storage::Timestamp(Key key) {
  if (timestamps_.count(key) == 0)
    return 0;
  return timestamps_[key];
}

// Init the storage
void Storage::InitStorage() {
  for (int i = 0; i < 1000000;i++) {
    Write(i, 0, 0);
  } 
}


// MODIFIED CODE ---------------------------------------------------
// RMW functions for images


bool Storage::ReadImage(Key key, Image* result, int txn_unique_id) {
  if (data_.count(key)) {

    // read in each image bit by bit
    Image img = images_[key];
    for (int x = 0; x < 50; x++) {
      (*result).byte[x] = img.byte[x];
    }
    return true;
  } else {
    return false;
  }
}

// Write value and timestamps
void Storage::WriteImage(Key key, Image image, int txn_unique_id) {

  // write each image bit by bit

  // Image img = images_[key];
  for (int x = 0; x < 50; x++) {
    (images_[key]).byte[x] = image.byte[x];
  }
  timestamps_[key] = GetTime();
}


// Init the storage
void Storage::InitImageStorage() {
  for (int i = 0; i < 1000;i++) {
    Image img;
    for (int x = 0; x < 50; x++) {
      img.byte[x] = 'a';
    }
    WriteImage(i, img, 0);
  } 
}


// for strings

bool Storage::ReadString(Key key, String* result, int txn_unique_id) {
  if (data_.count(key)) {

    // read in each image bit by bit
    String str = strings_[key];
    for (int x = 0; x < 10; x++) {
      (*result).byte[x] = str.byte[x];
    }
    return true;
  } else {
    return false;
  }
}

// Write value and timestamps
void Storage::WriteString(Key key, String str, int txn_unique_id) {

  // write each image bit by bit

  // Image img = images_[key];
  for (int x = 0; x < 10; x++) {
    (strings_[key]).byte[x] = str.byte[x];
  }
  timestamps_[key] = GetTime();
}


// Init the storage
void Storage::InitStringStorage() {
  for (int i = 0; i < 1000;i++) {
    String str;
    for (int x = 0; x < 10; x++) {
      str.byte[x] = 'a';
    }
    WriteString(i, str, 0);
  } 
}

// for blog strings

bool Storage::ReadBlogString(Key key, BlogString* result, int txn_unique_id) {
  if (data_.count(key)) {

    // read in each image bit by bit
    BlogString str = blog_strings_[key];
    for (int x = 0; x < 20; x++) {
      (*result).byte[x] = str.byte[x];
    }
    return true;
  } else {
    return false;
  }
}

// Write value and timestamps
void Storage::WriteBlogString(Key key, BlogString str, int txn_unique_id) {

  // write each image bit by bit

  // Image img = images_[key];
  for (int x = 0; x < 20; x++) {
    (blog_strings_[key]).byte[x] = str.byte[x];
  }
  timestamps_[key] = GetTime();
}


// Init the storage
void Storage::InitBlogStringStorage() {
  for (int i = 0; i < 1000;i++) {
    BlogString str;
    for (int x = 0; x < 20; x++) {
      str.byte[x] = 'a';
    }
    WriteBlogString(i, str, 0);
  } 
}



