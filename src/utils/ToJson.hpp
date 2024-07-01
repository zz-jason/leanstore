#include "leanstore/btree/core/BTreeNode.hpp"
#include "leanstore/buffer-manager/BufferFrame.hpp"
#include "leanstore/concurrency/WalEntry.hpp"
#include "leanstore/utils/JsonUtil.hpp"
#include "leanstore/utils/Log.hpp"

#include <sstream>

using leanstore::utils::AddMemberToJson;

namespace leanstore::utils {

const char kCrc32[] = "mCrc32";
const char kLsn[] = "mLsn";
const char kSize[] = "mSize";
const char kType[] = "mType";
const char kTxId[] = "mTxId";
const char kWorkerId[] = "mWorkerId";
const char kPrevLsn[] = "mPrevLsn";
const char kGsn[] = "mGsn";
const char kTreeId[] = "mTreeId";
const char kPageId[] = "mPageId";

//! Convert the object to a JSON document.
template <typename ObjType>
void ToJson(const ObjType* obj [[maybe_unused]], rapidjson::Document* doc [[maybe_unused]]) {
}

//! Convert the object to a JSON document.
template <typename ObjType, typename JsonType, typename JsonAllocator>
void ToJson(ObjType* obj [[maybe_unused]], JsonType* doc [[maybe_unused]],
            JsonAllocator* allocator [[maybe_unused]]) {
}

template <>
inline void ToJson(const leanstore::cr::WalEntry* entry, rapidjson::Document* doc) {
  // type
  {
    auto typeName = entry->TypeName();
    rapidjson::Value member;
    member.SetString(typeName.data(), typeName.size(), doc->GetAllocator());
    doc->AddMember(kType, std::move(member), doc->GetAllocator());
  }
}

template <>
inline void ToJson(const leanstore::cr::WalTxAbort* entry, rapidjson::Document* doc) {
  // base
  ToJson(static_cast<const leanstore::cr::WalEntry*>(entry), doc);

  // txid
  {
    rapidjson::Value member;
    member.SetUint64(entry->mTxId);
    doc->AddMember(kTxId, member, doc->GetAllocator());
  }
}

template <>
inline void ToJson(const leanstore::cr::WalTxFinish* entry, rapidjson::Document* doc) {
  // base
  ToJson(static_cast<const leanstore::cr::WalEntry*>(entry), doc);

  // txid
  {
    rapidjson::Value member;
    member.SetUint64(entry->mTxId);
    doc->AddMember(kTxId, member, doc->GetAllocator());
  }
}

template <>
inline void ToJson(const leanstore::cr::WalCarriageReturn* entry, rapidjson::Document* doc) {
  // base
  ToJson(static_cast<const leanstore::cr::WalEntry*>(entry), doc);

  // size
  {
    rapidjson::Value member;
    member.SetUint64(entry->mSize);
    doc->AddMember(kTxId, member, doc->GetAllocator());
  }
}

template <>
inline void ToJson(const leanstore::cr::WalEntryComplex* obj, rapidjson::Document* doc) {
  // base
  ToJson(static_cast<const leanstore::cr::WalEntry*>(obj), doc);

  // crc
  {
    rapidjson::Value member;
    member.SetUint(obj->mCrc32);
    doc->AddMember(kCrc32, member, doc->GetAllocator());
  }

  // lsn
  {
    rapidjson::Value member;
    member.SetUint64(obj->mLsn);
    doc->AddMember(kLsn, member, doc->GetAllocator());
  }

  // size
  {
    rapidjson::Value member;
    member.SetUint64(obj->mSize);
    doc->AddMember(kSize, member, doc->GetAllocator());
  }

  // txId
  {
    rapidjson::Value member;
    member.SetUint64(obj->mTxId);
    doc->AddMember(kTxId, member, doc->GetAllocator());
  }

  // workerId
  {
    rapidjson::Value member;
    member.SetUint64(obj->mWorkerId);
    doc->AddMember(kWorkerId, member, doc->GetAllocator());
  }

  // prev_lsn_in_tx
  {
    rapidjson::Value member;
    member.SetUint64(obj->mPrevLSN);
    doc->AddMember(kPrevLsn, member, doc->GetAllocator());
  }

  // psn
  {
    rapidjson::Value member;
    member.SetUint64(obj->mGsn);
    doc->AddMember(kGsn, member, doc->GetAllocator());
  }

  // treeId
  {
    rapidjson::Value member;
    member.SetInt64(obj->mTreeId);
    doc->AddMember(kTreeId, member, doc->GetAllocator());
  }

  // pageId
  {
    rapidjson::Value member;
    member.SetUint64(obj->mPageId);
    doc->AddMember(kPageId, member, doc->GetAllocator());
  }
}

//! Convert BufferFrame to JSON
template <>
inline void ToJson(leanstore::storage::BufferFrame* obj, rapidjson::Value* doc,
                   rapidjson::Value::AllocatorType* allocator) {
  LS_DCHECK(doc->IsObject());

  // header
  rapidjson::Value headerObj(rapidjson::kObjectType);
  {
    // write the memory address of the buffer frame
    rapidjson::Value member;
    std::stringstream ss;
    ss << reinterpret_cast<void*>(obj);
    auto hexStr = ss.str();
    member.SetString(hexStr.data(), hexStr.size(), *allocator);
    headerObj.AddMember("mAddress", member, *allocator);
  }

  {
    auto stateStr = obj->mHeader.StateString();
    rapidjson::Value member;
    member.SetString(stateStr.data(), stateStr.size(), *allocator);
    headerObj.AddMember("mState", member, *allocator);
  }

  {
    rapidjson::Value member;
    member.SetBool(obj->mHeader.mKeepInMemory);
    headerObj.AddMember("mKeepInMemory", member, *allocator);
  }

  {
    rapidjson::Value member;
    member.SetUint64(obj->mHeader.mPageId);
    headerObj.AddMember("mPageId", member, *allocator);
  }

  {
    rapidjson::Value member;
    member.SetUint64(obj->mHeader.mLastWriterWorker);
    headerObj.AddMember("mLastWriterWorker", member, *allocator);
  }

  {
    rapidjson::Value member;
    member.SetUint64(obj->mHeader.mFlushedGsn);
    headerObj.AddMember("mFlushedGsn", member, *allocator);
  }

  {
    rapidjson::Value member;
    member.SetBool(obj->mHeader.mIsBeingWrittenBack);
    headerObj.AddMember("mIsBeingWrittenBack", member, *allocator);
  }

  doc->AddMember("header", headerObj, *allocator);

  // page without payload
  rapidjson::Value pageMetaObj(rapidjson::kObjectType);
  {
    rapidjson::Value member;
    member.SetUint64(obj->mPage.mGSN);
    pageMetaObj.AddMember("mGSN", member, *allocator);
  }
  {
    rapidjson::Value member;
    member.SetUint64(obj->mPage.mBTreeId);
    pageMetaObj.AddMember("mBTreeId", member, *allocator);
  }
  {
    rapidjson::Value member;
    member.SetUint64(obj->mPage.mMagicDebuging);
    pageMetaObj.AddMember("mMagicDebuging", member, *allocator);
  }
  doc->AddMember("pageWithoutPayload", pageMetaObj, *allocator);
}

//! Convert BufferFrame to JSON
template <>
inline void ToJson(leanstore::storage::btree::BTreeNode* obj, rapidjson::Value* doc,
                   rapidjson::Value::AllocatorType* allocator) {
  LS_DCHECK(doc->IsObject());

  auto lowerFence = obj->GetLowerFence();
  if (lowerFence.size() == 0) {
    AddMemberToJson(doc, *allocator, "mLowerFence", "-inf");
  } else {
    AddMemberToJson(doc, *allocator, "mLowerFence", lowerFence);
  }

  auto upperFence = obj->GetUpperFence();
  if (upperFence.size() == 0) {
    AddMemberToJson(doc, *allocator, "mUpperFence", "+inf");
  } else {
    AddMemberToJson(doc, *allocator, "mUpperFence", upperFence);
  }

  AddMemberToJson(doc, *allocator, "mNumSeps", obj->mNumSeps);
  AddMemberToJson(doc, *allocator, "mIsLeaf", obj->mIsLeaf);
  AddMemberToJson(doc, *allocator, "mSpaceUsed", obj->mSpaceUsed);
  AddMemberToJson(doc, *allocator, "mDataOffset", obj->mDataOffset);
  AddMemberToJson(doc, *allocator, "mPrefixSize", obj->mPrefixSize);

  // hints
  {
    rapidjson::Value memberArray(rapidjson::kArrayType);
    for (auto i = 0; i < leanstore::storage::btree::BTreeNode::sHintCount; ++i) {
      rapidjson::Value hintJson;
      hintJson.SetUint64(obj->mHint[i]);
      memberArray.PushBack(hintJson, *allocator);
    }
    doc->AddMember("mHints", memberArray, *allocator);
  }

  AddMemberToJson(doc, *allocator, "mHasGarbage", obj->mHasGarbage);

  // slots
  {
    rapidjson::Value memberArray(rapidjson::kArrayType);
    for (auto i = 0; i < obj->mNumSeps; ++i) {
      rapidjson::Value arrayElement(rapidjson::kObjectType);
      AddMemberToJson(&arrayElement, *allocator, "mOffset",
                      static_cast<uint64_t>(obj->mSlot[i].mOffset));
      AddMemberToJson(&arrayElement, *allocator, "mKeyLen",
                      static_cast<uint64_t>(obj->mSlot[i].mKeySizeWithoutPrefix));
      AddMemberToJson(&arrayElement, *allocator, "mKey", obj->KeyWithoutPrefix(i));
      AddMemberToJson(&arrayElement, *allocator, "mPayloadLen",
                      static_cast<uint64_t>(obj->mSlot[i].mValSize));
      AddMemberToJson(&arrayElement, *allocator, "mHead",
                      static_cast<uint64_t>(obj->mSlot[i].mHead));
      memberArray.PushBack(arrayElement, *allocator);
    }
    doc->AddMember("mSlots", memberArray, *allocator);
  }
}

//! Convert the object to a JSON string.
template <typename ObjType>
inline std::string ToJsonString(const ObjType* entry [[maybe_unused]]) {
  rapidjson::Document doc(rapidjson::kObjectType);
  ToJson(entry, &doc);

  rapidjson::StringBuffer buffer;
  rapidjson::Writer writer(buffer);
  doc.Accept(writer);
  return buffer.GetString();
}

} // namespace leanstore::utils