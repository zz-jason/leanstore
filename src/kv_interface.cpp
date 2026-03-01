#include "leanstore/kv_interface.hpp"

#include "leanstore/btree/basic_kv.hpp"
#include "leanstore/tx/transaction_kv.hpp"

namespace leanstore {

namespace {

template <typename Fn>
decltype(auto) VisitKV(const KVVariant& kv, Fn&& fn) {
  return std::visit([&](auto tree) { return fn(tree.get()); }, kv);
}

} // namespace

OpCode KVInterface::Insert(Slice key, Slice val) {
  return VisitKV(kv_, [&](auto& tree) { return tree.Insert(key, val); });
}

OpCode KVInterface::UpdatePartial(Slice key, MutValCallback update_call_back,
                                  UpdateDesc& update_desc) {
  return VisitKV(
      kv_, [&](auto& tree) { return tree.UpdatePartial(key, update_call_back, update_desc); });
}

OpCode KVInterface::Remove(Slice key) {
  return VisitKV(kv_, [&](auto& tree) { return tree.Remove(key); });
}

OpCode KVInterface::RangeRemove(Slice start_key, Slice end_key, bool page_wise) {
  return VisitKV(kv_, [&](auto& tree) { return tree.RangeRemove(start_key, end_key, page_wise); });
}

OpCode KVInterface::ScanAsc(Slice start_key, ScanCallback callback) {
  return VisitKV(kv_, [&](auto& tree) { return tree.ScanAsc(start_key, callback); });
}

OpCode KVInterface::ScanDesc(Slice start_key, ScanCallback callback) {
  return VisitKV(kv_, [&](auto& tree) { return tree.ScanDesc(start_key, callback); });
}

OpCode KVInterface::Lookup(Slice key, ValCallback val_callback) {
  return VisitKV(kv_, [&](auto& tree) { return tree.Lookup(key, val_callback); });
}

OpCode KVInterface::PrefixLookup(Slice key, PrefixLookupCallback callback) {
  return VisitKV(kv_, [&](auto& tree) { return tree.PrefixLookup(key, callback); });
}

OpCode KVInterface::PrefixLookupForPrev(Slice key, PrefixLookupCallback callback) {
  return VisitKV(kv_, [&](auto& tree) { return tree.PrefixLookupForPrev(key, callback); });
}

uint64_t KVInterface::CountEntries() {
  return VisitKV(kv_, [&](auto& tree) { return tree.CountEntries(); });
}

BasicKV* KVInterface::GetBasicKV() {
  auto* basic_kv = std::get_if<std::reference_wrapper<BasicKV>>(&kv_);
  return basic_kv == nullptr ? nullptr : &basic_kv->get();
}

TransactionKV* KVInterface::GetTransactionKV() {
  auto* tx_kv = std::get_if<std::reference_wrapper<TransactionKV>>(&kv_);
  return tx_kv == nullptr ? nullptr : &tx_kv->get();
}

} // namespace leanstore
