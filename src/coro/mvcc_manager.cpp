#include "leanstore/coro/mvcc_manager.hpp"

#include "utils/json.hpp"

namespace leanstore {

utils::JsonObj MvccManager::Serialize() const {
  utils::JsonObj json_obj;
  json_obj.AddUint64(kKeyGlobalUsrTso, GetUsrTxTs());
  json_obj.AddUint64(kKeyGlobalSysTso, GetSysTxTs());
  return json_obj;
}

void MvccManager::Deserialize(const utils::JsonObj& json_obj) {
  auto usr_tx = *json_obj.GetUint64(kKeyGlobalUsrTso);
  auto sys_tx = *json_obj.GetUint64(kKeyGlobalSysTso);
  usr_tso_.store(usr_tx);
  sys_tso_.store(sys_tx);
  global_wmk_info_.wmk_of_all_tx_ = usr_tx;

  for (auto& logging : loggings_) {
    logging->SetLastHardenedUsrTx(usr_tx);
    logging->SetLastHardenedSysTx(sys_tx);
  }
}

} // namespace leanstore
