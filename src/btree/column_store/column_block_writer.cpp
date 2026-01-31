#include "leanstore/base/error.hpp"
#include "leanstore/btree/column_store/column_store.hpp"
#include "leanstore/buffer/buffer_manager.hpp"
#include "leanstore/lean_store.hpp"

#include <algorithm>
#include <cstring>
#include <vector>

namespace leanstore::column_store {

Result<ColumnBlockRef> WriteColumnBlock(LeanStore* store, lean_treeid_t tree_id,
                                        const std::vector<uint8_t>& block_bytes,
                                        uint32_t row_count) {
  const auto page_size = store->store_option_->page_size_;
  const uint32_t payload_capacity =
      static_cast<uint32_t>(page_size - sizeof(Page) - sizeof(ColumnPageHeader));
  if (payload_capacity == 0) {
    return Error::General("page payload too small for column block");
  }

  const uint32_t page_count =
      static_cast<uint32_t>((block_bytes.size() + payload_capacity - 1) / payload_capacity);
  if (page_count == 0) {
    return Error::General("empty column block");
  }

  std::vector<lean_pid_t> page_ids;
  page_ids.reserve(page_count);
  std::vector<BufferFrame*> frames;
  frames.reserve(page_count);
  for (uint32_t i = 0; i < page_count; ++i) {
    auto& bf = store->buffer_manager_->AllocNewPage(tree_id);
    frames.push_back(&bf);
    page_ids.push_back(bf.header_.page_id_);
  }

  for (uint32_t i = 0; i < page_count; ++i) {
    auto& bf = *frames[i];
    const uint32_t offset = i * payload_capacity;
    const uint32_t remaining = static_cast<uint32_t>(block_bytes.size() - offset);
    const uint32_t to_copy = std::min(payload_capacity, remaining);

    bf.page_.magic_debugging_ = kColumnPageMagic;
    auto* header = reinterpret_cast<ColumnPageHeader*>(bf.page_.payload_);
    header->magic_ = kColumnPageHeaderMagic;
    header->payload_bytes_ = to_copy;
    header->block_offset_ = offset;
    // Link pages so ReadColumnBlock can stream the payload.
    header->next_page_id_ = (i + 1 < page_count) ? page_ids[i + 1] : 0;

    std::memcpy(bf.page_.payload_ + sizeof(ColumnPageHeader), block_bytes.data() + offset, to_copy);

    if (auto res = store->buffer_manager_->WritePageSync(bf); !res) {
      return std::move(res.error());
    }
  }

  ColumnBlockRef ref{};
  ref.magic_ = kColumnBlockRefMagic;
  ref.version_ = kColumnBlockVersion;
  ref.first_page_id_ = page_ids.front();
  ref.page_count_ = page_count;
  ref.block_bytes_ = static_cast<uint32_t>(block_bytes.size());
  ref.row_count_ = row_count;
  return ref;
}

} // namespace leanstore::column_store
