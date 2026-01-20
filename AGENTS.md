# The LeanStore Project

LeanStore is a high-performance B-tree based key-value storage library optimized
for NVMe SSD and multi-core CPUs. Larger-than-memory database system with full
ACID transaction support.

## **MANDATORY** Agent Workflow

**For Every Task:**

- Write TODOs to `.AGENTS/${AGENT_NAME}_TODO.md`
- After task completion, write a brief summary to `.AGENTS/${AGENT_NAME}_SUMMARY.md`
- Write improvement suggestions for AGENTS.md to `.AGENTS/${AGENT_NAME}_SUGGESTION.md` (focus on efficiency, comfort, token reduction)
- Append newly gained knowledge to `.AGENTS/KNOWLEDGE_BASE.md`

**For Coding Task:**

- Run the `format` cmake target before commit
- Ensure all code builds and tests pass under the `debug_coro` preset
- Ensure `check-tidy` passes under the `debug_coro` preset (may require fixing existing files)
- Use conventional commit messages (see `.github/workflows/conventional-commits.yml`)

**For Coding Tasks:**

## Instructions

```sh
# using the debug_coro preset
cmake --preset debug_coro # configure the project
cmake --build build/debug_coro --target format # format the code
cmake --build build/debug_coro -j `nproc` # build the project
cmake --build build/debug_coro --target check-tidy # run clang-tidy checks
ctest --test-dir build/debug_coro --output-on-failure -j `nproc` # run the test cases
```

You can also use other cmake presets and cmake commands as needed.

## Knowledge Base

You can learn from the previously accumulated knowledge base in `.AGENTS/KNOWLEDGE_BASE.md`