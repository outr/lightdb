# 3.0 Plan
- [X] Remove Collection entirely and just use Store
- [X] Remove all name references to Collection
- [ ] Type-safe reference to Store implementation for Store-specific features in LightDB
- [ ] Separate Store into Store and Collection
  - the former just a key/value store and the latter providing search
  - separate trait for aggregation support