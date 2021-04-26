# lightdb
Prototype database concept using Lucene and HaloDB

First working release with minimal functionality in `0.1.0`

## 0.2 TODO
- [X] Support ObjectStore.all to iterate over the entire database
- [ ] Create backup and restore features
    - [ ] Real-time backup (write changes to incremental file)
    - [ ] Complete dump and restore
    - [ ] Post-restore incremental restore
    - [ ] Testing of empty database loads from backups if available
- [ ] Data integrity checks
    - [ ] Verify data identical between store and index
    - [ ] Rebuild index from store

## 0.3 TODO
- [ ] Complete Lucene type support
- [ ] Create benchmark tool to evaluate performance of basic operations to see how well this performs
- [ ] Create an SBT plugin to update base traits for case classes (ex. Person would generate PersonFields trait to be mixed into Person companion)