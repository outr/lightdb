# lightdb
Prototype database concept using Lucene and HaloDB

First working release with minimal functionality in `0.1.0`

## TODO
- [ ] Support ObjectStore.all to iterate over the entire database
- [ ] Create backup and restore features
- [ ] Create an SBT plugin to update base traits for case classes (ex. Person would generate PersonFields trait to be mixed into Person companion)
- [ ] Create LuceneStore backed by a Lucene index with byte arrays - same index for searching, separate field
- [ ] Create benchmark tool to evaluate performance of basic operations to see how well this performs