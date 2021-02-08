# lightdb
Prototype database concept using Lucene and HaloDB

Still in the prototype stages, but shows promise

## TODO
- [ ] Create an SBT plugin to update base traits for case classes (ex. Person would generate PersonFields trait to be mixed into Person companion)
- [ ] Create indexing, filtering, etc. features in companion object
- [ ] Implement very basic Lucene integration
- [ ] Create LuceneStore backed by a Lucene index with byte arrays - same index for searching, separate field
- [ ] Create benchmark tool to evaluate performance of basic operations to see how well this performs