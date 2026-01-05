package spec

import lightdb.opensearch.OpenSearchStore

@EmbeddedTest
class OpenSearchFileStorageSpec extends AbstractFileStorageSpec(OpenSearchStore) with OpenSearchTestSupport



