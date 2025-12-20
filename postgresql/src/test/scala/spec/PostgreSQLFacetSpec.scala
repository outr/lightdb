package spec
import lightdb.postgresql.PostgreSQLStore
import lightdb.store.CollectionManager

@EmbeddedTest
class PostgreSQLFacetSpec extends AbstractFacetSpec {
  override def storeManager: CollectionManager = PostgreSQLStore
}
