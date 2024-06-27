package benchmark

import fabric.rw.RW
import lightdb.{Id, LightDB, StoredValue}
import lightdb.document.{Document, DocumentModel}
import lightdb.duckdb.DuckDBIndexer
import lightdb.h2.H2Indexer
import lightdb.halo.HaloDBStore
import lightdb.index.{Indexed, IndexedCollection}
import lightdb.lucene.LuceneIndexer
import lightdb.sqlite.SQLiteIndexer
import lightdb.store.{AtomicMapStore, StoreManager}
import lightdb.transaction.Transaction
import lightdb.upgrade.DatabaseUpgrade
import lightdb.util.Unique
import org.apache.commons.io.FileUtils
import perfolation.double2Implicits

import java.io.File
import java.nio.file.Path
import java.time.Duration
import scala.util.Random
import scala.collection.parallel.CollectionConverters._
import scala.collection.parallel.immutable.ParRange

object TightLoops {
  private val RecordCount = 1_000_000
  private val CountIterations = 100_000
  private val StreamIterations = 1
  private val SearchIterations = 1

  private val AnimalNames = Vector(
    "Aardvark", "Aardwolf", "African buffalo", "African elephant", "African leopard", "African Tree Pangolin", "Albatross", "Alligator", "Alpaca", "American robin", "Anaconda", "Angel Fish", "Angelfish", "Anglerfish", "Ant", "Anteater", "Antelope", "Antlion", "Ape", "Aphid", "Arab horse", "Arabian leopard", "Archer Fish", "Arctic Fox", "Arctic Wolf", "Armadillo", "Arrow crab", "Asian Elephant", "Asp", "Atlantic Puffin", "Aye-Aye", "Baboon", "Badger", "Bald eagle", "Bandicoot", "Bangle Tiger", "Barnacle", "Barracuda", "Basilisk", "Bass", "Basset Hound", "Bat", "Beaked whale", "Bear", "Bearded Dragon", "Beaver", "Bedbug", "Bee", "Beetle", "Beluga Whale", "Big-horned sheep", "Billy goat", "Bird", "Bird of paradise", "Bison", "Black Bear", "Black Fly", "Black Footed Rhino", "Black panther", "Black Rhino", "Black widow spider", "Blackbird", "Blowfish", "Blue bird", "Blue jay", "Blue whale", "Boa", "Boar", "Bob-Cat", "Bobcat", "Bobolink", "Bonobo", "Booby", "Border Collie", "Bornean Orang-utan", "Bottle-Nose dolphin", "Bovid", "Box jellyfish", "Boxer dog", "Brown Bear", "Buck", "Budgie", "Buffalo", "Bug", "Bull", "Bull frog", "Bull Mastiff", "Butterfly", "Buzzard", "Caiman lizard", "Camel", "Canary", "Cape buffalo", "Capybara", "Cardinal", "Caribou", "Carp", "Cat", "Caterpillar", "Catfish", "Catshark", "Cattle", "Centipede", "Cephalopod", "Chameleon", "Cheetah", "Chickadee", "Chicken", "Chihuahua", "Chimpanzee", "Chinchilla", "Chipmunk", "Chupacabra", "Cicada", "Clam", "Clown Fish", "Clownfish", "Cobra", "Cockatiel", "Cockatoo", "Cocker Spaniel", "Cockroach", "Cod", "Coho", "Common Dolphin", "Common seal", "Condor", "Constrictor", "Coral", "Corn Snake", "Cougar", "Cow", "Coyote", "Crab", "Crane", "Crane fly", "Crawdad", "Crawfish", "Cray fish", "Crayfish", "Cricket", "Crocodile", "Crow", "Cuckoo", "Cuckoo bird", "Cuttle fish", "Dacshund", "Dalmation", "Damsel fly", "Damselfly", "Dart Frog", "Deer", "Devi Fish (Giant Sting ray)", "Diamond back rattler", "Dik-dik", "Dingo", "Dinosaur", "Doberman Pinscher", "Dodo bird", "Dog", "Dolly Varden", "Dolphin", "Donkey", "Door mouse", "Dormouse", "Dove", "Draft horse", "Dragon", "Dragonfly", "Drake", "Du-gong", "Duck", "Duckbill Platypus", "Dung beetle", "Eagle", "Earthworm", "Earwig", "Echidna", "Eclectus", "Eel", "Egret", "Elephant", "Elephant seal", "Elk", "Emu", "English pointer", "Ermine", "Erne", "Eurasian Lynx", "Falcon", "Ferret", "Finch", "Firefly", "Fish", "Flamingo", "Flatworm", "Flea", "Fly", "Flyingfish", "Fowl", "Fox", "Fresh Water Crocodile", "Frog", "Fruit bat", "Galapagos Land Iguana", "Galapagos Tortoise", "Galliform", "Gamefowl", "Gazelle", "Gecko", "Gerbil", "Giant Anteater", "Giant panda", "Giant squid", "Gibbon", "Gila monster", "Giraffe", "Gnat", "Goat", "Goldfish", "Goose", "Gopher", "Gorilla", "Grasshopper", "Great blue heron", "Great white shark", "Green fly", "Green poison dart frog", "Green Sea Turtle", "Grey Whale", "Grizzly bear", "Ground shark", "Ground sloth", "Groundhog", "Grouse", "Guan", "Guanaco", "Guinea pig", "Guineafowl", "Gull", "Guppy", "Haddock", "Halibut", "Hammerhead shark", "Hamster", "Hare", "Harrier", "Hawk", "Hedgehog", "Hermit crab", "Heron", "Herring", "Hippopotamus", "Hookworm", "Hornet", "Horse", "Hoverfly", "Hummingbird", "Humpback whale", "Hyena", "Hyrax", "Iguana", "Iguanodon", "Impala", "Inchworm", "Insect", "Irrawaddy Dolphin", "Irukandji jellyfish", "Jackal", "Jackrabbit", "Jaguar", "Jay", "Jellyfish", "June bug", "Junglefowl", "Kangaroo", "Kangaroo mouse", "Kangaroo rat", "Killer Whale", "King Cobra", "Kingfisher", "Kite", "Kiwi", "Koala", "Koi", "Komodo dragon", "Kookaburra", "Krill", "Ladybug", "Lama", "Lamb", "Lamprey", "Lancelet", "Land snail", "Lark", "Leatherback sea turtle", "Leech", "Lemming", "Lemur", "Leopard", "Leopon", "Lice", "Limpet", "Lion", "Lionfish", "Lizard", "Llama", "Lobster", "Locust", "Loon", "Louse", "Lungfish", "Lynx", "Macaw", "Mackerel", "Magpie", "Man-Of-War", "Manatee", "Mandrill", "Manta ray", "Mantis", "Marlin", "Marmoset", "Marmot", "Marsupials", "Mastodon", "Meadowlark", "Meerkat", "Mink", "Minnow", "Mite", "Mockingbird", "Mole", "Mollusk", "Mollusks", "Monarch Butterfly", "Mongoose", "Monitor lizard", "Monkey", "Moose", "Mosquito", "Moth", "Mountain goat", "Mountain Lion", "Mouse", "Mule", "Muskox", "Muskrat", "Naked Mole Rat", "Narwhal", "Nautilus", "Newt", "Nightingale", "Ocelot", "Octopus", "Opossum", "Orangutan", "Orca", "Osprey", "Ostrich", "Otter", "Owl", "Ox", "Panda", "Panther", "Panthera hybrid", "Parakeet", "Parrot", "Parrotfish", "Partridge", "Peacock", "Peafowl", "Pelican", "Penguin", "Perch", "Peregrine falcon", "Pheasant", "Pig", "Pigeon", "Pike", "Pilot whale", "Pinniped", "Piranha", "Planarian", "Platypus", "Polar bear", "Pony", "Porcupine", "Porpoise", "Possum", "Prairie dog", "Prawn", "Praying mantis", "Ptarmigan", "Puffin", "Puma", "Python", "Quail", "Quelea", "Quetzal", "Quokka", "Rabbit", "Raccoon", "Rainbow trout", "Rat", "Rattlesnake", "Raven", "Ray", "Red panda", "Reindeer", "Reptile", "Rhino", "Rhinoceros", "Right whale", "Ringworm", "Roadrunner", "Robin", "Rodent", "Rook", "Rooster", "Roundworm", "Saber-toothed cat", "Sailfish", "Salamander", "Salmon", "Salt water alligator", "Sandpiper", "Sawfish", "Scallop", "Scorpion", "Sea anemone", "Sea lion", "Sea slug", "Sea snail", "Sea urchin", "Seahorse", "Seal", "Shark", "Sheep", "Shrew", "Shrimp", "Siberian Husky", "Siberian Tiger", "Skink", "Skunk", "Skunks", "Slender Loris", "Sloth", "Sloth bear", "Slug", "Slugs", "Smelt", "Snail", "Snails", "Snake", "Snipe", "Snow Fox", "Snow Hare", "Snow leopard", "Sockeye salmon", "Sole", "Somali Wild Ass", "Sparrow", "Spectacled Bear", "Sperm whale", "Spider", "Spider monkey", "Sponge", "Spoonbill", "Squid", "Squirrel", "Star-nosed mole", "Starfish", "Steelhead trout", "Stingray", "Stoat", "Stork", "Sturgeon", "Sugar glider", "Swallow", "Swan", "Swift", "Swordfish", "Swordtail", "Tadpole", "Tahr", "Takin", "Tamarin", "Tapeworm", "Tapir", "Tarantula", "Tarpan", "Tarsier", "Tasmanian devil", "Tazmanian devil", "Tazmanian tiger", "Termite", "Tern", "Terrapin", "Thrush", "Tick", "Tiger", "Tiger shark", "Tiglon", "Toad", "Tortoise", "Toucan", "Trapdoor spider", "Tree frog", "Trout", "Tuna", "Turkey", "Turtle", "Tyrannosaurus", "Uakari", "Umbrella bird", "Urchin", "Urial", "Urutu", "Vampire bat", "Vampire squid", "Velociraptor", "Velvet worm", "Vervet", "Vicuna", "Viper", "Viper Fish", "Vole", "Vulture", "Wallaby", "Walrus", "Warbler", "Warthog", "Wasp", "Water buffalo", "Water Dragons", "Weasel", "Weevil", "Whale", "Whale Shark", "Whippet", "White Rhino", "White tailed dear", "Whitefish", "Whooper", "Whooping crane", "Widow Spider", "Wildcat", "Wildebeest", "Wolf", "Wolf Spider", "Wolverine", "Wombat", "Woodchuck", "Woodpecker", "Wren", "X-ray fish", "Xerinae", "Yak", "Yellow Bellied Marmot", "Yellow belly sapsucker", "Yellow finned tuna", "Yellow perch", "Yeti", "Yorkshire terrier", "Zander", "Zebra", "Zebra Dove", "Zebra finch", "Zebu", "Zorilla"
  )

  private def animal(): String = AnimalNames(Random.nextInt(AnimalNames.size))

  def elapsed(f: => Unit): String = {
    val start = System.currentTimeMillis()
    f
    val end = System.currentTimeMillis()
    ((end - start) / 1000.0).f(f = 3)
  }

  def main(args: Array[String]): Unit = {
    val totalTime = elapsed {
      val dbDir = new File("db")
      FileUtils.deleteDirectory(dbDir)
      dbDir.mkdirs()

      DB.init()
      DB.people.transaction { implicit transaction =>
        val insertTime = elapsed(insertRecords())
        scribe.info(s"Inserted in $insertTime")
        val countTime = elapsed {
          val count = countRecords()
          scribe.info(s"Inserted $count records")
        }
        transaction.commit()
        val countIndexesTime = elapsed {
          val count = countIndexes()
          scribe.info(s"Indexed $count records")
        }
        scribe.info(s"Counted in $countTime / $countIndexesTime")
        val streamTime = elapsed {
          streamRecords()
        }
        scribe.info(s"Streamed in $streamTime")
        val searchTime = elapsed {
          searchRecords()
        }
        scribe.info(s"Searched in $searchTime")
        val searchCountTime = elapsed {
          val count = searchAndCountRecords()
          scribe.info(s"Search Count is $count")
        }
        scribe.info(s"Searched Counted $searchCountTime")
      }
      DB.dispose()
    }
    scribe.info(s"Completed in $totalTime")
    sys.exit(0)
  }

  def insertRecords()(implicit transaction: Transaction[Person]): Unit = {
    var count = 0
    (0 until RecordCount)
      .iterator
      .foreach { index =>
        DB.people.set(Person(
          name = Unique(),
          age = index,
          tags = Set(animal(), animal(), animal())
        ))
        count += 1
      }
    scribe.info(s"Inserted $count records")
  }

  def countRecords()(implicit transaction: Transaction[Person]): Int = {
    var count = -1
    (0 until CountIterations)
      .iterator
      .foreach(_ => count = DB.people.count)
    count
  }

  def countIndexes()(implicit transaction: Transaction[Person]): Int = {
    var count = -1
    (0 until CountIterations)
      .iterator
      .foreach(_ => count = DB.people.indexer.count)
    count
  }

  def streamRecords()(implicit transaction: Transaction[Person]): Unit = ParRange(0, StreamIterations, 1, inclusive = false)
    .foreach { _ =>
      val count = DB.people.iterator.size
      scribe.info(s"Streamed: $count")
    }

  def searchRecords()(implicit transaction: Transaction[Person]): Unit = Range(0, SearchIterations, 1)
    .foreach { _ =>
      ParRange(0, RecordCount, 1, inclusive = false)
        .foreach { age =>
          val list = DB.people.query.filter(_.age === age).search.docs.iterator.toList
          if (list.size != 1) {
            scribe.warn(s"Unable to find age = $age")
          }
          if (list.head.age != age) {
            scribe.warn(s"${list.head.age} was not $age")
          }
        }
    }

  def searchAndCountRecords()(implicit transaction: Transaction[Person]): Int = DB.people.query
    .search
    .docs
    .iterator
    .foldLeft(0) { (count, _) =>
      count + 1
    }

  object DB extends LightDB {
    override lazy val directory: Option[Path] = Some(Path.of(s"db/tightLoops"))

    val people: IndexedCollection[Person, Person.type] = collection("people", Person, H2Indexer())

    override def storeManager: StoreManager = HaloDBStore

    override def upgrades: List[DatabaseUpgrade] = Nil
  }

  case class Person(name: String,
                    age: Int,
                    tags: Set[String],
                    _id: Id[Person] = Person.id()) extends Document[Person]

  object Person extends DocumentModel[Person] with Indexed[Person] {
    implicit val rw: RW[Person] = RW.gen

    val name: I[String] = index.one("name", _.name, store = true)
    val age: I[Int] = index.one("age", _.age, store = true)
    val tag: I[String] = index("tag", _.tags.toList)
  }
}
