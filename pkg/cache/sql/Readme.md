# SQL Cache

## Sections
- [ListOptions Informer](#listoptions-informer)
  - [List Options](#list-options)
  - [ListOption Indexer](#listoptions-indexer)
    - [SQL Store](#sql-store)
    - [Partitions](#partitions)
- [How to Use](#how-to-use)
- [Technical Information](#technical-information)
  - [SQL Tables](#sql-tables)
  - [Attach Driver](#attach-driver)
  - [SQLite Driver](#sqlite-driver)
  - [Connection Pooling](#connection-pooling)
  - [Encryption Defaults](#encryption-defaults)
  - [Indexed Fields](#indexed-fields)
  - [ListOptions Behavior](#listoptions-behavior)
  - [Troubleshooting Sqlite](#troubleshooting-sqlite)



## ListOptions Informer
The main usable feature from the SQL cache is the ListOptions Informer. The ListOptionsInformer provides listing functionality,
like any other informer, but with a wider array of options. The options are configured by informer.ListOptions.

### List Options
ListOptions includes the following:
* Match filters for indexed fields. Filters are for specifying the value a given field in an object should be in order to
be included in the list. Filters can be set to equals or not equals. Filters can be set to look for partial matches or
exact (strict) matches. Filters can be OR'd and AND'd with one another. Filters only work on fields that have been indexed.
* Primary field and secondary field sorting order. Can choose up to two fields to sort on. Sort order can be ascending
or descending. Default sorting is to sort on metadata.namespace in ascending first and then sort on metadata.name.
* Page size to specify how many items to include in a response.
* Page number to specify offset. For example, a page size of 50 and a page number of 2, will return items starting at
index 50. Index will be dependent on sort. Page numbers start at 1.

### ListOptions Factory
The ListOptions Factory helps manage multiple ListOption Informers. A user can call Factory.InformerFor(), to create new
ListOptions informers if they do not exist and retrieve existing ones.

### ListOptions Indexer
Like all other informers, the ListOptions informer uses an indexer to cache objects of the informer's type. A few features
set the ListOptions Indexer apart from others indexers:
* an on-disk store instead of an in-memory store.
* accepts list options backed by SQL queries for extended search/filter/sorting capability.
* AES GCM encryption using key hierarchy.

### SQL Store
The SQL store is the main interface for interacting with the database. This store backs the indexer, and provides all
functionality required by the cache.Store interface.

### Partitions
Partitions are constraints for ListOptionsInform ListByOptions() method that are separate from ListOptions. Partitions
are strict conditions that dictate which namespaces or names can be searched from. These overrule ListOptions and are
intended to be used as a way of enforcing RBAC.

## How to Use
```go
    package main
    import(
		"k8s.io/client-go/dynamic"
        "github.com/rancher/lasso/pkg/cache/sql/informer"
		"github.com/rancher/lasso/pkg/cache/sql/informer/factory"
    )

    func main() {
		cacheFactory, err := factory.NewCacheFactory()
		if err != nil {
			panic(err)
        }
		// config should be some rest config created from kubeconfig
		// there are other ways to create a config and any client that conforms to k8s.io/client-go/dynamic.ResourceInterface
		// will work.
		client, err := dynamic.NewForConfig(config)
		if err != nil {
			panic(err)
		}

		fields := [][]string{{"metadata", "name"}, {"metadata", "namespace"}}
		opts := &informer.ListOptions{}
		// gvk should be of type k8s.io/apimachinery/pkg/runtime/schema.GroupVersionKind
		c, err := cacheFactory.CacheFor(fields, client, gvk)
		if err != nil {
			panic(err)
		}

		// continueToken will just be an offset that can be used in Resume on a subsequent request to continue
		// to next page
		list, continueToken, err := c.ListByOptions(apiOp.Context(), opts, partitions, namespace)
		if err != nil {
			panic(err)
		}
	}
```

## Technical Information

### SQL Tables
There are three tables that are created for the ListOption informer:
* object table - this contains objects, including all their fields, as blobs. These blobs may be encrypted.
* fields table - this contains specific fields of value for objects. These are specified on informer create and are fields
that it is desired to filter or order on.
* indices table - the indices table stores indexes created and objects' values for each index. This backs the generic indexer
that contains the functionality needed to conform to cache.Indexer.

### Attach Driver
The fields tables may contain sensitive information, but because we need to use its values for indexing it cannot be encrypted.
The attach driver automatically attaches a second database on every real sqlite connection. The database is able to exist
in memory while the primary object database in on-disk. The attach allows queries to be made to either in a single connection.
An example of a query using both databases:
```sqlite
JOIN db2."secrets_fields" f ON o.key = f.key
```

### SQLite Driver
There are multiple SQLite drivers that this package could have used. One of the most, if not the most, popular SQLite golang
drivers is [mattn/go-sqlite3](https://github.com/mattn/go-sqlite3). This driver is not being used because it requires enabling
the cgo option when compiling and at the moment lasso's main consumer, rancher, does not compile with cgo. We did not want
the SQL informer to be the sole driver in switching to using cgo. Instead, modernc's driver which is in pure golang. Side-by-side
comparisons can be found indicating the cgo version is, as expected, more performant. If in the future it is deemed worthwhile
then the driver can be easily switched following these steps:
1. Replace empty import in `pkg/cache/sql/store`. Change `_ "modernc.org/sqlite"` to `_ "github.com/mattn/go-sqlite3"`.
2. In `attachdriver` package, register `SQLDriver` struct from `mattn/gosqlite3` instead of `Driver` from `modernc.org/sqlite`.

### Connection Pooling
While working with the `database/sql` package for go, it is important to understand how sql.Open() and other methods manage
connections. Open starts a connection pool; that is to say after calling open once, there may be anywhere from zero to many
connections attached to a sql.Connection. `database/sql` manages this connection pool under the hood. In most cases, an
application only need one sql.Connection, although sometimes application use two: one for writes, the other for reads. To
read more about the `sql` package's connection pooling read [Managing connections](https://go.dev/doc/database/manage-connections).

The use of connection pooling and the fact that lasso potentially has many go routines accessing the same connection pool,
means we have to be careful with writes. Exclusively using sql transaction to write helps ensure safety. To read more about
sql transactions read SQLite's [Transaction docs](https://www.sqlite.org/lang_transaction.html).

### Encryption Defaults
By default only specified types are encrypted. These types are hard-coded and defined by defaultEncryptedResourceTypes
in `pkg/cache/sql/informer/factory/informer_factory.go`. To enabled encryption for all types, set the ENV variable
`CATTLE_ENCRYPT_CACHE_ALL` to "true".

The key size used is 256 bits. Data-encryption-keys are stored in the object table and are rotated every 150,000 writes.

### Indexed Fields
Filtering and sorting only work on indexed fields. These fields are defined when using `CacheFor`. Objects will
have the following indexes by default:
* Fields in informer.defaultIndexedFields
* Fields passed to InformerFor()

### ListOptions Behavior
Defaults:
* Sort.PrimaryField: `metadata.namespace`
* Sort.SecondaryField: `metadata.name`
* Sort.PrimaryOrder: `ASC` (ascending)
* Sort.SecondaryOrder: `ASC` (ascending)
* All filters have partial matching set to false by default

There are some uncommon ways someone could use ListOptions where it would be difficult to predict what the result would be.
Below is a non-exhaustive list of some of these cases and what the behavior is:
* Setting Pagination.Page but not Pagination.PageSize will cause Page to be ignored
* Setting Sort.SecondaryField only will sort as though it was Sort.PrimaryField. Sort.SecondaryOrder will still be applied
and Sort.PrimaryOrder will be ignored

### Writing Secure Queries
Values should be supplied to SQL queries using placeholders, read [Avoiding SQL Injection Risk](https://go.dev/doc/database/sql-injection). Any other portions
of a query that may be user supplied, such as columns, should be carefully validated against a fixed set of acceptable values.

### Troubleshooting SQLite
A useful tool for troubleshooting the database files is the sqlite command line tool. Another useful tool is the goland
sqlite plugin. Both of these tools can be used with the database files. If running a database in-memory, there is no option
for navigating records. In-memory databases are used for the non-object database when `CATTLE_ENCRYPT_CACHE_ALL` is set to
"true".
