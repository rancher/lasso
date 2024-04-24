/*
Package attachdriver provides a sqlite driver that wraps any other driver and attaches a database at a given path when a connection is opened.
*/
package attachdriver

import (
	"database/sql"
	"database/sql/driver"
	"fmt"

	"github.com/pkg/errors"
	"modernc.org/sqlite"
)

// AttachDriver uses an underlying driver to open the connection and attaches a database
// to that connection using path.
type AttachDriver struct {
	path string
	d    driver.Driver
}

// Register registers the attachdriver with the given path with go's sql package
func Register(path string) {
	d := &AttachDriver{
		path: path,
		d:    &sqlite.Driver{},
	}
	sql.Register("attach_sqlite", d)
}

// Open opens a connection from the underlying driver and
// attaches a database as db2 using the underlying path.
func (d AttachDriver) Open(name string) (driver.Conn, error) {
	c, err := d.d.Open(name)
	if err != nil {
		return nil, err
	}

	q := fmt.Sprintf(`ATTACH DATABASE '%s' AS db2;`, d.path)
	stmt, err := c.Prepare(q)
	if err != nil {
		return nil, closeConnOnError(c, err)
	}

	_, err = stmt.Exec(nil)
	if err != nil {
		stmt.Close()
		return nil, closeConnOnError(c, err)
	}
	stmt.Close()
	return c, nil
}

// closeConnOnError closes the sql.Rows object and wraps errors if needed
func closeConnOnError(c driver.Conn, err error) error {
	if err == nil {
		return nil
	}
	ce := c.Close()
	if ce != nil {
		if err == nil {
			return ce
		}
		return errors.Wrap(ce, "while handling "+err.Error())
	}

	return err
}