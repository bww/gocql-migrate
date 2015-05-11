// 
// GoCQL Migrate
// Copyright (c) 2015 Mess, All rights reserved.
// 
// Developed by Mess in New York City
// http://thisismess.com/
// 

package migrate

import (
  "fmt"
  "log"
)

import (
  "github.com/gocql/gocql"
)

/**
 * Determine if a slice contains an element
 */
func contains(slice []string, element string) int {
  for i, e := range slice {
    if element == e {
      return i
    }
  }
  return -1
}

/**
 * Determine if a slice contains an element
 */
func containsKey(slice []Key, element Key) int {
  for i, e := range slice {
    if element == e {
      return i
    }
  }
  return -1
}

/**
 * Obtain map keys
 */
func mapkeys(from map[string]ColumnFamily) []string {
  keys := make([]string, len(from))
  i := 0
  for k, _ := range from {
    keys[i] = k
    i++
  }
  return keys
}

/**
 * Obtain column names
 */
func colnames(from map[string]Column) []string {
  keys := make([]string, len(from))
  i := 0
  for k, _ := range from {
    keys[i] = k
    i++
  }
  return keys
}

/**
 * Argument list
 */
func arglist(a []string) string {
  var l string
  for i, e := range a {
    if i > 0 {
      l += fmt.Sprintf(", %s", e)
    }else{
      l += e
    }
  }
  return l
}

/**
 * Key list (quoted)
 */
func keylist(k []Key) string {
  var l string
  for i, e := range k {
    if i > 0 {
      l += fmt.Sprintf(", %q", e)
    }else{
      l += fmt.Sprintf("%q", e)
    }
  }
  return l
}

/**
 * Column
 */
type Column struct {
  Name          string
  CType         string
  PKey          bool
}

/**
 * Order
 */
type Order struct {
  Name          string
  Direction     string
}

/**
 * Partition
 */
type Key string

/**
 * Column family
 */
type ColumnFamily struct {
  Columns       []Column
  Ordering      []Order
  Partitioning  []Key
}

/**
 * Columns by name
 */
func (c *ColumnFamily) columnsByName() map[string]Column {
  cols := make(map[string]Column)
  for _, e := range c.Columns {
    cols[e.Name] = e
  }
  return cols
}

/**
 * Obtain CQL to create this column family
 */
func (c *ColumnFamily) create(cf string) (string, error) {
  cql := fmt.Sprintf("CREATE COLUMNFAMILY %s (", cf)
  pkey := make([]Column, 0)
  
  if c.Columns == nil || len(c.Columns) < 1 {
    return "", fmt.Errorf("No columns defined")
  }
  
  i := 0
  for _, col := range c.Columns {
    if i > 0 { cql += ", " }
    cql += fmt.Sprintf("%q %s", col.Name, col.CType)
    if col.PKey {
      pkey = append(pkey, col)
    }
    i++
  }
  
  if len(pkey) < 1 {
    return "", fmt.Errorf("Column family defines no primary keys: %v", cf)
  }
  
  if i > 0 { cql += ", " }
  cql += "PRIMARY KEY ("
  
  i = 0
  for _, col := range pkey {
    if c.Partitioning != nil && len(c.Partitioning) > 1 {
      if i == 0 {
        cql += fmt.Sprintf("(%s)", keylist(c.Partitioning))
      }else if containsKey(c.Partitioning, Key(col.Name)) < 0 {
        cql += fmt.Sprintf(", %q", col.Name)
      }
    }else{
      if i > 0 { cql += ", " }
      cql += fmt.Sprintf("%q", col.Name)
    }
    i++
  }
  
  cql += "))"
  
  if c.Ordering != nil && len(c.Ordering) > 0 {
    cql += " WITH CLUSTERING ORDER BY ("
    
    i = 0
    for _, ord := range c.Ordering {
      if i > 0 { cql += ", " }
      cql += fmt.Sprintf("%q %s", ord.Name, ord.Direction)
      i++
    }
    
    cql += ")"
  }
  
  return cql, nil
}

/**
 * A keyspace
 */
type Keyspace map[string]ColumnFamily

/**
 * Setup a keyspace as necessary
 */
func (k Keyspace) Migrate(ksname string, c *gocql.Session) error {
  var iter *gocql.Iter
  var name string
  
  missing := mapkeys(k)
  present := make([]string, 0)
  
  iter = c.Query("SELECT columnfamily_name FROM System.schema_columnfamilies WHERE keyspace_name=?", ksname).Iter()
  for iter.Scan(&name) {
    present = append(present, name)
    if i := contains(missing, name); i >= 0 {
      missing = append(missing[:i], missing[i+1:]...)
    }
  }
  if err := iter.Close(); err != nil {
    panic(fmt.Errorf("Could not query column families: %v", err))
  }
  
  if(len(missing) > 0){
    log.Printf("[%s] creating column families: %v", ksname, missing)
  }else{
    log.Printf("[%s] keyspace is up-to-date", ksname)
  }
  
  for _, key := range missing {
    
    cf := k[key]
    query, err := cf.create(key)
    if err != nil {
      return err
    }
    
    log.Printf("[%s] %s", ksname, query)
    if err := c.Query(query).Exec(); err != nil {
      return err
    }
    
  }
  
  for _, key := range present {
    if cf, ok := k[key]; ok {
      columns := cf.columnsByName()
      
      iter = c.Query("SELECT column_name FROM system.schema_columns WHERE keyspace_name = ? and columnfamily_name = ? allow filtering", ksname, key).Iter()
      for iter.Scan(&name) {
        if _, ok := columns[name]; ok {
          delete(columns, name)
        }
      }
      if err := iter.Close(); err != nil {
        return fmt.Errorf("Could not query column names: %v", err)
      }
      
      if len(columns) > 0 {
        log.Printf("[%s] [%s] missing columns: %+v", ksname, key, colnames(columns))
        
        for _, e := range columns {
          log.Printf("[%s] [%s] adding column: %s.%q (%s)", ksname, key, key, e.Name, e.CType)
          query := fmt.Sprintf("ALTER TABLE %s ADD %q %s", key, e.Name, e.CType)
          if err := c.Query(query).Exec(); err != nil {
            return err
          }
        }
        
        log.Printf("[%s] [%s] updated", ksname, key)
      }
      
    }
  }
  
  return nil
}

