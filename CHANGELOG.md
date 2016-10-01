# Changes By Release

## 0.3.1
* Fixes issue where secondary indexes were not added if the table already existed.
* Updates `rethinkdb`: `2.3.2` -> `2.3.3` 

## 0.3.0
* Changes to use a `connection` object that's passed directly to r.connect instead of having `db` and `host` as top-level keys. Copies over `db` and `host` to remain backwards compatible.

## 0.2.3
* Fix bug where db was not being created when using #setup.

## 0.2.2
* Updates `rethinkdb`: `2.3.1` -> `2.3.2`

## 0.2.1
* Adds `host` option to the config.

## 0.2.0
* Updates `rethinkdb`: `2.2.3` -> `2.3.1` 

_Note_: This version no longer supports RethinkDB v2.2, only v2.3 and up
