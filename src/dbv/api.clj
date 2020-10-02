(ns dbv.api
  (:require dbv.db
            dbv.db-util
            dbv.datoms
            dbv.search))

;; Concept:
;;
;; Provides the user of this library with all important functions to
;; work with dbv.

(def db
  dbv.db/db)

(def entid
  dbv.db-util/entid)

(def datoms
  dbv.datoms/datoms)

(def squuid
  dbv.db-util/squuid)
