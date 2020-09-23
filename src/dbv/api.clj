(ns dbv.api
  (:require dbv.db))

;; Concept:
;;
;; Provides the user of this library with all important functions to
;; work with dbv.

(def db
  dbv.db/db)

(def entid
  dbv.db/entid)
