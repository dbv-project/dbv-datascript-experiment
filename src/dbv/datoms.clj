(ns dbv.datoms
  (:require [next.jdbc :as jdbc]
            [clojure.string :as str]
            [next.jdbc.protocols :as p]
            [datascript.db :as datascript-db]
            [dbv.db :as db]
            [dbv.db-util :as db-util]
            ))

(extend-protocol p/Connectable
  java.sql.Connection
  (get-connection [this opts] this))

(defn extract-datom
  [db result-set]
  (let [a (.getLong result-set
                    "a")]
    (datascript-db/datom (.getLong result-set
                                   "e")
                         (:ident (db-util/attribute db
                                                    a))
                         (.getObject result-set
                                     (db-util/column-name db
                                                          a))
                         (.getLong result-set
                                   "tx")
                         true
                         ;; TODO: handle retract
                         #_(= (.getLong result-set
                                        "rx")
                              Long/MAX_VALUE))))

(defn datoms-seq
  [db result-set]
  (when (.next result-set)
    (lazy-seq
     (cons (extract-datom db
                          result-set)
           (datoms-seq db result-set)))
    ))

(defn- datoms-select-sql
  [db]
  (str "select e,a,"
       (str/join "," (map name (:value-columns db)))
       ",rx,tx"
       " from " (:table db)
       " "))

(defn- datoms-eavt
  [db index & components]
  (let [connection (doto (jdbc/get-connection (:connectable db))
                     (.setAutoCommit false))]
    (let [statement (doto (.createStatement connection)
                      (.setFetchSize 50))]
      (case (count components)
        1
        (let [prepared-st (.prepareStatement connection
                                             (str (datoms-select-sql db)
                                                  " where"
                                                  " e = ?"
                                                  " and rx > ?"
                                                  " and tx <= ?")
                                             )]
          (.setLong prepared-st
                    1
                    (first components))
          (.setLong prepared-st
                    2
                    (:basis-tx db))
          (.setLong prepared-st
                    3
                    (:basis-tx db))
          (datoms-seq db
                      (.executeQuery prepared-st)))

        2
        (let [[e a] components]
          (let [prepared-st (.prepareStatement connection
                                               (str (datoms-select-sql db)
                                                    " where"
                                                    " e = ?"
                                                    " and a = ?"
                                                    " and rx > ?"
                                                    " and tx <= ?")
                                               )]
            (.setLong prepared-st
                      1
                      e)
            (.setLong prepared-st
                      2
                      (db-util/entid db
                                     a))
            (.setLong prepared-st
                      3
                      (:basis-tx db))
            (.setLong prepared-st
                      4
                      (:basis-tx db))
            (datoms-seq db
                        (.executeQuery prepared-st)))
          ))

      )))

(defn datoms
  [db index & components]
  (case index
    :eavt
    (apply datoms-eavt
           db
           index
           components)))

(extend-protocol datascript-db/IIndexAccess
  dbv.db.DB
  (-datoms [db index cs]
    (apply datoms db index cs))

  (-seek-datoms [db index cs]
    )

  (-rseek-datoms [db index cs]
    )

  (-index-range [db attr start end]
    )
  )

(extend-protocol datascript-db/IDB
  dbv.db.DB
  (-schema [db])
  (-attrs-by [db property]
    (case property
      :db.type/ref
      (= (:value-type (db-util/attribute db
                                         property))
         :db.type/ref)
      :db.cardinality/many
      (:is-component (db-util/attribute db
                                        property))))
  )

(comment
  (def data-source
    (jdbc/get-datasource {:dbtype "postgres"
                          :dbname "postgres"
                          :user "postgres"
                          :password "postgres"}))

  (def connection
    (doto (jdbc/get-connection data-source)
      (.setAutoCommit false)))

  (def db
    (-> (db/db {:connectable connection
                :table "eavrt"})
        (assoc :basis-tx 13194139535699)))

  (time
   (datoms db
           :eavt
           17592186045481
           :design/name
           ))

  (require '[datascript.pull-api :as pull])

  (time
   (pull/pull db '[:owner
                   :design/name
                   :design/uuid
                   :design/audio-selection
                   #_{:design/audio-selection [*
                                               {:audio-selection/source [*]}]}]
              17592186045481))

  )
