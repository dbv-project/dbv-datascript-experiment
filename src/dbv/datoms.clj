(ns dbv.datoms
  (:require [next.jdbc :as jdbc]
            [clojure.string :as str]
            [next.jdbc.protocols :as p]
            [datascript.db :as datascript-db]
            [dbv.db :as db]
            [dbv.db-util :as db-util]
            [next.jdbc.prepare :as prepare]
            [dbv.bootstrap :as bootstrap]
            ))

(extend-protocol p/Connectable
  java.sql.Connection
  (get-connection [this opts] this))

(extend-protocol prepare/SettableParameter
  clojure.lang.Keyword
  (set-parameter [v ^java.sql.PreparedStatement s ^long i]
    (.setString s i (pr-str v))))

(defn extract-datom
  [db ^java.sql.ResultSet result-set]
  (let [a (.getLong result-set
                    "a")
        value-type (db-util/value-type db
                                       a)
        deserialize (get-in bootstrap/types
                            [value-type
                             :deserialize])]
    (datascript-db/datom (.getLong result-set
                                   "e")
                         (db-util/ident db
                                        a)
                         (-> (.getObject result-set
                                         ^String
                                         (name value-type))
                             (deserialize))
                         (.getLong result-set
                                   "tx")
                         true
                         ;; TODO: handle retract
                         #_(= (.getLong result-set
                                        "rx")
                              Long/MAX_VALUE))))

(defn datoms-seq
  [db ^java.sql.ResultSet result-set]
  (when (.next result-set)
    (lazy-seq
     (cons (extract-datom db
                          result-set)
           (datoms-seq db result-set)))
    ))

(defn datoms
  [db index & components]
  (let [[c0 c1 c2 c3 c4] components
        [e a v t added] (case index
                          :eavt
                          components
                          :avet
                          [c2 c0 c1 c3 c4])
        a (db-util/entid db
                         a)
        connection (doto (jdbc/get-connection (:connectable db))
                     (.setAutoCommit false))]
    (let [statement (doto (.createStatement connection)
                      (.setFetchSize 50))
          prepared-st (.prepareStatement connection
                                         (str "select e,a,"
                                              (if a
                                                (db-util/column-name db
                                                                     a)
                                                (str/join "," (map name (:value-columns db))))
                                              ",rx,tx"
                                              " from " (:table db)
                                              " where "
                                              (str/join
                                               " and "
                                               (remove nil?
                                                       [(when e
                                                          "e = ?")
                                                        (when a
                                                          "a = ?")
                                                        (when v
                                                          (str (db-util/column-name db
                                                                                    a)
                                                               " = ?"))
                                                        "rx > ?"
                                                        "tx <= ?"]))))]
      (prepare/set-parameters
       prepared-st
       (remove nil?
               [e
                a
                v
                (:basis-tx db)
                (:basis-tx db)]))
      (datoms-seq db
                  (.executeQuery prepared-st))

      )))

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
    ((:rschema db) property))
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
