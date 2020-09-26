(ns dbv.search
  (:require [datascript.db :refer [datom e0 emax tx0 txmax] :as datascript-db]
            [next.jdbc :as jdbc]
            [dbv.db-util :as db-util]
            [dbv.datoms :as datoms]
            [dbv.bootstrap :as bootstrap]))

(defn slice
  [db from to]
  (let [connection (doto (jdbc/get-connection (:connectable db))
                     (.setAutoCommit false))]
    (let [statement (doto (.createStatement connection)
                      (.setFetchSize 50))]
      (let [value-column (db-util/column-name db
                                              (:a from))
            sql (str (datoms/datoms-select-sql db)
                     " where"
                     " a >= ?"
                     " and a <= ?"
                     " and " value-column " >= ?"
                     " and " value-column " <= ?"
                     " and rx > ?"
                     " and tx <= ?")
            prepared-st (.prepareStatement
                         connection
                         sql
                         )
            db-type (:value-type (db-util/attribute db
                                                    (:a from)))
            serialize (get-in bootstrap/types
                              [db-type
                               :serialize])]
        (.setLong prepared-st
                  1
                  (db-util/entid db
                                 (:a from)))
        (.setLong prepared-st
                  2
                  (db-util/entid db
                                 (:a to)))
        (.setObject prepared-st
                    3
                    (serialize (:v from)))
        (.setObject prepared-st
                    4
                    (serialize (:v to)))
        (.setLong prepared-st
                  5
                  (:basis-tx db))
        (.setLong prepared-st
                  6
                  (:basis-tx db))
        (println (.toString prepared-st))
        (datoms/datoms-seq db
                           (.executeQuery prepared-st)))
      )))

(defn search
  [db pattern]
  (let [[e a v tx] pattern
        multival? (= (:cardinality (db-util/attribute db
                                                      a))
                     :db.cardinality/many)]
    (prn [e a (some? v) tx])
    (datascript-db/case-tree
     [e a (some? v) tx]
     [nil   ;; e a v tx
      nil   ;; e a v _
      nil   ;; e a _ tx
      nil   ;; e a _ _
      nil   ;; e _ v tx
      nil   ;; e _ v _
      nil   ;; e _ _ tx
      nil   ;; e _ _ _
      nil   ;; _ a v tx
      (slice db (datom e0 a v tx0) (datom emax a v txmax)) ;; _ a v _
      #_(if (indexing? db a)                               ;; _ a v _
          (set/slice avet (datom e0 a v tx0) (datom emax a v txmax))
          (->> (set/slice aevt (datom e0 a nil tx0) (datom emax a nil txmax))
               (filter (fn [^Datom d] (= v (.-v d))))))
      nil   ;; _ a _ tx
      nil   ;; _ a _ _
      nil   ;; _ _ v tx
      nil   ;; _ _ v _
      nil   ;; _ _ _ tx
      nil   ;; _ _ _ _ eavt
      ])))

(extend-protocol datascript-db/ISearch
  dbv.db.DB
  (-search [db pattern]
    (search db
            pattern)))

(comment
  (def data-source
    (jdbc/get-datasource {:dbtype "postgres"
                          :dbname "postgres"
                          :user "postgres"
                          :password "postgres"}))

  (def connection
    (doto (jdbc/get-connection data-source)
      (.setAutoCommit false)))

  (require '[dbv.db :as db])

  (def db
    (-> (db/db {:connectable connection
                :table "eavrt"})
        (assoc :basis-tx 13194139535699)))

  (require '[datascript.query :as q])

  (time
   (q/q '[:find ?e
          :where
          [?e :design/name "Feed Square"]]
        db))

  (time
   (q/q '[:find ?e
          :where
          [?e :design/created-at #inst "2020-09-01T09:14:28.752-00:00"]]
        db))

  (time
   (q/q '[:find (pull ?e [:design/name :design/audio-selection])
          :where
          [?e :design/created-at #inst "2020-09-01T09:14:28.752-00:00"]]
        db))

  )
