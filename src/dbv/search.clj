(ns dbv.search
  (:require [datascript.db :refer [datom e0 emax tx0 txmax] :as datascript-db]
            [next.jdbc :as jdbc]
            [dbv.db-util :as db-util]
            [dbv.datoms :as datoms]
            [dbv.bootstrap :as bootstrap]
            [next.jdbc.prepare :as prepare]
            [clojure.string :as str]))

(defn slice
  [db from to]
  (let [connection (doto (jdbc/get-connection (:connectable db))
                     (.setAutoCommit false))]
    (let [statement (doto (.createStatement connection)
                      (.setFetchSize 50))]
      (let [value-column (when (:a from)
                           (db-util/column-name db
                                                (:a from)))
            sql (str "select e,a,"
                     ;; If the attribute is defined then only select
                     ;; the corresponding value column, to allow a
                     ;; Postgres Index Only Scan:
                     (or value-column
                         (str/join "," (map name (:value-columns db))))
                     ",rx,tx"
                     " from " (:table db)
                     " "
                     " where "
                     (str/join
                      " and "
                      (concat
                       (when (:e from)
                         ["e >= ?"
                          "e <= ?"])
                       (when (:a from)
                         ["a >= ?"
                          "a = ?"])
                       (when (:v from)
                         [(str value-column " >= ?")
                          (str value-column " <= ?")])
                       ["rx > ?"
                        "tx <= ?"])))
            prepared-st (.prepareStatement
                         connection
                         sql
                         )
            db-type (:value-type (db-util/attribute db
                                                    (:a from)))
            serialize (get-in bootstrap/types
                              [db-type
                               :serialize])]
        (prepare/set-parameters
         prepared-st
         (concat
          (when (:e from)
            [(:e from)
             (:e to)])

          (when (:a from)
            [(db-util/entid db
                            (:a from))
             (db-util/entid db
                            (:a to))])

          (when (:v from)
            [(serialize (:v from))
             (serialize (:v to))])

          [(:basis-tx db)
           (:basis-tx db)]
          ))
        ;; (println (.toString prepared-st))
        (datoms/datoms-seq db
                           (.executeQuery prepared-st)))
      )))

(defn search
  [db pattern]
  (let [[e a v tx] pattern
        multival? (= (:cardinality (db-util/attribute db
                                                      a))
                     :db.cardinality/many)
        not-implemented (fn [clause]
                          (throw (ex-info "not-implemented"
                                          {:clause clause})))]
    (datascript-db/case-tree
     [e a (some? v) tx]
     [nil ;; e a v tx
      nil ;; e a v _
      nil ;; e a _ tx
      (slice db (datom e a nil tx0) (datom e a nil txmax)) ;; e a _ _
      nil                                                  ;; e _ v tx
      nil                                                  ;; e _ v _
      nil                                                  ;; e _ _ tx
      nil                                                  ;; e _ _ _
      nil                                                  ;; _ a v tx
      (slice db (datom e0 a v tx0) (datom emax a v txmax)) ;; _ a v _
      #_(if (indexing? db a)                               ;; _ a v _
          (set/slice avet (datom e0 a v tx0) (datom emax a v txmax))
          (->> (set/slice aevt (datom e0 a nil tx0) (datom emax a nil txmax))
               (filter (fn [^Datom d] (= v (.-v d))))))
      nil ;; _ a _ tx
      (slice db (datom e0 a nil tx0) (datom emax a nil txmax)) ;; _ a _ _
      nil ;; _ _ v tx
      nil ;; _ _ v _
      nil ;; _ _ _ tx
      nil ;; _ _ _ _ eavt
      ])))

(extend-protocol datascript-db/ISearch
  dbv.db.DB
  (-search [db pattern]
    (search db
            pattern)))

(comment
  (do
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
    )

  (time
   (q/q '[:find [?e ...]
          :where
          [?e :design/name "Feed Square"]]
        db))

  (time
   (q/q '[:find ?e
          :where
          [?e :design/created-at #inst "2020-09-01T09:14:28.752-00:00"]]
        db))

  (time
   (q/q '[:find (pull ?e [:design/name :design/audio-selection :design/uuid])
          :where
          [?e :design/created-at #inst "2020-09-01T09:14:28.752-00:00"]]
        db))

  (time
   (q/q '[:find ?e .
          :where
          [?e :design/uuid  #uuid "5f4e10f1-7100-459c-82f5-7d3bc83914a5"]]
        db))


  (time
   (q/q '[:find ?uuid .
          :where
          [17592186045569 :design/uuid ?uuid]]
        db))

  (time
   (q/q '[:find ?e ?v
          :where
          [?e :design/uuid ?v]]
        db))

  )
