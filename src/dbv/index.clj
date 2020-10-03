(ns dbv.index
  (:require [next.jdbc :as jdbc]
            [dbv.db-util :as db-util]
            [datascript.query :as q]))

(defn create-ave-index!
  [db attribute]
  (let [attribute-eid (db-util/entid db
                                     attribute)
        value-column (db-util/column-name db
                                          attribute-eid)
        table (:table db)]
    (jdbc/execute! (:connectable db)
                   [(str "CREATE INDEX "
                         "dbv_" table "_ave_"
                         attribute-eid
                         " ON "
                         table
                         " (a,"
                         value-column
                         ",e,rx,tx) where "
                         " a = " attribute-eid)])))

(defn q-index-attributes
  [db]
  (q/q '[:find [?a ...]
         :where
         (or [?a :db/index true]
             [?a :db/unique])]
       db))

(defn create-ave-indexes!
  [db]
  (doseq [a (q-index-attributes db)]
    (create-ave-index! db
                       a)))

(comment
  (do
    (def data-source
      (jdbc/get-datasource {:dbtype "postgres"
                            :dbname "postgres"
                            :user "postgres"
                            :password "postgres"}))

    (require '[dbv.db :as db])

    (def db
      (-> (db/db {:connectable (jdbc/get-connection data-source)
                  :table "eavrt"})
          (assoc :basis-tx
                 13194185203999
                 ;; 13194139535699
                 )))
    )

  (q-index-attributes db)

  (create-ave-indexes! db)
  )
