(ns dbv.import
  (:require [datomic.api :as d]
            [digest]
            [dbv.bootstrap :as bootstrap]
            [next.jdbc :as jdbc]
            [next.jdbc.sql :as sql]))

;; Concept:
;;
;; Imports a Datomic database into a dbv database.

(defn base-transactions
  [datomic-db]
  (map
   (fn [[tx datoms]]
     {:data datoms
      :t (:tx (first datoms))})
   (sort-by first (group-by :tx (datomic/datoms (datomic/as-of datomic-db
                                                               999)
                                                :eavt
                                                )))))

(defn prepare-inserts
  [prepare-value transaction]
  (keep
   (fn [{:keys [e a v tx added] :as datom}]
     (when added
       (when-let [[column value] (prepare-value datom)]
         {:id (digest/sha1 (pr-str [e a v tx]))
          :e e
          :a a
          :rx Long/MAX_VALUE
          :tx tx
          column value}
         )))
   (:data transaction)))

(defn a-value-types
  [db]
  (into {}
        (datomic/q '[:find ?a ?value-type
                     :where
                     [?a :db/valueType ?type]
                     [?type :db/ident ?value-type]]
                   db))
  )

(defn prepare-value-fn
  [db]
  (let [a->value-types (a-value-types db)]
    (fn [datom]
      (let [value-type (a->value-types (:a datom))]
        (when-let [serialize-fn (get-in bootstrap/types
                                        [value-type
                                         :serialize])]
          [(keyword (name value-type))
           (serialize-fn (:v datom))])))))

(defn prepare-updates
  [prepare-value table transaction]
  (keep
   (fn [{:keys [e a v tx added] :as datom}]
     (when-not (:added datom)
       (when-let [[column value] (prepare-value datom)]
         [(str "update "
               table
               " set rx = ? where rx = ? and e = ? and a = ? and "
               (name column)
               " = ?")
          tx
          Long/MAX_VALUE
          e
          a
          value])))
   (:data transaction)))

(defn import-transaction!
  [{:keys [prepare-value connectable table]} transaction]
  ;; TODO: surround with Postgres transaction and compare-and-swap
  ;;       basis-t
  (doseq [insert (prepare-inserts prepare-value
                                  transaction)]
    (sql/insert! connectable
                 table
                 insert))
  (doseq [update (prepare-updates prepare-value
                                  table
                                  transaction)]
    (jdbc/execute! connectable
                   update)))

(comment
  (def con
    (dev/con))

  (def db
    (d/db con))

  (base-transactions db)

  (a-value-types db)

  (prepare-inserts (prepare-value-fn db)
                   (first (base-transactions db)))

  (def transactions
    (d/tx-range
     (d/log con)
     nil
     nil))

  (prepare-updates (prepare-value-fn db)
                   (last transactions))

  (def import-params
    {:connectable (jdbc/get-connection {:dbtype "postgres"
                                        :dbname "postgres"
                                        :user "postgres"
                                        :password "postgres"})
     :prepare-value (prepare-value-fn db)
     :table "eavrt"}
    )

  (import-transaction! import-params
                       (last transactions))

  (time
   (doseq [transaction (concat (base-transactions db)
                               transactions)]
     (import-transaction! import-params
                          transaction)))

  ;; check expected row count:
  (=
   (let [a (a-value-types db)]
     (count (filter
             (fn [datom]
               (when (:added datom)
                 (bootstrap/types (a (:a datom)))))
             (mapcat :data
                     (concat (base-transactions db)
                             transactions)))))
   (:count (jdbc/execute-one! (:connectable import-params)
                              [(str "select count(*) from "
                                    (:table import-params))])))
  )
