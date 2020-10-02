(ns dbv.datomic-export
  (:require [datomic.api :as d]))

(def datom->vec
  (juxt :e :a :v :tx :added))

(defn schema-datoms
  [db]
  (let [first-tx-eid (d/t->tx 0)]
    (take-while
      (fn [datom]
        (< (:e datom)
           first-tx-eid))
      (d/datoms db :eavt))))

(defn entity-datoms
  [db eid]
  (map datom->vec
       (seq (d/datoms db :eavt eid))))

(defn user-idents
  [db]
  (let [first-tx-eid (d/t->tx 0)]
    (mapcat
     (partial entity-datoms
              db)
     (filter
      (fn [eid]
        (< first-tx-eid
           eid))
      (keys (:keys db)))))
  )

(comment

  (count (map datom->vec
              (schema-datoms db)))

  (user-idents db)
  )
