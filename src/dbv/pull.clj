(ns dbv.pull
  (:require [next.jdbc :as jdbc]
            [clojure.string :as str]
            [dbv.db :as db]))

;; Concept:
;;
;; Tools to pull an entity from a dbv database accordingly to a
;; pull-spec.

(defn add-attribute-ident
  [db row]
  (assoc row
         :attribute-ident (get (:keys db)
                               (:a row))))

(defn a-column
  [db a]
  (-> (get (:attribute-types db)
           a)
      (name)
      (keyword)))

(defn add-v
  [db row]
  (assoc row
         :v (get row
                 (a-column db
                           (:a row)))))

(defn complete-row
  [db row]
  (->> row
       (add-attribute-ident db)
       (add-v db)))

(defn build-entity-map*
  [db eid entities]
  (reduce
   (fn [entity-map row]
     (case (get-in db
                   [:attribute-cardinality
                    (:a row)])
       :db.cardinality/one
       (assoc entity-map
              (:attribute-ident row)
              (if (:ref row)
                (assoc (build-entity-map* db
                                          (:ref row)
                                          entities)
                       :db/id
                       (:ref row))
                (:v row)))
       :db.cardinality/many
       (update entity-map
               (:attribute-ident row)
               (fn [v]
                 (conj (or v #{})
                       (if (:ref row)
                         (assoc (build-entity-map* db
                                                   (:ref row)
                                                   entities)
                                :db/id
                                (:ref row))
                         (:v row)))))
       ))
   {}
   (get entities
        eid)))

(defn build-entity-map
  [db eid rows]
  (let [entities (->> rows
                      (map (partial complete-row
                                    db))
                      (group-by :e))]
    (build-entity-map* db
                       eid
                       entities)))

(defn select-zero
  [db eid aidents]
  (let [tx (:basis-tx db)]
    (str "select e,a,"
         (str/join "," (map name (:value-columns db)))
         " from " (:table db)
         " where "
         " e = " eid
         " and rx > " tx
         " and tx <= " tx
         " and a in (" (str/join "," (map (partial db/entid db)
                                          aidents))  ")"
         )))

(defn select-level
  [db level aidents]
  (str
   " l" level
   " as ("
   "   select e,a," (str/join "," (map name (:value-columns db)))
   "   from " (:table db)
   " where e in ( "
   "select ref from l" (dec level) " where a in (" (str/join "," (map (partial db/entid db) aidents)) ")"
   "))")
  )

(defn select-union
  [level-count]
  (str/join " union all "
            (map
             (fn [level]
               (str "select * from l"
                    level))
             (range level-count))))

(comment
  (def connection
    (jdbc/get-connection {:dbtype "postgres"
                          :dbname "postgres"
                          :user "postgres"
                          :password "postgres"}))

  (def sample-db
    (-> (db/db {:connectable connection
                :table "eavrt"})
        (assoc :basis-tx 13194139535699)))

  (def rows
    (time
     (jdbc/execute! connection
                    [(str "with l0 as ("
                          (select-zero sample-db
                                       17592186045481
                                       [:design/name
                                        :design/audio-selection])
                          ")"
                          " , "
                          (select-level sample-db
                                        1
                                        [:design/audio-selection])
                          " , "
                          (select-level sample-db
                                        2
                                        [:audio-selection/source])
                          (select-union 3)
                          )])))

  (time
   (build-entity-map sample-db
                     17592186045481
                     rows))


  )
