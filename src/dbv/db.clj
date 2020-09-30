(ns dbv.db
  (:require [next.jdbc :as jdbc]
            [clojure.set :as set]
            [clojure.edn :as edn]
            [dbv.bootstrap :as bootstrap]))

;; Concept:
;;
;; Provides the function `db` to retrieve the current database as a
;; value.

(defn- remove-key-namespace
  [m]
  (into {}
        (map (fn [[k v]]
               [(keyword (name k)) v]))
        m))

(defn- e-keyword-map
  [rows]
  (into {}
        (map (fn [row]
               [(:e row)
                (edn/read-string (:keyword row))]))
        rows))

(defn execute!
  [{:keys [connectable]} & args]
  (->> (apply jdbc/execute!
              connectable
              args)
       (map remove-key-namespace)))

(defn q-e-idents
  [db*]
  (-> (execute! db*
                [(str "select e, keyword from "
                      (:table db*)
                      " where a = 10")])
      (e-keyword-map)))

(defn q-types
  [db*]
  (let [a-ident (get-in db*
                        [:ids
                         :db/ident])
        table (:table db*)]
    (-> (execute! db*
                  [(str "select a.e, b.keyword from "
                        table
                        " a inner join "
                        table
                        " b on a.ref = b.e and b.a = ? where a.e in (select e from "
                        table
                        " where a = ?) and a.a = ?")
                   a-ident
                   a-ident
                   (get-in db*
                           [:ids
                            :db/valueType])])
        (e-keyword-map))))

(defn q-cardinality
  [db*]
  (let [a-ident (get-in db*
                        [:ids
                         :db/ident])
        table (:table db*)]
    (-> (execute! db*
                  [(str "select a.e, b.keyword from "
                        table
                        " a inner join "
                        table
                        " b on a.ref = b.e and b.a = ? where a.e in (select e from "
                        table
                        " where a = ?) and a.a = ?")
                   a-ident
                   a-ident
                   (get-in db*
                           [:ids
                            :db/cardinality])])
        (e-keyword-map))))

(defn q-unique
  [db*]
  (let [a-ident (get-in db*
                        [:ids
                         :db/ident])
        table (:table db*)]
    (-> (execute! db*
                  [(str "select a.e, b.keyword from "
                        table
                        " a inner join "
                        table
                        " b on a.ref = b.e and b.a = ? where a.e in (select e from "
                        table
                        " where a = ?) and a.a = ?")
                   a-ident
                   a-ident
                   (get-in db*
                           [:ids
                            :db/unique])])
        (e-keyword-map))))

(defn q-is-component
  [db*]
  (let [table (:table db*)]
    (->> (execute! db*
                   [(str "select e from "
                         table
                         " where a = ? and boolean = true")
                    (get-in db*
                            [:ids
                             :db/isComponent])])
         (map :e)
         (into #{}))))

(defrecord DB [])

(defn rschema
  [db]
  {
   :db/unique (into #{}
                    (map (:keys db))
                    (keys (:attribute-unique db)))
   :db.unique/identity (into #{}
                             (comp
                              (keep (fn [[eid db-unique]]
                                      (when (= db-unique
                                               :db.unique/identity)
                                        eid)))
                              (map (:keys db)))
                             (:attribute-unique db))
   :db.unique/value (into #{}
                          (comp
                           (keep (fn [[eid db-unique]]
                                   (when (= db-unique
                                            :db.unique/value)
                                     eid)))
                           (map (:keys db)))
                          (:attribute-unique db))
   ;; :db/index
   :db.cardinality/many (into #{}
                              (comp
                               (keep (fn [[eid db-cardinality]]
                                       (when (= db-cardinality
                                                :db.cardinality/many)
                                         eid)))
                               (map (:keys db)))
                              (:attribute-cardinality db))
   :db.type/ref (into #{}
                      (comp
                       (keep (fn [[eid db-type]]
                               (when (= db-type
                                        :db.type/ref)
                                 eid)))
                       (map (:keys db)))
                      (:attribute-types db))
   :db/isComponent (into #{}
                         (map (:keys db))
                         (:attribute-components db))
   ;; :db.type/tuple
   ;; :db/attrTuples
   })

(defn db
  [{:keys [connectable
           table] :as connection-spec}]
  (let [keys (q-e-idents connection-spec)
        ids (set/map-invert keys)
        db* (merge (->DB)
                   {:ids ids
                    :keys keys
                    :connectable connectable
                    :table table
                    :value-columns (bootstrap/value-columns)})
        db** (assoc db*
                    :attribute-types (q-types db*)
                    :attribute-cardinality (q-cardinality db*)
                    :attribute-components (q-is-component db*)
                    :attribute-unique (q-unique db*)
                    )]
    ;; TODO: augment current t
    (assoc db**
           :rschema (rschema db**))
    )
  )

(comment
  (def connection-spec
    {:connectable (jdbc/get-connection {:dbtype "postgres"
                                        :dbname "postgres"
                                        :user "postgres"
                                        :password "postgres"})
     :table "eavrt"}
    )

  (time (db connection-spec))
  )
