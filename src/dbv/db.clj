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

(defn db
  [{:keys [connectable
           table] :as connection-spec}]
  (let [keys (q-e-idents connection-spec)
        ids (set/map-invert keys)
        db* {:ids ids
             :keys keys
             :connectable connectable
             :table table
             :value-columns (bootstrap/value-columns)}]
    ;; TODO: augment current t
    (assoc db*
           :attribute-types (q-types db*)
           :attribute-cardinality (q-cardinality db*)
           ))
  )

(defn entid
  [db ident]
  (get-in db
          [:ids
           ident]))

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
