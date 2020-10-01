(ns dbv.bootstrap
  (:require [clojure.edn :as edn]
            [clojure.string :as str]
            [next.jdbc :as jdbc]))

;; Concept:
;;
;; Tools to bootstrap a dbv database.

(def types
  {:db.type/boolean
   {:column-type "boolean"
    :serialize identity
    :deserialize identity}

   :db.type/double
   {:column-type "float(16)"
    :serialize double
    :deserialize double}

   :db.type/float
   {:column-type "float(8)"
    :serialize float
    :deserialize float}

   ;; :db.type/fn ; not supported

   :db.type/instant
   {:column-type "timestamp"
    :serialize (fn [date]
                 (java.sql.Timestamp. (.getTime date)))
    :deserialize (fn [sql-timestamp]
                   (java.util.Date. (.getTime sql-timestamp)))}

   :db.type/keyword
   {:column-type "text"
    :serialize pr-str
    :deserialize edn/read-string}

   :db.type/long
   {:column-type "bigint"
    :serialize long
    :deserialize long}

   :db.type/ref
   {:column-type "bigint"
    :serialize long
    :deserialize long}

   :db.type/string
   {:column-type "text"
    :serialize identity
    :deserialize identity}

   :db.type/symbol
   {:column-type "text"
    :serialize pr-str
    :deserialize edn/read-string}

   ;; :db.type/tuple ; not supported

   ;; TODO: maybe use composite types or jsonb column
   ;;       [["type1","tuple1"],["type2","tuple2"] ...]

   :db.type/uuid
   {:column-type "uuid"
    :serialize identity
    :deserialize identity}})

(defn create-table!
  [{:keys [connectable table]}]
  (jdbc/execute-one!
   connectable
   [(str "create table "
         table
         " (id char(40) primary key, "
         (str/join ", "
                   (map
                    (fn [[a t]]
                      (str (name a)
                           " "
                           t))
                    (concat
                     [["e" "bigint"]
                      ["a" "bigint"]]
                     (sort (map (juxt key (comp :column-type
                                                val))
                                types))
                     [["rx" "bigint"]
                      ["tx" "bigint"]])))
         ")")]))

(defn value-columns
  []
  (sort (map (comp keyword name)
             (keys types))))

(defn all-columns
  []
  (concat
   [:id :e :a :rx :tx]
   (value-columns)))

(defn create-ea-index!
  [{:keys [connectable table]}]
  (jdbc/execute!
   connectable
   [(str "CREATE INDEX "
         "dbv_" table "_ea"
         " ON "
         table
         " (e,a,rx,tx) include ("
         (str/join "," (map name (value-columns)))
         ");")]))

(defn create-eav-indexes!
  [{:keys [connectable table]}]
  (doseq [value-column (map name (value-columns))]
    (jdbc/execute! connectable
                   [(str "CREATE INDEX "
                         "dbv_" table "_eav_"
                         value-column
                         " ON "
                         table
                         " (e,a,"
                         value-column
                         ",rx,tx) where "
                         value-column
                         " is not null;")])))

(defn bootstrap!
  [{:keys [connectable table] :as params}]
  (create-table! params)
  (create-ea-index! params)
  (create-eav-indexes! params)
  )

(comment
  (def bootstrap-params
    {:connectable (jdbc/get-connection {:dbtype "postgres"
                                        :dbname "postgres"
                                        :user "postgres"
                                        :password "postgres"})
     :table "eavrt"}
    )

  (bootstrap! bootstrap-params)

  (jdbc/execute-one! (:connectable bootstrap-params)
                     [(str "drop table "
                           (:table bootstrap-params))])
  )
