(ns dbv.datomic-import
  (:require [digest]
            [dbv.bootstrap :as bootstrap]
            [next.jdbc :as jdbc]
            [next.jdbc.sql :as sql]
            [clojure.edn :as edn]
            [clojure.java.io :as io]))

(defn prepare-value
  [a->value-types datom]
  (let [[e a v tx added] datom
        value-type (a->value-types a)]
    (when-let [serialize-fn (get-in bootstrap/types
                                    [value-type
                                     :serialize])]
      [(keyword (name value-type))
       (serialize-fn v)])))

(defn find-attribute-ident
  [attribute-ident datoms]
  (some
    (fn [[e a v tx added]]
      (when (and added
                 (= v attribute-ident))
        e))
    datoms))

(defn find-idents
  [schema-datoms]
  (let [ident-attribute (find-attribute-ident :db/ident
                                              schema-datoms)]
    (into {}
          (map
            (fn [[e a v]]
              [e v])
            (filter
              (fn [[e a]]
                (= a ident-attribute))
              schema-datoms)))))

(defn a-value-types
  [schema-datoms]
  (let [value-type-attribute (find-attribute-ident :db/valueType
                                                   schema-datoms)
        idents (find-idents schema-datoms)]
    (into {}
          (map
           (fn [[e a v]]
             [e (idents v)])
           (filter
            (fn [[e a]]
              (= a value-type-attribute))
            schema-datoms)))))

(defn prepare-insert
  [prepare-value datom]
  (let [[e a v tx added] datom]
    (when added
      (when-let [[column value] (prepare-value datom)]
        {:id (digest/sha1 (pr-str [e a v tx]))
         :e e
         :a a
         :rx Long/MAX_VALUE
         :tx tx
         column value}
        ))))

(defn insert!
  [{:keys [prepare-value connectable table]} datoms]
  ;; TODO: surround with Postgres transaction and compare-and-swap
  ;;       basis-t
  (doseq [insert (keep (partial prepare-insert prepare-value)
                       datoms)]
    (sql/insert! connectable
                 table
                 insert)))

(comment
  (def sample-schema-datoms
    (edn/read-string {:default tagged-literal}
                     (slurp "schema.edn")))

  (find-attribute-ident :db/valueType
                        sample-schema-datoms)

  (find-idents sample-schema-datoms)

  (a-value-types sample-schema-datoms)

  )

(defn edn-line-seq
  "Like `clojure.core/line-seq`, plus it parses each line as edn data."
  [rdr]
  (sequence (map (fn [line]
                   (edn/read-string {:default tagged-literal}
                                    line)))
            (line-seq rdr)))

(defn import!
  [{:keys [prepare-value connectable table]} datom-seq]
  (let [all-columns (bootstrap/all-columns)
        row-fn (apply juxt
                      all-columns)]
    (doseq [datoms (partition-all
                    1000
                    (keep (partial prepare-insert prepare-value)
                          datom-seq)
                    )]
      (sql/insert-multi! connectable
                         table
                         all-columns
                         (map row-fn
                              datoms)))))

(comment
  (def import-params
    {:connectable (jdbc/get-connection {:dbtype "postgres"
                                        :dbname "postgres"
                                        :user "postgres"
                                        :password "postgres"})
     :prepare-value (partial prepare-value
                             (a-value-types sample-schema-datoms))
     :table "eavrt"}
    )

  (insert! import-params
           sample-schema-datoms)

  (insert! import-params
           (edn/read-string {:default tagged-literal}
                            (slurp "user-idents.edn")))

  (time (import! import-params
                 (edn-line-seq (io/reader "datoms.edn"))))

  )
