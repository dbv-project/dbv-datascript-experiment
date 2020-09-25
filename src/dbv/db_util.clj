(ns dbv.db-util)

(defn entid
  [db ident]
  (if (integer? ident)
    ident
    (get-in db
            [:ids
             ident])))

(defn ident
  [db eid]
  (if (keyword? eid)
    eid
    (get-in db
            [:keys
             eid])))

(defn attribute
  [db attrid]
  (let [a (entid db
                 attrid)]
    {:ident (ident db
                   attrid)
     :value-type (get-in db
                         [:attribute-types
                          a])
     :is-component (contains? (:attribute-components db)
                              a)
     :cardinality (get-in db
                          [:attribute-cardinality
                           a])
     }))

(defn column-name
  [db a]
  (-> (get (:attribute-types db)
           a)
      (name)))
