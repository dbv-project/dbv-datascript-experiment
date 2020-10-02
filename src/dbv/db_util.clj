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
    {:id a
     :ident (ident db
                   attrid)
     :value-type (get-in db
                         [:attribute-types
                          a])
     :is-component (contains? (:attribute-components db)
                              a)
     :cardinality (get-in db
                          [:attribute-cardinality
                           a])
     :unique (get-in db
                     [:attribute-unique
                      a])
     }))

(defn value-type
  [db a]
  (get (:attribute-types db)
       (entid db
              a)))

(defn column-name
  [db a]
  (-> (value-type db
                  a)
      (name)))

(defn a-column
  [db a]
  (keyword (column-name db a)))

(defn squuid
  "See: https://github.com/clojure-cookbook/clojure-cookbook/blob/master/01_primitive-data/1-24_uuids.asciidoc"
  []
  (let [uuid (java.util.UUID/randomUUID)
        time (System/currentTimeMillis)
        secs (quot time 1000)
        lsb (.getLeastSignificantBits uuid)
        msb (.getMostSignificantBits uuid)
        timed-msb (bit-or (bit-shift-left secs 32)
                          (bit-and 0x00000000ffffffff msb))]
    (java.util.UUID. timed-msb lsb)))
