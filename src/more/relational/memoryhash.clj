(ns more.relational.memoryhash
   (:require [more.relational.hashRel :as hashrel])
  (:use [criterium.core :as crit])
  (:use [more.relational.tools])
  (:use [more.relational.runtimehashrel]))



(def employees (load-raw-data-employees))




#_(defn lulu
  [steps max-t]
  (let [relvar  (batrel/batvar (batrel/convertToBats [:emp_no :birth_date :first_name :last_name :gender :hire_date] #{})) ]
  (loop [tuples (take max-t employees)
         result []]
    (if (empty? tuples)
      (do
        [ relvar result])
      (do
        (doseq [t (take steps tuples)] (batrel/insert! relvar t))
        (crit/force-gc)
        (recur (drop steps tuples) (conj result (crit/heap-used))))))))



;  (lulu 100 10000)




(defn create-employee-files []
  (doseq [c [1000
           5000
           10000
           50000
           100000
           200000
           300000]]
  (hashrel/save-relvar (hashrel/relvar (hashrel/rel [:emp_no :birth_date :first_name :last_name :gender :hire_date] (take c employees))) (str "resources/hashrel-" c ".db"))))



(defn create-db-files []
  (doseq [c [1000
             5000
             10000
             50000
             100000
             200000
             300024]]
    (let[db (create-employee-database-nocons c)
         tupel-count (apply + (map (fn[[_ v]] (count @v)) db))]
    (hashrel/save-db db (str "resources/hashrel-db-" tupel-count ".db")))))

(defn create-db-for-testing-files []
  (doseq [c [1000
             2500
             5000
             10000
             15000
             20000
             30000 ]]
    (let[db (create-employee-database-nocons c)
         tupel-count (apply + (map (fn[[_ v]] (count @v)) db))]
    (hashrel/save-db db (str "resources/hashrel-testing-db-" c ".db")))))


(create-db-for-testing-files)
