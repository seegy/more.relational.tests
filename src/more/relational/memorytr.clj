(ns more.relational.memorytr
   (:require [more.relational.transrelational :as trel])
  (:use [criterium.core :as crit])
  (:use [more.relational.tools])
  (:use [more.relational.runtimetr]))






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
  (trel/save-transvar (trel/transvar (trel/tr [:emp_no :birth_date :first_name :last_name :gender :hire_date] (take c (load-raw-data-employees)))) (str "resources/tr-" c ".db"))))


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
    (trel/save-db db (str "resources/tr-db-" tupel-count ".db")))))

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
    (trel/save-db db (str "resources/tr-testing-db-" c ".db")))))



;(create-db-for-testing-files)
