(ns more.relational.memoryhash
   (:require [more.relational.hashRel :as hashrel])
  (:use [criterium.core :as crit])
  (:use [more.relational.tools])
  (:use [more.relational.runtimehashrel]))



(defn create-employee-files []
  (doseq [c [1000
           5000
           10000
           50000
           100000
           200000
           300000]]
  (hashrel/save-relvar (hashrel/relvar (hashrel/rel [:emp_no :birth_date :first_name :last_name :gender :hire_date] (take c (load-raw-data-employees)))) (str "resources/hashrel-" c ".db"))))



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

;(create-db-files )

(defn insert-mem-test
  []
  (let [db (hashrel/load-db "resources/hashrel-db-3919015.db")
        rvar (:employee db)
        duplicates (take 100 (load-raw-data-employees))
        news (mapv (fn[n] (create-employee-dummies)) (range 100))]
    (Thread/sleep 5000)
    (hashrel/insert! rvar duplicates)
    (Thread/sleep 5000)
    (hashrel/insert! rvar news)))






