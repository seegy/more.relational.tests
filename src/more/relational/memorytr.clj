(ns more.relational.memorytr
   (:require [more.relational.transrelational :as trel])
  (:use [criterium.core :as crit])
  (:use [more.relational.tools])
  (:use [more.relational.runtimetr]))






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


(defn insert-mem-test-dup
  []
  (let [db (trel/load-db "resources/tr-db-3919015.db")
        rvar (:employee db)
        duplicates (take 100 (load-raw-data-employees))]
    (trel/insert! rvar duplicates)))


(defn insert-mem-test-new
  []
  (let [db (trel/load-db "resources/tr-db-3919015.db")
        rvar (:employee db)
        news (mapv (fn[n] (create-employee-dummies)) (range 100))]
    (trel/insert! rvar news)))


(defn delete-mem-test-ps
  []
  (let [db (trel/load-db "resources/tr-db-3919015.db")
        rvar (:employee db)
        ids-to-delete [44948 33758 18936 12585]]
    (doseq [id ids-to-delete] (trel/delete! rvar (trel/tr-fn [t] (= (:emp_no t) id))))))



(defn delete-mem-test-as
  []
  (let [db (trel/load-db "resources/tr-db-3919015.db")
        rvar (:employee db)]
    (trel/delete! rvar (trel/tr-fn [t] (= (:gender t) "F")))))


(defn search-mem-test-ps
  []
  (let [db (trel/load-db "resources/tr-db-3919015.db")
        rvar (:employee db)
        ids-to-delete [44948 33758 18936 12585]]
    (doseq [id ids-to-delete] (trel/restriction @rvar (trel/tr-fn [t] (= (:emp_no t) id))))))



(defn search-mem-test-as
  []
  (let [db (trel/load-db "resources/tr-db-3919015.db")
        rvar (:employee db)]
    (trel/restriction @rvar (trel/tr-fn [t] (= (:gender t) "F")))))


