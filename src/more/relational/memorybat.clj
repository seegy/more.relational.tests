(ns more.relational.memorybat
   (:require [more.relational.bat :as batrel])
  (:use [criterium.core :as crit])
  (:use [more.relational.tools])
  (:use [more.relational.runtimebat]))



(defn create-employee-files []
  (doseq [c [1000
           5000
           10000
           50000
           100000
           200000
           300000]]
  (batrel/save-batvar (batrel/batvar (batrel/convertToBats [:emp_no :birth_date :first_name :last_name :gender :hire_date] (take c (load-raw-data-employees)))) (str "resources/bat-" c ".db"))))

;(create-employee-files)



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
    (batrel/save-db db (str "resources/bat-db-" tupel-count ".db")))))


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
    (batrel/save-db db (str "resources/bat-testing-db-" c ".db")))))



(defn insert-mem-test-dup
  []
  (let [db (batrel/load-db "resources/bat-db-3919015.db")
        rvar (:employee db)
        duplicates (take 100 (load-raw-data-employees))]
    (doseq [t duplicates] (batrel/insert! rvar t))))



(defn insert-mem-test-new
  []
  (let [db (batrel/load-db "resources/bat-db-3919015.db")
        rvar (:employee db)
        news (mapv (fn[n] (create-employee-dummies)) (range 100))]
    (doseq [t news](batrel/insert! rvar t))))




(defn delete-mem-test-ps
  []
  (let [db (batrel/load-db "resources/bat-db-3919015.db")
        rvar (:employee db)
        ids-to-delete [44948 33758 18936 12585]]
    (doseq [id ids-to-delete]
      (let[ result (batrel/select (:emp_no  @rvar) = id)
            tuples (batrel/makeTable [] (keys @rvar) (vals (assoc @rvar :emp_no (batrel/join (batrel/mirror result) (:emp_no  @rvar) =))))]
        (doseq [t tuples] (batrel/delete! rvar t))))))




(defn delete-mem-test-as
  []
  (let [db (batrel/load-db "resources/bat-db-3919015.db")
        rvar (:employee db)]
    (let[ result (batrel/select (:gender  @rvar) not= "F")]
      (batrel/assign! rvar (assoc @rvar :gender (batrel/join (batrel/mirror result) (:gender  @rvar) =))))))




(defn search-mem-test-ps
  []
  (let [db (batrel/load-db "resources/bat-db-3919015.db")
        rvar (:employee db)
        ids-to-delete [44948 33758 18936 12585]]
    (doseq [id ids-to-delete]
      (let[ result (batrel/select (:emp_no  @rvar) = id)]
        (batrel/makeTable [] (keys @rvar) (vals (assoc @rvar :emp_no (batrel/join (batrel/mirror result) (:emp_no  @rvar) =))))))))


(defn search-mem-test-as
  []
  (let [db (batrel/load-db "resources/bat-db-3919015.db")
        rvar (:employee db)]
    (let[ result (batrel/select (:gender  @rvar) not= "F")]
      (batrel/makeTable [] (keys @rvar) (vals (assoc @rvar :gender (batrel/join (batrel/mirror result) (:gender  @rvar) =)))))))





