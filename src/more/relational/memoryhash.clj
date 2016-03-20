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


(defn insert-mem-test-dup
  []
  (let [rvar (hashrel/load-relvar "resources/hashrel-300024.db")
        duplicates (take 100 (load-raw-data-employees))]
    (println "starting")
    (hashrel/insert! rvar duplicates)))

(defn insert-mem-test-new
  []
  (let [rvar (hashrel/load-relvar "resources/hashrel-300024.db")
        news (mapv (fn[n] (create-employee-dummies)) (range 100))]
    (println "starting")
    (hashrel/insert! rvar news)))



(defn delete-mem-test-ps
  []
  (let [rvar (hashrel/load-relvar "resources/hashrel-300024.db")
        ids-to-delete [44948 33758 18936 12585]]
    (println "starting")
    (doseq [id ids-to-delete] (hashrel/delete! rvar (hashrel/relfn [t] (= (:emp_no t) id))))))



(defn delete-mem-test-as
  []
  (let [rvar (hashrel/load-relvar "resources/hashrel-300024.db")]
    (println "starting")
    (hashrel/delete! rvar (hashrel/relfn [t] (= (:gender t) "F")))))




(defn search-mem-test-ps
  []
  (let [rvar (hashrel/load-relvar "resources/hashrel-300024.db")
        ids-to-delete [44948 33758 18936 12585]]
    (println "starting")
    (doseq [id ids-to-delete] (hashrel/restrict @rvar #(= (:emp_no %) id)))))


(defn search-mem-test-as
  []
  (let [rvar (hashrel/load-relvar "resources/hashrel-300024.db")]
    (println "starting")
    (hashrel/restrict @rvar (hashrel/relfn [t] (= (:gender t) "F")))))



(defn join-mem-test-es
  []
  (let [emp_rvar (hashrel/load-relvar "resources/hashrel-300024.db")
        sal_rvar (hashrel/load-relvar "resources/hashrel-salaries.db")]
    (println "starting")
    (hashrel/join @emp_rvar @sal_rvar)))


(defn join-mem-test-se
  []
  (let [emp_rvar (hashrel/load-relvar "resources/hashrel-300024.db")
        sal_rvar (hashrel/load-relvar "resources/hashrel-salaries.db")]
    (println "starting")
    (hashrel/join @sal_rvar @emp_rvar)))

