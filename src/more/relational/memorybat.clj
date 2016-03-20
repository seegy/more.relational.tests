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
  (let [rvar (batrel/load-batvar "resources/bat-300024.db")
        duplicates (take 100 (load-raw-data-employees))]
    (println "starting")
    (doseq [t duplicates] (batrel/insert! rvar t))
    (println "finished with: " (count (:emp_no @rvar)))))



(defn insert-mem-test-new
  []
  (let [rvar (batrel/load-batvar "resources/bat-300024.db")
        news (mapv (fn[n] (create-employee-dummies)) (range 100))]
    (println "starting")
    (doseq [t news](batrel/insert! rvar t))
    (println "finished with: " (count (:emp_no @rvar)))))




(defn delete-mem-test-ps
  []
  (let [rvar (batrel/load-batvar "resources/bat-300024.db")
        ids-to-delete [44948 33758 18936 12585]]
    (println "starting")
    (doseq [id ids-to-delete]
      (let[ result (batrel/select (:emp_no  @rvar) = id)
            tuples (batrel/makeTable [] (keys @rvar) (vals (assoc @rvar :emp_no (batrel/join (batrel/mirror result) (:emp_no  @rvar) =))))]
        (doseq [t tuples] (batrel/delete! rvar t))))))




(defn delete-mem-test-as
  []
  (let [rvar (batrel/load-batvar "resources/bat-300024.db")]
    (println "starting")
    (let[ result (batrel/select (:gender  @rvar) not= "F")]
      (batrel/assign! rvar (assoc @rvar :gender (batrel/join (batrel/mirror result) (:gender  @rvar) =))))))




#_(defn search-mem-test-ps
  []
  (let [rvar (batrel/load-batvar "resources/bat-300024.db")
        ids-to-delete [44948 33758 18936 12585]]
    (println "starting")
    (doseq [id ids-to-delete]
      (let[ result (batrel/select (:emp_no  @rvar) = id)]
        (batrel/makeTable [] (keys @rvar) (vals (assoc @rvar :emp_no (batrel/join (batrel/mirror result) (:emp_no  @rvar) =))))))))


#_(defn search-mem-test-as
  []
  (let [rvar (batrel/load-batvar "resources/bat-300024.db")]
    (println "starting")
    (let[ result (batrel/select (:gender  @rvar) not= "F")]
      (batrel/makeTable [] (keys @rvar) (vals (assoc @rvar :gender (batrel/join (batrel/mirror result) (:gender  @rvar) =)))))))


(defn search-mem-test-ps
  []
  (let [rvar (batrel/load-batvar "resources/bat-300024.db")
        ids-to-delete [44948 33758 18936 12585]]
    (println "starting")
    (doseq [id ids-to-delete]
       (batrel/select (:emp_no  @rvar) = id))))


(defn search-mem-test-as
  []
  (let [rvar (batrel/load-batvar "resources/bat-300024.db")]
    (println "starting")
     (batrel/select (:gender  @rvar) not= "F")))


(defn join-mem-test-es
  []
  (let [emp_rvar (batrel/load-batvar "resources/bat-300024.db")
        sal_rvar (batrel/load-batvar "resources/bat-salaries.db")]
    (println "starting")
    (batrel/join  (:emp_no @emp_rvar)  (batrel/reverse (:emp_no @sal_rvar)) = )))


(defn join-mem-test-se
  []
  (let [emp_rvar (batrel/load-batvar "resources/bat-300024.db")
        sal_rvar (batrel/load-batvar "resources/bat-salaries.db")]
    (println "starting")
    (batrel/join  (:emp_no @sal_rvar)  (batrel/reverse (:emp_no @emp_rvar)) = )))



