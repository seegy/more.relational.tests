(ns more.relational.memorybat
   (:require [more.relational.bat :as batrel])
  (:use [criterium.core :as crit])
  (:use [more.relational.tools]))



(def employees (load-raw-data-employees))




(defn lulu
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



;(def a (batrel/batvar (batrel/convertToBats [:emp_no :birth_date :first_name :last_name :gender :hire_date] (take 100000 employees))))


