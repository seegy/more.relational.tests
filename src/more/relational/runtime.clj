(ns more.relational.runtime
  (:require [more.relational.transrelational]
            [more.relational.bat]
            [more.relational.hashRel])
  (:use criterium.core))


(def employees-data (set (read-string  (str "[" (slurp  "resources/employees.clj" ) "]" ))))
(def xrel (map #(zipmap [:emp_no :birth_date :first_name :last_name :gender :hire_date] %) employees-data))


(def creating-tuple-counter [0 1000 5000 10000 50000 100000 300000])

(defn do-creating-tests
  []
;  {:hashrel (reduce (fn[m counter] (assoc m counter (first (:sample-mean (criterium.core/quick-benchmark (more.relational.hashRel/rel (take counter xrel)) {:verbose true})))))
;            {} creating-tuple-counter)
;   :batrel (reduce (fn[m counter] (assoc m counter (criterium.core/quick-benchmark (more.relational.bat/convertToBats (take counter xrel)) {:verbose true})))
;            {} creating-tuple-counter)
;   :transrel (reduce (fn[m counter] (assoc m counter (criterium.core/quick-benchmark (more.relational.transrelational/tr (take counter xrel)) {:verbose true})))
;            {} creating-tuple-counter)}
  )




(defn do-criterium-testing
  [verbose? quick?]
  (let [create-results (do-creating-tests)]
  create-results))


(def something (do-criterium-testing true true))

;(use '(incanter core charts))

