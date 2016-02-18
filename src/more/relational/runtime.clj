(ns more.relational.runtime
  (:require [more.relational.transrelational]
            [more.relational.bat]
            [more.relational.hashRel :as hashrel])
  (:use criterium.core))


(def employees-data (set (read-string  (str "[" (slurp  "resources/employees.clj" ) "]" ))))
(def xrel-emp (sort-by :emp_no (map #(zipmap [:emp_no :birth_date :first_name :last_name :gender :hire_date] %) employees-data)))

(def salaries-data (take 100000  (set (read-string  (str "[" (slurp  "resources/salaries.clj" ) "]" )))))
(def xrel-sal (sort-by :emp_no (map #(zipmap [:emp_no :salary :from_date :to_date] %) salaries-data)))

(def employees-max-count (count employees-data))
(def salaries-max-count (count salaries-data))

(def employee-constraints #{ {:key :emp_no}})
(def salaries-constraints #{ {:key #{:emp_no, :from_date}}})

(defn hashrel-employee-test
  [employee-count]
  (let [employee-count (if (> employee-count employees-max-count) employees-max-count employee-count)
        xrel-emp (take employee-count xrel-emp)
        xrel-sal (take-while #(<= (:emp_no %) (:emp_no (last xrel-emp))) xrel-sal)
        emp-relvar (hashrel/relvar (hashrel/rel xrel-emp) employee-constraints)
        sal-relvar (hashrel/relvar (hashrel/rel xrel-sal) (conj salaries-constraints {:foreign-key {:key :emp_no, :referenced-relvar emp-relvar, :referenced-key :emp_no}}))]
    (println "ready")))

(hashrel-employee-test employees-max-count)


#_(defn do-creating-tests
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

