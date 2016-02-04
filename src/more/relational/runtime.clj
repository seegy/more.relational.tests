(ns more.relational.runtime
  (:require [more.relational.transrelational]
            [more.relational.bat]
            [more.relational.hashRel])
  (:use criterium.core))


(def employees-data (take 10000 (set (read-string  (str "[" (slurp  "resources/employees.clj" ) "]" )))))
(def xrel (map #(zipmap [:emp_no :birth_date :first_name :last_name :gender :hire_date] %) employees-data))


(def result (criterium.core/benchmark (def hashRel-employees (more.relational.hashRel/rel xrel)) {:verbose true}))


(count (:samples result))
result
