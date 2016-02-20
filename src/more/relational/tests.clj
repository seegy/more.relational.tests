(ns more.relational.tests
  (:require [more.relational.runtime])
  (:gen-class))

(defn -main
  "I don't do a whole lot ... yet."
  [& args]
  (let [all? (if (contains? (set args) "all") true false)
        hashrel? (if (contains? (set args) "hashrel") true false)
        employee? (if (contains? (set args) "employee") true false)
        tuple-count (if (contains? (set args) "-c") (read-string (nth args (inc (.indexOf args "-c")))) 1000)
        wait-for-start-signal? (if (contains? (set args) "-s") true false)]
    (when wait-for-start-signal?
      (do (print "Press return to start process...") (flush) (read-line)))
    (when (or all? (and hashrel? employee?))
      (more.relational.runtime/hashrel-employee-test tuple-count))))

