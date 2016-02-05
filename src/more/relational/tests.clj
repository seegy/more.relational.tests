(ns more.relational.tests
  (:require [more.relational.runtime])
  (:gen-class))

(defn -main
  "I don't do a whole lot ... yet."
  [& args]
  (let [quick? (if (contains? (set args) "-q") true false)
        verbose? (if (contains? (set args) "-v") true false)]
    (more.relational.runtime/do-criterium-testing verbose? quick?)))
