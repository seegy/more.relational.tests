(ns more.relational.tests
  (:gen-class))

(defn -main
  "I don't do a whole lot ... yet."
  [& args]
  (let [quick? (if (contains? (set args) "-q") true false)
        verbose? (if (contains? (set args) "-v") true false)]
    (println "quick?=" quick?)))
