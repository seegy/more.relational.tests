(ns more.relational.tests
  (:require [more.relational.runtimehashrel]
            [more.relational.runtimetr]
            [more.relational.runtimebat]
            [more.relational.memoryhash]
            [more.relational.memorybat]
            [more.relational.memorytr])
  (:gen-class))

(defn -main
  "I don't do a whole lot ... yet."
  [& args]
  (let [start (. System (nanoTime))
        all? (if (contains? (set args) "all") true false)
        hashrel? (if (contains? (set args) "hashrel") true false)
        batrel?  (if (contains? (set args) "bat") true false)
        tr?  (if (contains? (set args) "tr") true false)
        employee? (if (contains? (set args) "runtime") true false)
        creating? (if (contains? (set args) "creating") true false)
        search? (if (contains? (set args) "search") true false)
        join? (if (contains? (set args) "join") true false)
        manipulation? (if (contains? (set args) "manipulation") true false)
        tuple-count (if (contains? (set args) "-c") (read-string (nth args (inc (.indexOf args "-c")))) 1000)
        wait-for-start-signal? (if (contains? (set args) "-s") true false)
        mem? (if (contains? (set args) "mem") true false)
        delete? (if (contains? (set args) "delete") true false)
        ps? (if (contains? (set args) "ps") true false)
        as? (if (contains? (set args) "as") true false)
        new? (if (contains? (set args) "new") true false)
        dup? (if (contains? (set args) "dup") true false)]
    (when wait-for-start-signal?
      (do (print "Press return to start process...") (flush) (read-line)))

    (when (and hashrel? employee?)
      (when all?
        (more.relational.runtimehashrel/employee-test tuple-count))
      (when creating?
        (more.relational.runtimehashrel/creation-test tuple-count))
      (when search?
        (more.relational.runtimehashrel/search-test tuple-count))
      (when join?
        (more.relational.runtimehashrel/join-test tuple-count))
      (when manipulation?
        (more.relational.runtimehashrel/manipilation-test tuple-count)))


    (when  (and batrel? employee?)
       (when all?
        (more.relational.runtimebat/employee-test tuple-count))
      (when creating?
        (more.relational.runtimebat/creation-test tuple-count))
      (when search?
        (more.relational.runtimebat/search-test tuple-count))
      (when join?
        (more.relational.runtimebat/join-test tuple-count))
      (when manipulation?
        (more.relational.runtimebat/manipilation-test tuple-count)))

     (when  (and tr? employee?)
       (when all?
        (more.relational.runtimetr/employee-test tuple-count))
      (when creating?
        (more.relational.runtimetr/creation-test tuple-count))
      (when search?
        (more.relational.runtimetr/search-test tuple-count))
      (when join?
        (more.relational.runtimetr/join-test tuple-count))
      (when manipulation?
        (more.relational.runtimetr/manipilation-test tuple-count)))

    (when (and hashrel? mem?)
      (when creating?
        (when new?
          (more.relational.memoryhash/insert-mem-test-new))
        (when dup?
          (more.relational.memoryhash/insert-mem-test-dup)))

      (when delete?
        (when ps?
          (more.relational.memoryhash/delete-mem-test-ps))
        (when as?
          (more.relational.memoryhash/delete-mem-test-as)))

      (when search?
        (when ps?
          (more.relational.memoryhash/search-mem-test-ps))
        (when as?
          (more.relational.memoryhash/search-mem-test-as))))

    (when (and batrel? mem?)
      (when creating?
        (when new?
          (more.relational.memorybat/insert-mem-test-new))
        (when dup?
          (more.relational.memorybat/insert-mem-test-dup)))

      (when delete?
        (when ps?
          (more.relational.memorybat/delete-mem-test-ps))
        (when as?
          (more.relational.memorybat/delete-mem-test-as)))

      (when search?
        (when ps?
          (more.relational.memorybat/search-mem-test-ps))
        (when as?
          (more.relational.memorybat/search-mem-test-as))))

    (when (and tr? mem?)
      (when creating?
        (when new?
          (more.relational.memorytr/insert-mem-test-new))
        (when dup?
          (more.relational.memorytr/insert-mem-test-dup)))

      (when delete?
        (when ps?
          (more.relational.memorytr/delete-mem-test-ps))
        (when as?
          (more.relational.memorytr/delete-mem-test-as)))

      (when search?
        (when ps?
          (more.relational.memorytr/search-mem-test-ps))
        (when as?
          (more.relational.memorytr/search-mem-test-as))))


    (println "gesamtzeit" (/ (double (- (. System (nanoTime)) start)) 1000000000.0))))



;(-main "tr" "employee" "all" "-c"  "1000000")
