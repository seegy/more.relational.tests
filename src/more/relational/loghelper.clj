(ns  more.relational.loghelper)


(require '[clojure.string :as str])

(defn to-millis [s]
  (let [number (re-find #"\d+,\d*" s)
        number (read-string  (str/replace number #"," "."))
        number (cond
                     (re-find #"µs" s) (/ number 1000)
                     (re-find #"sec" s) (* number 1000)
                       (re-find #"min" s) (* number 60000)
                       :else number)
                 ]
    (str/replace (str number) #"\." ",")))


(defn create-Parser
  [file]
  (let [file (slurp file)
        runs (drop 1 (str/split file #"\S+ - \S+ - creating\n\nSum of tupel in database:  \d+\n"))
        result (sequence (comp ;(map (fn[s] (first (str/split s #"\n\ncreating"))))
                               ;(map (fn[s] (second (str/split s (re-pattern (str "creating " tblname))))))
                               (map (fn[s] (map #(to-millis (second %)) (re-seq #"Execution time mean : (\d+,*\d* \S+)\n" s))))
                               (map (fn [[a b c d e f g h i j k l]] (str a "\t" b "\t" c "\t" d "\t" e "\t" f "\t" g "\t" h "\t" i "\t" j "\t" k "\t" l)))
                           ) runs)
         ]
    (conj result "Relation\tRelVar\tRelation\tRelVar\tRelation\tRelVar\tRelation\tRelVar\tRelation\tRelVar\tRelation\tRelVar" )))


(def filepath "/home/seegy/git/more.relational.tests/outs-2016-03-01-14:19:36/bat-create.out")


(defn FICKDICH
  [file]
  (let [file (slurp file)
        runs (drop 1 (str/split file #"\S+ - \S+ - search\n"))
        result (sequence (comp
                               (map (fn[s] (apply vector (map #(to-millis (second %)) (re-seq #"Execution time mean : (\d+,*\d* \S+)\n" s)))))
                               (map (fn [[a b c d e f g h i j k l m]] (str a "\t" b "\t" c "\t" d "\t" e "\t" f "\t" g "\t" h "\t" i "\t" j "\t" k "\t" l "\t"  m)))
                           ) runs)
         ]
    (conj result "pointsearch-key-bm-1\tpointsearch-key-bm-2\tpointsearch-key-bm-3\tpointsearch-key-bm-4\tpointsearch-no-key-bm-1\tpointsearch-no-key-bm-2\tpointsearch-no-key-bm-3\tpointsearch-no-key-bm-4\tareasearch-bm-1\tareasearch-bm-2\tareasearch-bm-3\tareasearch-bm-4\tareasearch-bm-5" )))







(defn bat-arsch
  [file]
  (let [file (slurp file)
        runs (drop 1 (str/split file #"\S+ - \S+ - search\n"))
        result (sequence (comp
                               (map (fn[s] (apply vector (map #(to-millis (second %)) (re-seq #"Execution time mean : (\d+,*\d* \S+)\n" s)))))
                               (map (fn [[a b c d e f g h i j k l m a2 b2 c2 d2 e2 f2 g2 h2 i2 j2 k2 l2 m2]] (str a "\t" b "\t" c "\t" d "\t" e "\t" f "\t" g "\t" h "\t" i "\t" j "\t" k "\t" l "\t"  m "\t"
                                                                                                                  a2  "\t" b2  "\t" c2  "\t" d2  "\t" e2  "\t" f2  "\t" g2  "\t" h2  "\t" i2  "\t" j2  "\t" k2  "\t" l2 "\t"  m2)))
                           ) runs)
         ]
    (conj result "pointsearch-key-bm-1\tpointsearch-key-bm-2\tpointsearch-key-bm-3\tpointsearch-key-bm-4\tpointsearch-no-key-bm-1\tpointsearch-no-key-bm-2\tpointsearch-no-key-bm-3\tpointsearch-no-key-bm-4\tareasearch-bm-1\tareasearch-bm-2\tareasearch-bm-3\tareasearch-bm-4\tareasearch-bm-5\tpointsearch-key-bm-1\tpointsearch-key-bm-2\tpointsearch-key-bm-3\tpointsearch-key-bm-4\tpointsearch-no-key-bm-1\tpointsearch-no-key-bm-2\tpointsearch-no-key-bm-3\tpointsearch-no-key-bm-4\tareasearch-bm-1\tareasearch-bm-2\tareasearch-bm-3\tareasearch-bm-4\tareasearch-bm-5" )))










(defn tr-arsch  [file]
  (let [file (slurp file)
        runs (drop 1 (str/split file #"\S+ - \S+ - search\n"))
        result (sequence (comp
                               (map (fn[s] (apply vector (map #(str/replace  (second %) #"\." ",") (re-seq #"\[(\d+\.+\d*)" s)))))
                               (map (fn [[a b c d e f g h i j k l m a2 b2 c2 d2 e2 f2 g2 h2 i2 j2 k2 l2 m2]] (str a "\t" b "\t" c "\t" d "\t" e "\t" f "\t" g "\t" h "\t" i "\t" j "\t" k "\t" l "\t"  m "\t"
                                                                                                                  a2  "\t" b2  "\t" c2  "\t" d2  "\t" e2  "\t" f2  "\t" g2  "\t" h2  "\t" i2  "\t" j2  "\t" k2  "\t" l2 "\t"  m2)))
                           ) runs)
         ]
    (conj result "pointsearch-key-bm-1\tpointsearch-key-bm-2\tpointsearch-key-bm-3\tpointsearch-key-bm-4\tpointsearch-no-key-bm-1\tpointsearch-no-key-bm-2\tpointsearch-no-key-bm-3\tpointsearch-no-key-bm-4\tareasearch-bm-1\tareasearch-bm-2\tareasearch-bm-3\tareasearch-bm-4\tareasearch-bm-5\tpointsearch-key-bm-1\tpointsearch-key-bm-2\tpointsearch-key-bm-3\tpointsearch-key-bm-4\tpointsearch-no-key-bm-1\tpointsearch-no-key-bm-2\tpointsearch-no-key-bm-3\tpointsearch-no-key-bm-4\tareasearch-bm-1\tareasearch-bm-2\tareasearch-bm-3\tareasearch-bm-4\tareasearch-bm-5" )))





(defn  joinparser
  [file]
  (let [file (slurp file)
        runs (drop 1 (str/split file #"\S+ - \S+ - join\n"))
        result (sequence (comp
                               (map (fn[s] (apply vector (map #(to-millis (second %)) (re-seq #"Execution time mean : (\d+,*\d* \S+)\n" s)))))
                               (map (fn[[a b c d e f g h i j k l m a2 b2 c2 d2 e2 f2 g2 h2 i2 j2 k2 l2 m2]] (str a "\t" b "\t" c "\t" d "\t" e "\t" f "\t" g "\t" h "\t" i "\t" j "\t" k "\t" l "\t"  m "\t"
                                                                                                                  a2  "\t" b2  "\t" c2  "\t" d2  "\t" e2  "\t" f2  "\t" g2  "\t" h2  "\t" i2  "\t" j2  "\t" k2  "\t" l2 "\t"  m2)))
                           ) runs)
         ]
    (conj result "join-bm-1\tjoin-bm-2\tjoin-bm-3\tjoin-bm-4\tjoin-bm-5\tjoin-bm-6\tjoin-bm-1\tjoin-bm-2\tjoin-bm-3\tjoin-bm-4\tjoin-bm-5\tjoin-bm-6" )))







(defn mani-arsch  [file]
  (let [file (slurp file)
        runs (drop-last 0 (str/split file #"gesamtzeit \d+.\d+\n"))
        result (sequence (comp
                               (map (fn[s] (read-string (second (first (re-seq #"#####4  (\[.+\])" s))))))
                               (map (fn [[a b c d e f g h i j k l ]] (str a "\t" b "\t" c "\t" d "\t" e "\t" f "\t" g "\t" h "\t" i "\t" j "\t" k "\t" l)))
                           ) runs)
         ]
    (conj result "insert-bm-1\tinsert-bm-2\tinsert-bm-3\tinsert-bm-4\tdelete-bm-1\tdelete-bm-2\tdelete-bm-3\tdelete-bm-4\tupdate-bm-1\tupdate-bm-2\tupdate-bm-3\tupdate-bm-4" )))
















