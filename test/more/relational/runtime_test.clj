(ns more.relational.runtime-test
  (:require [clojure.test :refer :all]
            [more.relational.tests :refer :all]
            [more.relational.transrelational]
            [more.relational.bat]
            [more.relational.hashRel]))





(def employees-data (take 10000 (set (read-string  (str "[" (slurp  "resources/employees.clj" ) "]" )))))
(def xrel (map #(zipmap [:emp_no :birth_date :first_name :last_name :gender :hire_date] %) employees-data))


(def salaries-data (take 100000  (set (read-string  (str "[" (slurp  "resources/salaries.clj" ) "]" )))))
(def xrel-sal (map #(zipmap [:emp_no :salary :from_date :to_date] %) salaries-data))


(println "\n\nCreating relations")
(time (def hashRel-employees (more.relational.hashRel/rel xrel)))
(time (def bat-employees (more.relational.bat/convertToBats xrel)))
(time (def tr-employees (more.relational.transrelational/tr xrel)))

(time (def hashRel-salaries (more.relational.hashRel/rel xrel-sal)))
(time (def bat-salaries (more.relational.bat/convertToBats xrel-sal)))
(time (def tr-salaries (more.relational.transrelational/tr xrel-sal)))



; #################### Punktsuche

(println "\n\nPunktsuche")

(time (more.relational.hashRel/restrict hashRel-employees (more.relational.hashRel/relfn [t] (= (:emp_no t) 485652 ))))

(time (let [id (time (more.relational.bat/join (more.relational.bat/mirror (more.relational.bat/select (:emp_no bat-employees) = 485652)) (:emp_no bat-employees) =))]
       ))


(time (more.relational.transrelational/restriction tr-employees (more.relational.transrelational/restrict-fn [t] (= (:emp_no t) 485652))))




;################# Bereichssuche


(println "\n\nBereichssuche 1")

(count (time (more.relational.hashRel/restrict hashRel-employees (more.relational.hashRel/relfn [t] (= (:gender t) "F" )))))

(time (let [females (time (more.relational.bat/join (more.relational.bat/mirror (more.relational.bat/select (:gender bat-employees) = "F")) (:gender bat-employees) =))]
        ))


(time (more.relational.transrelational/restriction tr-employees (more.relational.transrelational/restrict-fn [t] (= (:gender t) "F"))))



(println "\n\nBereichssuche 2")

(time (more.relational.hashRel/restrict hashRel-employees (more.relational.hashRel/relfn [t] (and (= (:gender t) "F" ) (= "1952-11-09" (:birth_date t))))))
(time (let [females (time (more.relational.bat/join (more.relational.bat/mirror (more.relational.bat/select (:gender bat-employees) = "F"))
                                             (:gender bat-employees) =))
            birth (time (more.relational.bat/join (more.relational.bat/mirror (more.relational.bat/select (:birth_date bat-employees) = "1952-11-09"))
                                           (:birth_date bat-employees) =))]
        ))


  (time (more.relational.transrelational/restriction tr-employees (more.relational.transrelational/restrict-fn [t] (and (= (:gender t) "F" ) (= "1952-11-09" (:birth_date t))))))




(println "\n\nBereichssuche 3")

(count (time (more.relational.hashRel/restrict hashRel-employees (more.relational.hashRel/relfn [t] (and (and (= (:gender t) "M" )
                                                              (= "1952-11-09" (:birth_date t)))
                                                         (or  (= (:first_name t) "Genta")
                                                              (> (:emp_no t) 35000)))))))

(time (let [females (more.relational.bat/select (:gender bat-employees) = "M")
            birth  (more.relational.bat/select (:birth_date bat-employees) = "1952-11-09")
            first-name (more.relational.bat/select (:first_name bat-employees) = "Genta")
            id (more.relational.bat/select (:emp_no bat-employees) > 35000)
            all (more.relational.bat/intersect (more.relational.bat/intersect females birth) (more.relational.bat/union first-name id))]
              (more.relational.bat/join (more.relational.bat/mirror all)  (:emp_no bat-employees) =)))


(time (more.relational.transrelational/restriction tr-employees (more.relational.transrelational/restrict-fn [t] (and (and (= (:gender t) "M" )
                                                              (= "1952-11-09" (:birth_date t)))
                                                         (or (= (:first_name t) "Genta")
                                                              (> (:emp_no t) 35000))))))


; ########### mani

(def toInsert {:emp_no 0, :birth_date "", :first_name "", :last_name "", :gender "", :hire_date ""})


(println "\n\nInsert")

(time (more.relational.hashRel/union hashRel-employees (more.relational.hashRel/rel toInsert)))

(time (let[ newId (inc (apply clojure.core/max (map (fn[[_ bat]] (more.relational.bat/max (more.relational.bat/reverse bat))) bat-employees)))]
    (into {} (map (fn[[name value]] [name (more.relational.bat/insert (get bat-employees name) newId value)]) toInsert))))

(time (more.relational.transrelational/insert tr-employees toInsert))



(println "\n\nDelete")

(time (more.relational.hashRel/difference hashRel-employees (more.relational.hashRel/rel {:emp_no 16574, :birth_date "1963-05-06", :first_name "Nevio", :last_name "Penz", :gender "M", :hire_date "1990-08-23"})))

(time (let[todelete (into {} (map (fn[attr] [attr [ 3655 (more.relational.bat/find (get bat-employees attr) 3655) ]]) (keys bat-employees))) ]
   (into {} (map (fn [[attr [h t]]] [attr (more.relational.bat/delete (get bat-employees attr) h t )] ) todelete))))

(time (more.relational.transrelational/delete tr-employees 0 0))










; ###### join

(println "\n\nJoin")

(count (time (more.relational.hashRel/join hashRel-employees hashRel-salaries)))

(count (time (let [join-table (more.relational.bat/join (:emp_no bat-employees) (more.relational.bat/reverse (:emp_no bat-salaries)) = )] join-table)))

(count (time (more.relational.transrelational/join tr-employees tr-salaries)))
