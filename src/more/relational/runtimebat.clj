(ns more.relational.runtimebat
  (:require [more.relational.bat :as batrel])
  (:use criterium.core))


(defn load-raw-data
  []
  (let [employees-data (read-string (slurp  "resources/employees.clj-dump" ))
        salaries-data  (read-string (slurp  "resources/salaries.clj-dump" ))
        departments-data  (read-string (slurp  "resources/departments.clj-dump" ))
        titles-data  (read-string (slurp  "resources/titles.clj-dump" ))
        dept_emp-data  (read-string (slurp  "resources/dept_emp.clj-dump" ))
        dept_manager-data  (read-string (slurp  "resources/dept_manager.clj-dump" ))]

  {:xrel-emp (sort-by :emp_no  employees-data)
   :xrel-sal (sort-by :emp_no  salaries-data)
   :xrel-department departments-data
   :xrel-titles (sort-by :emp_no  titles-data)
   :xrel-dept_emp (sort-by :emp_no  dept_emp-data)
   :xrel-dept_manager (sort-by :emp_no  dept_manager-data)}))



(defn- get_emps_by_manager
  [manxrel empxrel empcount]
  (let [empno_in_man (into #{} (map #(:emp_no %) manxrel))
        manager (into #{} (filter #(contains? empno_in_man (:emp_no %)) empxrel))
        not_manger (clojure.set/difference (set empxrel) manager)
        ]
    (apply conj manager (take (- empcount (count manager)) not_manger))))



(defn- empno_filter
  [emp_nos xrel]
   (filterv #(contains? emp_nos (:emp_no %)) xrel))

(defn randomID [] (+ 10000000 (rand-int 100000)))



(defmacro my-time
  "Evaluates expr and prints the time it took.  Returns the value of expr."
  [expr]
  `(let [start# (. System (nanoTime))
         ret# ~expr]
     [ret# (/ (double (- (. System (nanoTime)) start#)) 1000000.0)]))


(defn average
  [numbers]
    (/ (apply + numbers) (count numbers)))

(defmacro my-quickbenchmark
  [expr n]
  `(let [result-and-runtimes# (mapv (fn[x#](my-time ~expr)) (range ~n))
         runtimes# (map second result-and-runtimes#)
         avg# (average runtimes#)
         closest-under-avg# (apply max (filter #(<= % avg#) runtimes#))
         closest-upper-avg# (apply min (filter #(>= % avg#) runtimes#))]
    {:results (map first result-and-runtimes#)
     :sample-mean [avg# (list closest-under-avg# closest-upper-avg#)]}))




(defn create-employee-database
  [base-count]
  (let [raw-data (load-raw-data)
        employees-max-count (count (:xrel-emp raw-data))
        employee-count (if (> base-count employees-max-count) employees-max-count base-count)
        xrel-dept-man (:xrel-dept_manager raw-data)
        xrel-emp (get_emps_by_manager xrel-dept-man (:xrel-emp raw-data) base-count)
        emp_nos (into #{} (map #(:emp_no %) xrel-emp))
        emp-relvar (batrel/batvar (batrel/convertToBats [:emp_no :birth_date :first_name :last_name :gender :hire_date] (take employee-count xrel-emp)) {:key :emp_no})
        dept-relvar (batrel/batvar (batrel/convertToBats [:dept_no :dept_name] (:xrel-department raw-data)) {:key :dept_no})
        xrel-sal (empno_filter emp_nos (:xrel-sal raw-data))
        xrel-titles (empno_filter emp_nos (:xrel-titles raw-data))
        xrel-dept-emp (empno_filter emp_nos (:xrel-dept_emp raw-data))
        sal-relvar (batrel/batvar (batrel/convertToBats [:emp_no :salary :from_date :to_date] xrel-sal) #{{:key #{:emp_no, :from_date}}
                                                                                                       {:foreign-key {:key :emp_no,
                                                                                                                      :referenced-relvar emp-relvar,
                                                                                                                      :referenced-key :emp_no}}})

        title-relvar  (batrel/batvar (batrel/convertToBats [:emp_no :title :from_date] xrel-titles) #{{:key #{:emp_no, :title, :from_date}}
                                                                                                       {:foreign-key {:key :emp_no,
                                                                                                                      :referenced-relvar emp-relvar,
                                                                                                                      :referenced-key :emp_no}}})

        dept-man-relvar  (batrel/batvar (batrel/convertToBats [:dept_no :emp_no :from_date] xrel-dept-man) #{{:key #{:emp_no, :dept_no}}
                                                                                                       {:foreign-key {:key :emp_no,
                                                                                                                      :referenced-relvar emp-relvar,
                                                                                                                      :referenced-key :emp_no}}
                                                                                                       {:foreign-key {:key :dept_no,
                                                                                                                      :referenced-relvar dept-relvar,
                                                                                                                      :referenced-key :dept_no}} })

        dept-emp-relvar  (batrel/batvar (batrel/convertToBats [:emp_no :dept_no :from_date] xrel-dept-emp) #{{:key #{:emp_no, :dept_no, :from_date}}
                                                                                                       {:foreign-key {:key :emp_no,
                                                                                                                      :referenced-relvar emp-relvar,
                                                                                                                      :referenced-key :emp_no}}
                                                                                                       {:foreign-key {:key :dept_no,
                                                                                                                      :referenced-relvar dept-relvar,
                                                                                                                      :referenced-key :dept_no}} })]


    {:employee emp-relvar,
     :salaries sal-relvar
     :department dept-relvar
     :titles title-relvar
     :department_manager dept-man-relvar
     :department_employee dept-emp-relvar}))




(defn test-create-employee-database
  [base-count]
  (let [raw-data (load-raw-data)
        employees-max-count (count (:xrel-emp raw-data))
        employee-count (if (> base-count employees-max-count) employees-max-count base-count)
        xrel-dept-man (:xrel-dept_manager raw-data)
        xrel-emp (get_emps_by_manager xrel-dept-man (:xrel-emp raw-data) base-count)
        emp_nos (into #{} (map #(:emp_no %) xrel-emp))
        emp-relvar (batrel/batvar (batrel/convertToBats [:emp_no :birth_date :first_name :last_name :gender :hire_date]  (take employee-count xrel-emp)) {:key :emp_no})
        dept-relvar (batrel/batvar (batrel/convertToBats [:dept_no :dept_name] (:xrel-department raw-data)) {:key :dept_no})
        xrel-sal (empno_filter emp_nos (:xrel-sal raw-data))
        xrel-titles (empno_filter emp_nos (:xrel-titles raw-data))
        xrel-dept-emp (empno_filter emp_nos (:xrel-dept_emp raw-data))
        emp_rel (batrel/convertToBats  (take employee-count xrel-emp))
        dep_rel (batrel/convertToBats  (:xrel-department raw-data))
        sal_rel (batrel/convertToBats [:emp_no :salary :from_date :to_date] xrel-sal)
        tit_rel (batrel/convertToBats [:emp_no :title :from_date] xrel-titles)
        dep_mal_rel (batrel/convertToBats [:dept_no :emp_no :from_date] xrel-dept-man)
        dep_emp_rel (batrel/convertToBats [:emp_no :dept_no :from_date] xrel-dept-emp)
        ]

    (println "\nSum of tupel in database: " (+ employee-count 9 (count xrel-sal) (count xrel-titles) (count xrel-dept-man) (count xrel-dept-emp)))
    (println "\ncreating employees " employee-count )
    (criterium.core/quick-bench  (batrel/convertToBats  (take employee-count xrel-emp)) )
    (criterium.core/quick-bench (batrel/batvar emp_rel{:key :emp_no}))

    (println "\ncreating department "  9 )

    (criterium.core/quick-bench  (batrel/convertToBats  (:xrel-department raw-data)) )
    (criterium.core/quick-bench  (batrel/batvar dep_rel {:key :dept_no}) )

    (println "\ncreating salaris with " (count xrel-sal)             " foreign-keys:"  )
    (criterium.core/quick-bench       (batrel/convertToBats [:emp_no :salary :from_date :to_date] xrel-sal))
    (criterium.core/quick-bench (batrel/batvar sal_rel #{{:key #{:emp_no, :from_date}}
                                                                                                       {:foreign-key {:key :emp_no,
                                                                                                                      :referenced-relvar emp-relvar,
                                                                                                                      :referenced-key :emp_no}}}) )

    (println "\ncreating title with " (count xrel-titles)            " foreign-keys:"  )
    (criterium.core/quick-bench       (batrel/convertToBats [:emp_no :title :from_date] xrel-titles))
    (criterium.core/quick-bench (batrel/batvar tit_rel #{{:key #{:emp_no, :title, :from_date}}
                                                                                                       {:foreign-key {:key :emp_no,
                                                                                                                      :referenced-relvar emp-relvar,
                                                                                                                      :referenced-key :emp_no}}}) )

    (println "\ncreating dept-mananger with " (count xrel-dept-man)  " foreign-keys:"  )
    (criterium.core/quick-bench      (batrel/convertToBats [:dept_no :emp_no :from_date] xrel-dept-man))
    (criterium.core/quick-bench (batrel/batvar dep_mal_rel #{{:key #{:emp_no, :dept_no}}
                                                                                                       {:foreign-key {:key :emp_no,
                                                                                                                      :referenced-relvar emp-relvar,
                                                                                                                      :referenced-key :emp_no}}
                                                                                                       {:foreign-key {:key :dept_no,
                                                                                                                      :referenced-relvar dept-relvar,
                                                                                                                      :referenced-key :dept_no}} }) )

    (println "\ncreating dept_employees with " (count xrel-dept-emp)      " foreign-keys:"  )
    (criterium.core/quick-bench      (batrel/convertToBats [:emp_no :dept_no :from_date] xrel-dept-emp))
    (criterium.core/quick-bench (batrel/batvar dep_emp_rel #{{:key #{:emp_no, :dept_no, :from_date}}
                                                                                                       {:foreign-key {:key :emp_no,
                                                                                                                      :referenced-relvar emp-relvar,
                                                                                                                      :referenced-key :emp_no}}
                                                                                                       {:foreign-key {:key :dept_no,
                                                                                                                      :referenced-relvar dept-relvar,
                                                                                                                      :referenced-key :dept_no}} }) )))


(defn employee-operation-tests
  [database]
  (let[
       employee @(:employee database)
       salaries @(:salaries database)
       titles @(:titles database)
       department_manager @(:department_manager database)
       department_employee @(:department_employee database)
       department @(:department database)

       xrel-emp (batrel/makeTable! (:employee database))
       first-tupel (first xrel-emp)
       middle-tupel (nth (seq xrel-emp) (long (/  (count employee) 2)))
       last-tupel (last xrel-emp)

       middle-tupel-sal (nth (batrel/makeTable! (:salaries database)) (long (/  (count salaries) 2)))]

    (println "\npointsearch-key-bm-1: ")
    (criterium.core/quick-bench  (batrel/select (:emp_no employee) = (:emp_no first-tupel)))

    (println "\npointsearch-key-bm-2: ")
    (criterium.core/quick-bench (batrel/select (:emp_no employee) = (:emp_no middle-tupel)))

    (println "\npointsearch-key-bm-3: ")
    (criterium.core/quick-bench (batrel/select (:emp_no employee) = (:emp_no last-tupel)))

    (println "\npointsearch-key-bm-3: ")
    (criterium.core/quick-bench (batrel/select (:emp_no employee) = 1499999))


    (println "\npointsearch-no-key-bm-1: ")
    (criterium.core/quick-bench (let [new-birth-date (batrel/select (:birth_date employee) = (:birth_date first-tupel))
                                      new-last-name (batrel/select (:last_name employee) = (:last_name first-tupel))
                                      new-first-name (batrel/select (:first_name employee) = (:first_name first-tupel))]))

    (println "\npointsearch-no-key-bm-2: " )
    (criterium.core/quick-bench (let [new-gender     (batrel/select (:gender employee) = (:gender middle-tupel))
                                      new-last-name  (batrel/select (:last_name employee) = (:last_name middle-tupel))
                                      new-first-name (batrel/select (:first_name employee) = (:first_name middle-tupel))]))


    (println "\npointsearch-no-key-bm-3: " )
    (criterium.core/quick-bench (let [new-birth-date (batrel/select (:birth_date employee) = (:birth_date middle-tupel))
                                      new-last-name  (batrel/select (:last_name employee) = (:last_name middle-tupel))
                                      new-first-name (batrel/select (:first_name employee) = (:first_name middle-tupel))]))

    (println "\npointsearch-no-key-bm-3: " )
    (criterium.core/quick-bench (let[new-birth-date (batrel/select (:birth_date employee) = "XXXXXXX")
                                      new-last-name  (batrel/select (:last_name employee) = "YYYYYYYY")
                                      new-first-name (batrel/select (:first_name employee) = "ZZZZZZ")]))

    (println "\nareasearch-bm-1: " )
    (criterium.core/quick-bench  (let [new-emp-no (batrel/select (:emp_no employee) > (:emp_no first-tupel))]))

    (println "\nareasearch-bm-2: " )
    (criterium.core/quick-bench (let [new-emp-no (batrel/select (:emp_no employee) < (:emp_no last-tupel))]))

    (println "\nareasearch-bm-3: " )
    (criterium.core/quick-bench (let [new-gender (batrel/select (:gender employee) = "F")]))

    (println "\nareasearch-bm-4: " )
    (criterium.core/quick-bench (let [new-gender (batrel/select (:gender employee) not= "F")]))

    (println "\nareasearch-bm-5: " )
    (criterium.core/quick-bench (let [new-salary (batrel/select (:salary salaries) >= (:salary middle-tupel-sal))]))
    ))



(defn employee-operation-tests-with-result-set
  [database]
  (let[
       employee @(:employee database)
       salaries @(:salaries database)
       titles @(:titles database)
       department_manager @(:department_manager database)
       department_employee @(:department_employee database)
       department @(:department database)

       xrel-emp (batrel/makeTable! (:employee database))
       first-tupel (first xrel-emp)
       middle-tupel (nth (seq xrel-emp) (long (/  (count employee) 2)))
       last-tupel (last xrel-emp)

       middle-tupel-sal (nth (batrel/makeTable! (:salaries database)) (long (/  (count salaries) 2)))]

    (println "\npointsearch-key-bm-1: ")
    (criterium.core/quick-bench  (let[ result (batrel/select (:emp_no employee) = (:emp_no first-tupel))]
                                   (batrel/makeTable [] (keys employee) (vals (assoc employee :emp_no (batrel/join (batrel/mirror result) (:emp_no employee) =))))))

    (println "\npointsearch-key-bm-2: ")
    (criterium.core/quick-bench (let[result (batrel/select (:emp_no employee) = (:emp_no middle-tupel))]
                                  (batrel/makeTable [] (keys employee) (vals (assoc employee :emp_no (batrel/join (batrel/mirror result) (:emp_no employee) =))))))

    (println "\npointsearch-key-bm-3: ")
    (criterium.core/quick-bench (let[result (batrel/select (:emp_no employee) = (:emp_no last-tupel))]
                                (batrel/makeTable  [](keys employee) (vals (assoc employee :emp_no (batrel/join (batrel/mirror result) (:emp_no employee) =))))))

    (println "\npointsearch-key-bm-3: ")
    (criterium.core/quick-bench (let[result (batrel/select (:emp_no employee) = 1499999)]
                                  (batrel/makeTable [] (keys employee) (vals (assoc employee :emp_no (batrel/join (batrel/mirror result) (:emp_no employee) =))))))


    (println "\npointsearch-no-key-bm-1: ")
    (criterium.core/quick-bench (let [new-birth-date (batrel/mirror (batrel/select (:birth_date employee) = (:birth_date first-tupel)))
                                      new-last-name (batrel/mirror (batrel/select (:last_name employee) = (:last_name first-tupel)))
                                      new-first-name (batrel/mirror (batrel/select (:first_name employee) = (:first_name first-tupel)))
                                      result (batrel/join (batrel/join new-birth-date new-last-name =) new-first-name =) ]
                                  (batrel/makeTable [] (keys employee) (vals (assoc employee :emp_no (batrel/join result (:emp_no employee) =))))))

    (println "\npointsearch-no-key-bm-2: " )
    (criterium.core/quick-bench (let [new-gender     (batrel/select (:gender employee) = (:gender middle-tupel))
                                      new-last-name  (batrel/select (:last_name employee) = (:last_name middle-tupel))
                                      new-first-name (batrel/select (:first_name employee) = (:first_name middle-tupel))
                                      result (batrel/join (batrel/join new-gender new-last-name =) new-first-name =)]
                                  (batrel/makeTable [] (keys employee) (vals (assoc employee :emp_no (batrel/join result (:emp_no employee) =))))))


    (println "\npointsearch-no-key-bm-3: " )
    (criterium.core/quick-bench (let [new-birth-date (batrel/select (:birth_date employee) = (:birth_date middle-tupel))
                                      new-last-name  (batrel/select (:last_name employee) = (:last_name middle-tupel))
                                      new-first-name (batrel/select (:first_name employee) = (:first_name middle-tupel))
                                      result (batrel/join (batrel/join new-birth-date new-last-name =) new-first-name =) ]
                                  (batrel/makeTable [] (keys employee) (vals (assoc employee :emp_no (batrel/join result (:emp_no employee) =))))))

    (println "\npointsearch-no-key-bm-3: " )
    (criterium.core/quick-bench (let[new-birth-date (batrel/select (:birth_date employee) = "XXXXXXX")
                                      new-last-name  (batrel/select (:last_name employee) = "YYYYYYYY")
                                      new-first-name (batrel/select (:first_name employee) = "ZZZZZZ")
                                      result (batrel/join (batrel/join new-birth-date new-last-name =) new-first-name =) ]
                                  (batrel/makeTable  [](keys employee) (vals (assoc employee :emp_no (batrel/join result (:emp_no employee) =))))))

    (println "\nareasearch-bm-1: " )
    (criterium.core/quick-bench  (let [new-emp-no (batrel/select (:emp_no employee) > (:emp_no first-tupel))]
                                   (batrel/makeTable [] (keys employee) (vals (assoc employee :emp_no (batrel/join (batrel/mirror new-emp-no) (:emp_no employee) =))))))

    (println "\nareasearch-bm-2: " )
    (criterium.core/quick-bench (let [new-emp-no (batrel/select (:emp_no employee) < (:emp_no last-tupel))]
                                   (batrel/makeTable [] (keys employee) (vals (assoc employee :emp_no (batrel/join (batrel/mirror new-emp-no) (:emp_no employee) =))))))

    (println "\nareasearch-bm-3: " )
    (criterium.core/quick-bench (let [new-gender (batrel/select (:gender employee) = "F")]
                                   (batrel/makeTable [] (keys employee) (vals (assoc employee :emp_no (batrel/join (batrel/mirror new-gender) (:emp_no employee) =))))))

    (println "\nareasearch-bm-4: " )
    (criterium.core/quick-bench (let [new-gender (batrel/select (:gender employee) not= "F")]
                                   (batrel/makeTable [] (keys employee) (vals (assoc employee :emp_no (batrel/join (batrel/mirror new-gender) (:emp_no employee) =))))))

    (println "\nareasearch-bm-5: " )
    (criterium.core/quick-bench (let [new-salary (batrel/select (:salary salaries) >= (:salary middle-tupel-sal))]
                                   (batrel/makeTable [] (keys salaries) (vals (assoc employee :emp_no (batrel/join (batrel/mirror new-salary) (:emp_no salaries) =))))))
    ))



(defn employee-join-tests
  [database]
  (let [
       employee @(:employee database)
       salaries @(:salaries database)
       titles @(:titles database)
       department_manager @(:department_manager database)
       department_employee @(:department_employee database)
       department @(:department database) ]
    (println "\njoin-bm-1: " )
    (criterium.core/quick-bench (batrel/join  (:emp_no employee) (batrel/reverse (:emp_no salaries)) = ))

    (println "\njoin-bm-2: " )
    (criterium.core/quick-bench (batrel/join  (:emp_no salaries) (batrel/reverse (:emp_no employee))  = ))

    (println "\njoin-bm-3: " )
    (criterium.core/quick-bench (batrel/join  (:emp_no titles) (batrel/reverse (:emp_no employee)) = ))

    (println "\njoin-bm-4: " )
    (criterium.core/quick-bench (batrel/join  (:dept_no department_manager) (batrel/reverse (:dept_no department)) = ))

    (println "\njoin-bm-5: " )
    (criterium.core/quick-bench (let [depman-empl (batrel/join (:emp_no department_manager) (batrel/reverse (:emp_no employee)) =)
                                      dep-depman  (batrel/join (:dept_no department) (batrel/reverse (:emp_no department_manager)) =)]))

    (println "\njoin-bm-6: " )
    (criterium.core/quick-bench (let [depman-empl (batrel/join (:emp_no department_manager) (batrel/reverse (:emp_no employee)) =)
                                      dep-depman  (batrel/join (:dept_no department) (batrel/reverse (:dept_no department_manager)) =)
                                      title-emp (batrel/join  (:emp_no titles) (batrel/reverse (:emp_no employee)) = )
                                      emp_sal (batrel/join  (:emp_no employee) (batrel/reverse (:emp_no salaries)) = )
                                      empl_dep-empl (batrel/join  (:emp_no employee) (batrel/reverse (:emp_no department_employee)) = )
                                      dep_dep-empl (batrel/join (:dept_no department) (batrel/reverse (:dept_no department_employee)) =)
                                      ]))))


(defn employee-join-tests-with-result-set
  [database]
  (let [
       employee @(:employee database)
       salaries @(:salaries database)
       titles @(:titles database)
       department_manager @(:department_manager database)
       department_employee @(:department_employee database)
       department @(:department database) ]
    (println "\njoin-bm-1: " )
    (criterium.core/quick-bench (let [ sal_emp_ids (batrel/join  (:emp_no salaries)  (batrel/reverse (:emp_no employee)) = )
                                       new_emp (into salaries (map (fn [[k v]] [(keyword (str "emp" (name k))) (batrel/join sal_emp_ids v =)]) employee))]
                                          (apply batrel/makeTable [] (keys new_emp) (vals new_emp))))

    (println "\njoin-bm-2: " )
    (criterium.core/quick-bench (let [ sal_emp_ids (batrel/join  (:emp_no salaries)  (batrel/reverse (:emp_no employee)) = )
                                       new_emp (into salaries (map (fn [[k v]] [(keyword (str "emp" (name k))) (batrel/join sal_emp_ids v =)]) employee))]
                                  (batrel/makeTable [] (keys new_emp) (vals new_emp))))

    (println "\njoin-bm-3: " )
    (criterium.core/quick-bench (let [ tit_emp_ids (batrel/join  (:emp_no titles) (batrel/reverse (:emp_no employee)) = )
                                       new_emp (into titles (map (fn [[k v]] [(keyword (str "emp" (name k))) (batrel/join tit_emp_ids v =)]) employee))]
                                  (batrel/makeTable [] (keys new_emp) (vals new_emp))))

    (println "\njoin-bm-4: " )
    (criterium.core/quick-bench (let [ depman_dep_ids (batrel/join  (:dept_no department_manager) (batrel/reverse (:dept_no department)) = )
                                       new (into department_manager (map (fn [[k v]] [(keyword (str "dep_" (name k))) (batrel/join depman_dep_ids v =)]) department))]
                                  (batrel/makeTable [] (keys new) (vals new))))

    (println "\njoin-bm-5: " )
    (criterium.core/quick-bench (let [depman-empl (batrel/join (:emp_no department_manager) (batrel/reverse (:emp_no employee)) =)
                                      depman_dep_ids (batrel/join  (:dept_no department_manager) (batrel/reverse (:dept_no department)) = )
                                      new (into department_manager (map (fn [[k v]] [(keyword (str "emp" (name k))) (batrel/join depman-empl v =)]) employee))
                                      new (into new (map (fn [[k v]] [(keyword (str "dep" (name k))) (batrel/join depman_dep_ids v =)]) department))]
                                  (batrel/makeTable [] (keys new) (vals new))))

    (println "\njoin-bm-6: " )
    (criterium.core/quick-bench (let [
                                      depman_dep_ids (batrel/join  (:dept_no department_manager) (batrel/reverse (:dept_no department)) = )
                                     new-depman (into department_manager (map (fn [[k v]] [(keyword (str "dep" (name k))) (batrel/join depman_dep_ids v =)]) department))

                                     depman-empl (batrel/join (:emp_no employee) (batrel/reverse (:emp_no department_manager)) =)
                                     new_emp (into employee (map (fn [[k v]] [(keyword (str "emp" (name k))) (batrel/join depman-empl v =)]) new-depman))

                                      emp_tit (batrel/join  (:emp_no employee) (batrel/reverse (:emp_no titles)) = )
                                      new_emp (into new_emp (map (fn[[k v]] [(keyword (str "tit_" (name k))) (batrel/join emp_tit v =)]) titles))

                                      emp_sal (batrel/join  (:emp_no employee) (batrel/reverse (:emp_no salaries)) = )
                                      new_emp (into new_emp (map (fn[[k v]] [(keyword (str "sal_" (name k))) (batrel/join emp_sal v =)]) salaries))

                                       dep_dep-empl (batrel/join (:dept_no department) (batrel/reverse (:dept_no department_employee)) =)
                                     new-depemp (into department_employee (map (fn [[k v]] [(keyword (str "dep" (name k))) (batrel/join dep_dep-empl v =)]) department))

                                      empl_dep-empl (batrel/join  (:emp_no employee) (batrel/reverse (:emp_no department_employee)) = )
                                      new_emp (into new_emp (map (fn[[k v]] [(keyword (str "depemp_" (name k))) (batrel/join empl_dep-empl v =)]) new-depemp)) ]
                                        (batrel/makeTable [] (keys new_emp) (vals new_emp))))))






(defn employee-manipulation
  [database]
  (let [first-employee (first (batrel/makeTable! (:employee database)))
         employee @(:employee database)
       salaries @(:salaries database)
       titles @(:titles database)
       department_manager @(:department_manager database)
       department_employee @(:department_employee database)
       department @(:department database)
        insert-bm-1 (my-quickbenchmark  (batrel/insert! (:employee database) {:emp_no (randomID), :birth_date "1958-02-19", :first_name "Saniya", :last_name "Kalloufi", :gender "M", :hire_date "1994-09-15"}) 6)
        insert-bm-2 (my-quickbenchmark  (batrel/insert! (:titles database) {:emp_no (:emp_no first-employee), :title (str  (randomID)), :from_date "YYYYYYY"}) 6)
        insert-bm-3 (my-quickbenchmark  (batrel/insert! (:salaries database) {:emp_no (:emp_no first-employee), :salary 10000000, :from_date (str (randomID)), :to_date "1998-02-08"}) 6)
        insert-bm-4 (my-quickbenchmark  (batrel/insert! (:department_employee database) {:emp_no (:emp_no first-employee), :dept_no "d008" :from_date (str (randomID))}) 6)


        delete-bm-1 (my-quickbenchmark (let[ result (batrel/select (:salary salaries) > 53383)
                                             tuples (batrel/makeTable [] (keys salaries) (vals (assoc salaries :emp_no (batrel/join (batrel/mirror result) (:emp_no salaries) =))))]
                                   (map #(batrel/delete! salaries %) tuples) )1)

        delete-bm-2 (my-quickbenchmark (let[ result (batrel/select (:emp_no titles) > (:emp_no first-employee))
                                             tuples (batrel/makeTable [] (keys titles) (vals (assoc titles :emp_no (batrel/join (batrel/mirror result) (:emp_no titles) =))))]
                                   (map #(batrel/delete! titles %) tuples) )1)

        delete-bm-3 (my-quickbenchmark (let[ result (batrel/select (:title titles) = "Senior Staff")
                                             tuples (batrel/makeTable [] (keys titles) (vals (assoc titles :emp_no (batrel/join (batrel/mirror result) (:emp_no titles) =))))]
                                   (map #(batrel/delete! titles %) tuples) )1)

        delete-bm-4 (my-quickbenchmark (let[ tuples (batrel/makeTable!  (:department_employee database))]
                                   (map #(batrel/delete! department_employee %) tuples) )1)



        update-bm-1 (my-quickbenchmark (batrel/update! (:employee database)  :gender "F" "M") 1)

        update-bm-2 (my-quickbenchmark (let[ result (batrel/select (:emp_no salaries) = (:emp_no first-employee))
                                             to-delete (batrel/join (batrel/mirror result) (:salary salaries) = ) ]
                                         (map #(batrel/update! salaries :salary (:head %) (:tail %) 1) to-delete) )1)

        update-bm-3 (my-quickbenchmark (let[ result (batrel/select (:dept_no department) = "d008")
                                             to-delete (batrel/join (batrel/mirror result) (:dept_name department) = ) ]
                                         (map #(batrel/update! department :dept_name (:head %) (:tail %) "") to-delete) )1)

        update-bm-4 (my-quickbenchmark (let[ result (batrel/select (:emp_no employee) = (:emp_no first-employee))
                                             to-delete (batrel/join (batrel/mirror result) (:first_name employee) = ) ]
                                         (map #(batrel/update! department :first_name (:head %) (:tail %) "XXXXXXXX") to-delete) )1)


        ]
    (println "\ninsert-bm-1: " (:sample-mean insert-bm-1))
    (println "\ninsert-bm-2: " (:sample-mean insert-bm-2))
    (println "\ninsert-bm-3: " (:sample-mean insert-bm-3))
    (println "\ninsert-bm-4: " (:sample-mean insert-bm-4))


    (println "\ndelete-bm-1: " (:sample-mean delete-bm-1))
    (println "\ndelete-bm-2: " (:sample-mean delete-bm-2))
    (println "\ndelete-bm-3: " (:sample-mean delete-bm-3))
    (println "\ndelete-bm-4: " (:sample-mean delete-bm-4))

    (println "\nupdate-bm-1: " (:sample-mean update-bm-1))
    (println "\nupdate-bm-2: " (:sample-mean update-bm-2))
    (println "\nupdate-bm-3: " (:sample-mean update-bm-3))
    (println "\nupdate-bm-4: " (:sample-mean update-bm-4))
    ))



(defn employee-test
  [employee-count]
  (println "\nbat - employee")
  (test-create-employee-database employee-count)
  (let [database (create-employee-database employee-count)]
    (employee-operation-tests database)
    (employee-operation-tests-with-result-set database)
    (employee-join-tests database)
    (employee-join-tests-with-result-set database)
    (employee-manipulation database)
    ))


(defn creation-test
  [employee-count]
  (println "\nbat - employee - creating")
  (test-create-employee-database employee-count))

(defn search-test
  [employee-count]
  (println "\nbat - employee - search")
  (let[database (create-employee-database employee-count)]
    (println "\n without resolving to useable data:")
    (employee-operation-tests database)
    (println "\n with resolving to useable data:")
    (employee-operation-tests-with-result-set database)))

(defn join-test
  [employee-count]
  (println "\nbat - employee - join")
  (let[database (create-employee-database employee-count)]
    (println "\n without resolving to useable data:")
    (employee-join-tests database)
    (println "\n with resolving to useable data:")
    (employee-join-tests-with-result-set database)))

(defn manipilation-test
  [employee-count]
  (println "\nbat - employee - manipulation")
  (employee-manipulation (create-employee-database employee-count)))


