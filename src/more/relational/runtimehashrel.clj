(ns more.relational.runtimehashrel
  (:require [more.relational.hashRel :as hashrel])
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
        emp-relvar (hashrel/relvar (hashrel/rel [:emp_no :birth_date :first_name :last_name :gender :hire_date] (take employee-count xrel-emp)) {:key :emp_no})
        dept-relvar (hashrel/relvar (hashrel/rel [:dept_no :dept_name] (:xrel-department raw-data)) {:key :dept_no})
        xrel-sal (empno_filter emp_nos (:xrel-sal raw-data))
        xrel-titles (empno_filter emp_nos (:xrel-titles raw-data))
        xrel-dept-emp (empno_filter emp_nos (:xrel-dept_emp raw-data))
        sal-relvar (hashrel/relvar (hashrel/rel [:emp_no :salary :from_date :to_date] xrel-sal) #{{:key #{:emp_no, :from_date}}
                                                                                                       {:foreign-key {:key :emp_no,
                                                                                                                      :referenced-relvar emp-relvar,
                                                                                                                      :referenced-key :emp_no}}})

        title-relvar  (hashrel/relvar (hashrel/rel [:emp_no :title :from_date] xrel-titles) #{{:key #{:emp_no, :title, :from_date}}
                                                                                                       {:foreign-key {:key :emp_no,
                                                                                                                      :referenced-relvar emp-relvar,
                                                                                                                      :referenced-key :emp_no}}})

        dept-man-relvar  (hashrel/relvar (hashrel/rel [:dept_no :emp_no :from_date] xrel-dept-man) #{{:key #{:emp_no, :dept_no}}
                                                                                                       {:foreign-key {:key :emp_no,
                                                                                                                      :referenced-relvar emp-relvar,
                                                                                                                      :referenced-key :emp_no}}
                                                                                                       {:foreign-key {:key :dept_no,
                                                                                                                      :referenced-relvar dept-relvar,
                                                                                                                      :referenced-key :dept_no}} })

        dept-emp-relvar  (hashrel/relvar (hashrel/rel [:emp_no :dept_no :from_date] xrel-dept-emp) #{{:key #{:emp_no, :dept_no, :from_date}}
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
        emp-relvar (hashrel/relvar (hashrel/rel [:emp_no :birth_date :first_name :last_name :gender :hire_date] (take employee-count xrel-emp)) {:key :emp_no})
        dept-relvar (hashrel/relvar (hashrel/rel [:dept_no :dept_name] (:xrel-department raw-data)) {:key :dept_no})
        xrel-sal (empno_filter emp_nos (:xrel-sal raw-data))
        xrel-titles (empno_filter emp_nos (:xrel-titles raw-data))
        xrel-dept-emp (empno_filter emp_nos (:xrel-dept_emp raw-data))
        emp_rel(hashrel/rel [:emp_no :birth_date :first_name :last_name :gender :hire_date] (take employee-count xrel-emp))
        dep_rel(hashrel/rel [:dept_no :dept_name] (:xrel-department raw-data))
        sal_rel(hashrel/rel [:emp_no :salary :from_date :to_date] xrel-sal)
        tit_rel(hashrel/rel [:emp_no :title :from_date] xrel-titles)
        dep_man_rel(hashrel/rel [:dept_no :emp_no :from_date] xrel-dept-man)
        dep_emp_rel(hashrel/rel [:emp_no :dept_no :from_date] xrel-dept-emp)
        ]

    (println "\nSum of tupel in database: " (+ employee-count 9 (count xrel-sal) (count xrel-titles) (count xrel-dept-man) (count xrel-dept-emp)))
    (println "\ncreating employees " employee-count )
    (criterium.core/quick-bench (hashrel/rel [:emp_no :birth_date :first_name :last_name :gender :hire_date] (take employee-count xrel-emp)))
    (criterium.core/quick-bench (hashrel/relvar emp_rel {:key :emp_no}))

    (println "\ncreating department "  9 )
    (criterium.core/quick-bench (hashrel/rel [:dept_no :dept_name] (:xrel-department raw-data)))
    (criterium.core/quick-bench  (hashrel/relvar dep_rel {:key :dept_no}) )

    (println "\ncreating salaris with " (count xrel-sal)             " foreign-keys:"  )
    (criterium.core/quick-bench (hashrel/rel [:emp_no :salary :from_date :to_date] xrel-sal))
    (criterium.core/quick-bench (hashrel/relvar sal_rel #{{:key #{:emp_no, :from_date}}
                                                                                                       {:foreign-key {:key :emp_no,
                                                                                                                      :referenced-relvar emp-relvar,
                                                                                                                      :referenced-key :emp_no}}}) )

    (println "\ncreating title with " (count xrel-titles)            " foreign-keys:"  )
    (criterium.core/quick-bench (hashrel/rel [:emp_no :title :from_date] xrel-titles))
    (criterium.core/quick-bench (hashrel/relvar tit_rel #{{:key #{:emp_no, :title, :from_date}}
                                                                                                       {:foreign-key {:key :emp_no,
                                                                                                                      :referenced-relvar emp-relvar,
                                                                                                                      :referenced-key :emp_no}}}) )

    (println "\ncreating dept-mananger with " (count xrel-dept-man)  " foreign-keys:"  )
    (criterium.core/quick-bench (hashrel/rel [:dept_no :emp_no :from_date] xrel-dept-man))
    (criterium.core/quick-bench (hashrel/relvar dep_man_rel #{{:key #{:emp_no, :dept_no}}
                                                                                                       {:foreign-key {:key :emp_no,
                                                                                                                      :referenced-relvar emp-relvar,
                                                                                                                      :referenced-key :emp_no}}
                                                                                                       {:foreign-key {:key :dept_no,
                                                                                                                      :referenced-relvar dept-relvar,
                                                                                                                      :referenced-key :dept_no}} }) )

    (println "\ncreating dept_employees with " (count xrel-dept-emp)      " foreign-keys:"  )
    (criterium.core/quick-bench (hashrel/rel [:emp_no :dept_no :from_date] xrel-dept-emp))
    (criterium.core/quick-bench (hashrel/relvar dep_emp_rel #{{:key #{:emp_no, :dept_no, :from_date}}
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

       first-tupel (first employee)
       middle-tupel (nth (seq employee) (long (/  (count employee) 2)))
       last-tupel (last employee)

       middle-tupel-sal (nth (seq salaries) (long (/  (count salaries) 2)))]

    (println "\npointsearch-key-bm-1: ")
    (criterium.core/quick-bench  (hashrel/restrict employee #(= (:emp_no %) (:emp_no first-tupel))) )

    (println "\npointsearch-key-bm-2: ")
    (criterium.core/quick-bench (hashrel/restrict employee #(= (:emp_no %) (:emp_no middle-tupel))) )

    (println "\npointsearch-key-bm-3: ")
    (criterium.core/quick-bench (hashrel/restrict employee #(= (:emp_no %) (:emp_no last-tupel))) )

    (println "\npointsearch-key-bm-3: ")
    (criterium.core/quick-bench (hashrel/restrict employee #(= (:emp_no %) 1499999)) )


    (println "\npointsearch-no-key-bm-1: ")
    (criterium.core/quick-bench (hashrel/restrict employee #(and (= (:birth_date %) (:birth_date first-tupel)) (= (:last_name %) (:last_name first-tupel)) (= (:first_name  %) (:first_name first-tupel)))))

    (println "\npointsearch-no-key-bm-2: " )
    (criterium.core/quick-bench (hashrel/restrict employee #(and (= (:gender %) (:gender middle-tupel)) (= (:last_name %) (:last_name middle-tupel))(= (:first_name  %) (:first_name  middle-tupel)))))

    (println "\npointsearch-no-key-bm-3: " )
    (criterium.core/quick-bench (hashrel/restrict employee #(and (= (:birth_date %) (:birth_date last-tupel)) (= (:last_name %) (:last_name last-tupel))(= (:first_name  %) (:first_name  last-tupel)))) )

    (println "\npointsearch-no-key-bm-3: " )
    (criterium.core/quick-bench (hashrel/restrict employee #(and (= (:birth_date %) "XXXXXXX") (= (:last_name %) "YYYYYYYY")(= (:first_name  %) "ZZZZZZ"))) )


    (println "\nareasearch-bm-1: " )
    (criterium.core/quick-bench  (hashrel/restrict employee #(> (:emp_no %) (:emp_no first-tupel)))  )

    (println "\nareasearch-bm-2: " )
    (criterium.core/quick-bench  (hashrel/restrict employee #(< (:emp_no %) (:emp_no last-tupel))))

    (println "\nareasearch-bm-3: " )
    (criterium.core/quick-bench  (hashrel/restrict employee #(= (:gender %) "F")) )

    (println "\nareasearch-bm-4: " )
    (criterium.core/quick-bench  (hashrel/restrict employee #(not (= (:gender %) "F"))) )

    (println "\nareasearch-bm-5: " )
    (criterium.core/quick-bench  (hashrel/restrict salaries #(>= (:salary %) (:salary middle-tupel-sal))) )
    ))

;(criterium.core/bench (str "foo" "bar" "baz"))


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
    (criterium.core/quick-bench (hashrel/join  employee salaries) )

    (println "\njoin-bm-2: " )
    (criterium.core/quick-bench (hashrel/join  salaries employee) )

    (println "\njoin-bm-3: " )
    (criterium.core/quick-bench (hashrel/join  titles employee) )

    (println "\njoin-bm-4: " )
    (criterium.core/quick-bench (hashrel/join  department_manager department) )

    (println "\njoin-bm-5: " )
    (criterium.core/quick-bench (hashrel/join (hashrel/join department_manager employee) department) )

    (println "\njoin-bm-6: " )
    (criterium.core/quick-bench (->> employee
                                     (hashrel/join department_employee)
                                     (hashrel/join titles)
                                     (hashrel/join department)
                                     (hashrel/join salaries)
                                     (hashrel/join department_manager)))
    ))


(defn employee-manipulation
  [database]
  (let [first-employee (first @(:employee database))
        insert-bm-1 (my-quickbenchmark  (hashrel/insert! (:employee database) {:emp_no (randomID), :birth_date "1958-02-19", :first_name "Saniya", :last_name "Kalloufi", :gender "M", :hire_date "1994-09-15"}) 6)
        insert-bm-2 (my-quickbenchmark  (hashrel/insert! (:titles database) {:emp_no (:emp_no first-employee), :title (randomID), :from_date "YYYYYYY"}) 6)
        insert-bm-3 (my-quickbenchmark  (hashrel/insert! (:salaries database) {:emp_no (:emp_no first-employee), :salary 10000000, :from_date (randomID), :to_date "1998-02-08"}) 6)
        insert-bm-4 (my-quickbenchmark  (hashrel/insert! (:department_employee database) {:emp_no (:emp_no first-employee), :dept_no "d008" :from_date (randomID)}) 6)


        delete-bm-1 (my-quickbenchmark (hashrel/delete! (:salaries database) (hashrel/relfn [t](> (:salary t) 53383))) 1)
        delete-bm-2 (my-quickbenchmark (hashrel/delete! (:titles database) (hashrel/relfn [t](> (:emp_no t) (:emp_no first-employee)))) 1)
        delete-bm-3 (my-quickbenchmark (hashrel/delete! (:titles database) (hashrel/relfn [t](= (:title t) "Senior Staff"))) 1)
        delete-bm-4 (my-quickbenchmark (hashrel/delete! (:department_employee database) (hashrel/relfn [t] true)) 1)


        update-bm-1 (my-quickbenchmark (hashrel/update! (:employee database) (hashrel/relfn [t] (= (:gender t) "F")) :gender "M") 1)
        update-bm-2 (my-quickbenchmark (hashrel/update! (:salaries database) (hashrel/relfn [t](= (:emp_no t) (:emp_no first-employee))) :salary 1) 1)
        update-bm-3 (my-quickbenchmark (hashrel/update! (:department database) (hashrel/relfn [t](= (:dept_no t) "d008")) :dept_name "" ) 1)
        update-bm-4 (my-quickbenchmark (hashrel/update! (:employee database) (hashrel/relfn [t](= (:emp_no t) (:emp_no first-employee))) :first_name "XXXXXXXX") 1)


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
  (println "\nhashrel - employee")
  (test-create-employee-database employee-count)
  (let [database (create-employee-database employee-count)]
    (employee-operation-tests database)
    (employee-join-tests database)
    (employee-manipulation database)
    ))


(defn creation-test
  [employee-count]
  (println "\nhashrel - employee - creating")
  (test-create-employee-database employee-count))

(defn search-test
  [employee-count]
  (println "\nhashrel - employee - search")
  (employee-operation-tests (create-employee-database employee-count)))

(defn join-test
  [employee-count]
  (println "\nhashrel - employee - join")
  (employee-join-tests (create-employee-database employee-count)))

(defn manipilation-test
  [employee-count]
  (println "\nhashrel - employee - manipulation")
  (employee-manipulation (create-employee-database employee-count)))


