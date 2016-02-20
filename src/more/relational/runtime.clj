(ns more.relational.runtime
  (:require [more.relational.transrelational]
            [more.relational.bat]
            [more.relational.hashRel :as hashrel])
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


(defn- empno_filter
  [emp_no xrel]
  (vec (take-while #(<= (:emp_no %) emp_no) xrel)))

(defn create-hashrel-employee-database
  [base-count]
  (let [raw-data (load-raw-data)
        employees-max-count (count (:xrel-emp raw-data))
        employee-count (if (> base-count employees-max-count) employees-max-count base-count)
        xrel-emp (take employee-count (:xrel-emp raw-data))
        employee-benchmark (criterium.core/quick-benchmark (hashrel/relvar (hashrel/rel [:emp_no :birth_date :first_name :last_name :gender :hire_date] (take employee-count xrel-emp)) {:key :emp_no}) {:verbose true})
        department-benchmark (criterium.core/quick-benchmark  (hashrel/relvar (hashrel/rel [:dept_no :dept_name] (:xrel-department raw-data)) {:key :dept_no}) {:verbose true})
        emp-relvar (-> employee-benchmark :results first)
        dept-relvar (-> department-benchmark :results first)
        xrel-sal (empno_filter (:emp_no (last xrel-emp)) (:xrel-sal raw-data))
        xrel-titles (empno_filter (:emp_no (last xrel-emp)) (:xrel-titles raw-data))
        xrel-dept-man (empno_filter (:emp_no (last xrel-emp)) (:xrel-dept_manager raw-data))
        xrel-dept-emp (empno_filter (:emp_no (last xrel-emp)) (:xrel-dept_emp raw-data))
        salaries-benchmark (criterium.core/quick-benchmark (hashrel/relvar (hashrel/rel [:emp_no :salary :from_date :to_date] xrel-sal) #{{:key #{:emp_no, :from_date}}
                                                                                                       {:foreign-key {:key :emp_no,
                                                                                                                      :referenced-relvar emp-relvar,
                                                                                                                      :referenced-key :emp_no}}}) {:verbose true})

        titles-benchmark (criterium.core/quick-benchmark (hashrel/relvar (hashrel/rel [:emp_no :title :from_date] xrel-titles) #{{:key #{:emp_no, :title, :from_date}}
                                                                                                       {:foreign-key {:key :emp_no,
                                                                                                                      :referenced-relvar emp-relvar,
                                                                                                                      :referenced-key :emp_no}}}) {:verbose true})

        dept-man-benchmark (criterium.core/quick-benchmark (hashrel/relvar (hashrel/rel [:dept_no :emp_no :from_date] xrel-dept-man) #{{:key #{:emp_no, :dept_no}}
                                                                                                       {:foreign-key {:key :emp_no,
                                                                                                                      :referenced-relvar emp-relvar,
                                                                                                                      :referenced-key :emp_no}}
                                                                                                       {:foreign-key {:key :dept_no,
                                                                                                                      :referenced-relvar dept-relvar,
                                                                                                                      :referenced-key :dept_no}} }) {:verbose true})

        dept-emp-benchmark (criterium.core/quick-benchmark (hashrel/relvar (hashrel/rel [:emp_no :dept_no :from_date] xrel-dept-emp) #{{:key #{:emp_no, :dept_no}}
                                                                                                       {:foreign-key {:key :emp_no,
                                                                                                                      :referenced-relvar emp-relvar,
                                                                                                                      :referenced-key :emp_no}}
                                                                                                       {:foreign-key {:key :dept_no,
                                                                                                                      :referenced-relvar dept-relvar,
                                                                                                                      :referenced-key :dept_no}} }) {:verbose true})


        sal-relvar (-> salaries-benchmark :results first)
        title-relvar (-> titles-benchmark :results first)
        dept-man-relvar (-> dept-man-benchmark :results first)
        dept-emp-relvar (-> dept-emp-benchmark :results first)]

        (println "Sum of tupel in database: " (+ employee-count 9 (count xrel-sal) (count xrel-titles) (count xrel-dept-man) (count xrel-dept-emp)))
        (println "creating employees " employee-count (:sample-mean employee-benchmark))
        (println "creating department "  9 (:sample-mean department-benchmark))
        (println "creating salaris with " (count xrel-sal)             " foreign-keys:"  (:sample-mean salaries-benchmark))
        (println "creating title with " (count xrel-titles)            " foreign-keys:"  (:sample-mean titles-benchmark))
        (println "creating dept-mananger with " (count xrel-dept-man)  " foreign-keys:"  (:sample-mean dept-man-benchmark))
        (println "creating dept_employees with " (count xrel-dept-emp)      " foreign-keys:"  (:sample-mean dept-emp-benchmark))

    {:employee emp-relvar,
         :salaries sal-relvar
         :department dept-relvar
         :titles title-relvar
         :department_manager dept-man-relvar
         :department_employee dept-emp-relvar}))


(defn hashrel-employee-operation-tests
  [database]
  (let[first-tupel (first @(:employee database))
       middle-tupel (nth (seq @(:employee database)) (long (/  (count @(:employee database)) 2)))
       last-tupel (last @(:employee database))

       middle-tupel-sal (nth (seq @(:salaries database)) (long (/  (count @(:salaries database)) 2)))

       pointsearch-key-bm-1 (criterium.core/quick-benchmark  (hashrel/restrict @(:employee database) #(= (:emp_no %) (:emp_no first-tupel)))  {:verbose true})
       pointsearch-key-bm-2 (criterium.core/quick-benchmark (hashrel/restrict @(:employee database) #(= (:emp_no %) (:emp_no middle-tupel))) {:verbose true})
       pointsearch-key-bm-3 (criterium.core/quick-benchmark (hashrel/restrict @(:employee database) #(= (:emp_no %) (:emp_no last-tupel))) {:verbose true})
       pointsearch-key-bm-4 (criterium.core/quick-benchmark (hashrel/restrict @(:employee database) #(= (:emp_no %) 1499999)) {:verbose true})

       pointsearch-no-key-bm-1 (criterium.core/quick-benchmark (hashrel/restrict @(:employee database) #(and (= (:birth_date %) (:birth_date first-tupel)) (= (:last_name %) (:last_name first-tupel)) (= (:first_name  %) (:first_name first-tupel)))) {:verbose true})
       pointsearch-no-key-bm-2 (criterium.core/quick-benchmark (hashrel/restrict @(:employee database) #(and (= (:gender %) (:gender middle-tupel)) (= (:last_name %) (:last_name middle-tupel))(= (:first_name  %) (:first_name  middle-tupel)))) {:verbose true})
       pointsearch-no-key-bm-3 (criterium.core/quick-benchmark (hashrel/restrict @(:employee database) #(and (= (:birth_date %) (:birth_date last-tupel)) (= (:last_name %) (:last_name last-tupel))(= (:first_name  %) (:first_name  last-tupel)))) {:verbose true})
       pointsearch-no-key-bm-4 (criterium.core/quick-benchmark (hashrel/restrict @(:employee database) #(and (= (:birth_date %) "XXXXXXX") (= (:last_name %) "YYYYYYYY")(= (:first_name  %) "ZZZZZZ"))) {:verbose true})

       areasearch-bm-1 (criterium.core/quick-benchmark  (hashrel/restrict @(:employee database) #(> (:emp_no %) (:emp_no first-tupel)))  {:verbose true})
       areasearch-bm-2 (criterium.core/quick-benchmark  (hashrel/restrict @(:employee database) #(< (:emp_no %) (:emp_no last-tupel)))  {:verbose true})
       areasearch-bm-3 (criterium.core/quick-benchmark  (hashrel/restrict @(:employee database) #(= (:gender %) "F"))  {:verbose true})
       areasearch-bm-4 (criterium.core/quick-benchmark  (hashrel/restrict @(:employee database) #(not (= (:gender %) "F")))  {:verbose true})
       areasearch-bm-5 (criterium.core/quick-benchmark  (hashrel/restrict @(:salaries database) #(>= (:salary %) (:salary middle-tupel-sal)))  {:verbose true})


       join-bm-1  (criterium.core/quick-benchmark (hashrel/join  @(:employee database) @(:salaries database)) {:verbose true})
       join-bm-2  (criterium.core/quick-benchmark (hashrel/join  @(:salaries database) @(:employee database)) {:verbose true})
       join-bm-3  (criterium.core/quick-benchmark (hashrel/join  @(:titles database) @(:employee database)) {:verbose true})
       join-bm-4  (criterium.core/quick-benchmark (hashrel/join  @(:department_manager database) @(:department database)) {:verbose true})
       join-bm-5  (criterium.core/quick-benchmark (hashrel/join (hashrel/join  @(:department_manager database) @(:employee database)) @(:department database))  {:verbose true})
       join-bm-5  (criterium.core/quick-benchmark (->> @(:employee database)
                                                       (hashrel/join @(:department_employee database))
                                                       (hashrel/join @(:titles database))
                                                       (hashrel/join @(:department database))
                                                       (hashrel/join @(:salaries database))
                                                       (hashrel/join @(:department_manager database)))  {:verbose true})
      ]

    (println "pointsearch-key-bm-1: " (:sample-mean pointsearch-key-bm-1))
    (println "pointsearch-key-bm-2: " (:sample-mean pointsearch-key-bm-2))
    (println "pointsearch-key-bm-3: " (:sample-mean pointsearch-key-bm-3))
    (println "pointsearch-key-bm-3: " (:sample-mean pointsearch-key-bm-4))

    (println "pointsearch-no-key-bm-1: " (:sample-mean pointsearch-no-key-bm-1))
    (println "pointsearch-no-key-bm-2: " (:sample-mean pointsearch-no-key-bm-2))
    (println "pointsearch-no-key-bm-3: " (:sample-mean pointsearch-no-key-bm-3))
    (println "pointsearch-no-key-bm-3: " (:sample-mean pointsearch-no-key-bm-4))

    (println "areasearch-bm-1: " (:sample-mean areasearch-bm-1))
    (println "areasearch-bm-2: " (:sample-mean areasearch-bm-2))
    (println "areasearch-bm-3: " (:sample-mean areasearch-bm-3))
    (println "areasearch-bm-4: " (:sample-mean areasearch-bm-4))
    (println "areasearch-bm-5: " (:sample-mean areasearch-bm-5))

    (println "join-bm-1: " (:sample-mean join-bm-1))
    (println "join-bm-2: " (:sample-mean join-bm-2))
    (println "join-bm-3: " (:sample-mean join-bm-3))
    (println "join-bm-4: " (:sample-mean join-bm-4))
    (println "join-bm-5: " (:sample-mean join-bm-5))
    ))



(defn hashrel-employee-manipulation
  [database]
  (let [first-employee (first @(:employee database))
        insert-bm-1 (criterium.core/quick-benchmark  (hashrel/insert! (:employee database) {:emp_no 1, :birth_date "1958-02-19", :first_name "Saniya", :last_name "Kalloufi", :gender "M", :hire_date "1994-09-15"})  {:verbose true})
        insert-bm-2 (criterium.core/quick-benchmark  (hashrel/insert! (:titles database) {:emp_no (:emp_no first-employee), :title "XXXXXXX", :from_date "YYYYYYY"})  {:verbose true})
        insert-bm-3 (criterium.core/quick-benchmark  (hashrel/insert! (:salaries database) {:emp_no (:emp_no first-employee), :salary 10000000, :from_date "YYYYYYYYY", :to_date "1998-02-08"})  {:verbose true})
        insert-bm-4 (criterium.core/quick-benchmark  (hashrel/insert! (:department_manager database) {:emp_no (:emp_no first-employee), :dept_no "d008" :from_date "XXXXXXX"})  {:verbose true})
        ]
    (println "insert-bm-1: " (:sample-mean insert-bm-1))
    (println "insert-bm-2: " (:sample-mean insert-bm-2))
    (println "insert-bm-3: " (:sample-mean insert-bm-3))
    (println "insert-bm-4: " (:sample-mean insert-bm-4))
    ))



(defn hashrel-employee-test
  [employee-count]
  (println "hashrel - employee")
  (let [database (create-hashrel-employee-database employee-count)]
    (hashrel-employee-operation-tests database)
    ;(hashrel-employee-manipulation database)))

