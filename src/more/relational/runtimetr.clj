(ns more.relational.runtimetr
  (:require [more.relational.transrelational :as trel])
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

(defn uuid [] (str (java.util.UUID/randomUUID)))



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
        xrel-emp (take employee-count (:xrel-emp raw-data))
        emp-relvar (trel/transvar (trel/tr [:emp_no :birth_date :first_name :last_name :gender :hire_date] (take employee-count xrel-emp)) {:key :emp_no})
        dept-relvar (trel/transvar (trel/tr [:dept_no :dept_name] (:xrel-department raw-data)) {:key :dept_no})
        xrel-sal (empno_filter (:emp_no (last xrel-emp)) (:xrel-sal raw-data))
        xrel-titles (empno_filter (:emp_no (last xrel-emp)) (:xrel-titles raw-data))
        xrel-dept-man (empno_filter (:emp_no (last xrel-emp)) (:xrel-dept_manager raw-data))
        xrel-dept-emp (empno_filter (:emp_no (last xrel-emp)) (:xrel-dept_emp raw-data))
        sal-relvar (trel/transvar (trel/tr [:emp_no :salary :from_date :to_date] xrel-sal) #{{:key #{:emp_no, :from_date}}
                                                                                                       {:foreign-key {:key :emp_no,
                                                                                                                      :referenced-relvar emp-relvar,
                                                                                                                      :referenced-key :emp_no}}})

        title-relvar  (trel/transvar (trel/tr [:emp_no :title :from_date] xrel-titles) #{{:key #{:emp_no, :title, :from_date}}
                                                                                                       {:foreign-key {:key :emp_no,
                                                                                                                      :referenced-relvar emp-relvar,
                                                                                                                      :referenced-key :emp_no}}})

        dept-man-relvar  (trel/transvar (trel/tr [:dept_no :emp_no :from_date] xrel-dept-man) #{{:key #{:emp_no, :dept_no}}
                                                                                                       {:foreign-key {:key :emp_no,
                                                                                                                      :referenced-relvar emp-relvar,
                                                                                                                      :referenced-key :emp_no}}
                                                                                                       {:foreign-key {:key :dept_no,
                                                                                                                      :referenced-relvar dept-relvar,
                                                                                                                      :referenced-key :dept_no}} })

        dept-emp-relvar  (trel/transvar (trel/tr [:emp_no :dept_no :from_date] xrel-dept-emp) #{{:key #{:emp_no, :dept_no, :from_date}}
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
        xrel-emp (take employee-count (:xrel-emp raw-data))
        emp-relvar (trel/transvar (trel/tr [:emp_no :birth_date :first_name :last_name :gender :hire_date] (take employee-count xrel-emp)) {:key :emp_no})
        dept-relvar (trel/transvar (trel/tr [:dept_no :dept_name] (:xrel-department raw-data)) {:key :dept_no})
        xrel-sal (empno_filter (:emp_no (last xrel-emp)) (:xrel-sal raw-data))
        xrel-titles (empno_filter (:emp_no (last xrel-emp)) (:xrel-titles raw-data))
        xrel-dept-man (empno_filter (:emp_no (last xrel-emp)) (:xrel-dept_manager raw-data))
        xrel-dept-emp (empno_filter (:emp_no (last xrel-emp)) (:xrel-dept_emp raw-data))]

    (println "\nSum of tupel in database: " (+ employee-count 9 (count xrel-sal) (count xrel-titles) (count xrel-dept-man) (count xrel-dept-emp)))
    (println "\ncreating employees " employee-count )
    (criterium.core/quick-bench (trel/transvar (trel/tr [:emp_no :birth_date :first_name :last_name :gender :hire_date] (take employee-count xrel-emp)) {:key :emp_no}))

    (println "\ncreating department "  9 )
    (criterium.core/quick-bench  (trel/transvar (trel/tr [:dept_no :dept_name] (:xrel-department raw-data)) {:key :dept_no}) )

    (println "\ncreating salaris with " (count xrel-sal)             " foreign-keys:"  )
    (criterium.core/quick-bench (trel/transvar (trel/tr [:emp_no :salary :from_date :to_date] xrel-sal) #{{:key #{:emp_no, :from_date}}
                                                                                                       {:foreign-key {:key :emp_no,
                                                                                                                      :referenced-relvar emp-relvar,
                                                                                                                      :referenced-key :emp_no}}}) )

    (println "\ncreating title with " (count xrel-titles)            " foreign-keys:"  )
    (criterium.core/quick-bench (trel/transvar (trel/tr [:emp_no :title :from_date] xrel-titles) #{{:key #{:emp_no, :title, :from_date}}
                                                                                                       {:foreign-key {:key :emp_no,
                                                                                                                      :referenced-relvar emp-relvar,
                                                                                                                      :referenced-key :emp_no}}}) )

    (println "\ncreating dept-mananger with " (count xrel-dept-man)  " foreign-keys:"  )
    (criterium.core/quick-bench (trel/transvar (trel/tr [:dept_no :emp_no :from_date] xrel-dept-man) #{{:key #{:emp_no, :dept_no}}
                                                                                                       {:foreign-key {:key :emp_no,
                                                                                                                      :referenced-relvar emp-relvar,
                                                                                                                      :referenced-key :emp_no}}
                                                                                                       {:foreign-key {:key :dept_no,
                                                                                                                      :referenced-relvar dept-relvar,
                                                                                                                      :referenced-key :dept_no}} }) )

    (println "\ncreating dept_employees with " (count xrel-dept-emp)      " foreign-keys:"  )
    (criterium.core/quick-bench (trel/transvar (trel/tr [:emp_no :dept_no :from_date] xrel-dept-emp) #{{:key #{:emp_no, :dept_no, :from_date}}
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

       first-tupel (first (seq employee))
       middle-tupel (nth (seq employee) (long (/  (count employee) 2)))
       last-tupel (last (seq employee))

       middle-tupel-sal (nth (seq salaries) (long (/  (count salaries) 2)))]

    (println "\npointsearch-key-bm-1: ")
    (criterium.core/quick-bench  (trel/restriction employee (trel/tr-fn [t] (= (:emp_no t) (:emp_no first-tupel))) ))

    (println "\npointsearch-key-bm-2: ")
    (criterium.core/quick-bench (trel/restriction employee (trel/tr-fn [t](= (:emp_no t) (:emp_no middle-tupel))) ))

    (println "\npointsearch-key-bm-3: ")
    (criterium.core/quick-bench (trel/restriction employee (trel/tr-fn [t](= (:emp_no t) (:emp_no last-tupel))) ))

    (println "\npointsearch-key-bm-3: ")
    (criterium.core/quick-bench (trel/restriction employee (trel/tr-fn [t](= (:emp_no t) 1499999)) ))


    (println "\npointsearch-no-key-bm-1: ")
    (criterium.core/quick-bench (trel/restriction employee (trel/tr-fn [t](and (= (:birth_date t) (:birth_date first-tupel)) (= (:last_name t) (:last_name first-tupel)) (= (:first_name  t) (:first_name first-tupel))))))

    (println "\npointsearch-no-key-bm-2: " )
    (criterium.core/quick-bench (trel/restriction employee (trel/tr-fn [t](and (= (:gender t) (:gender middle-tupel)) (= (:last_name t) (:last_name middle-tupel))(= (:first_name  t) (:first_name  middle-tupel))))))

    (println "\npointsearch-no-key-bm-3: " )
    (criterium.core/quick-bench (trel/restriction employee (trel/tr-fn [t](and (= (:birth_date t) (:birth_date last-tupel)) (= (:last_name t) (:last_name last-tupel))(= (:first_name  t) (:first_name  last-tupel))))) )

    (println "\npointsearch-no-key-bm-3: " )
    (criterium.core/quick-bench (trel/restriction employee (trel/tr-fn [t] (and (= (:birth_date t) "XXXXXXX") (= (:last_name t) "YYYYYYYY")(= (:first_name  t) "ZZZZZZ"))) ))


    (println "\nareasearch-bm-1: " )
    (criterium.core/quick-bench  (trel/restriction employee (trel/tr-fn [t](> (:emp_no t) (:emp_no first-tupel))) ) )

    (println "\nareasearch-bm-2: " )
    (criterium.core/quick-bench  (trel/restriction employee (trel/tr-fn [t](< (:emp_no t) (:emp_no last-tupel)))))

    (println "\nareasearch-bm-3: " )
    (criterium.core/quick-bench  (trel/restriction employee (trel/tr-fn [t](= (:gender t) "F"))) )

    (println "\nareasearch-bm-4: " )
    (criterium.core/quick-bench  (trel/restriction employee (trel/tr-fn [t](not (= (:gender t) "F")))) )

    (println "\nareasearch-bm-5: " )
    (criterium.core/quick-bench  (trel/restriction salaries (trel/tr-fn [t](>= (:salary t) (:salary middle-tupel-sal)))) )
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
    (criterium.core/quick-bench (trel/join  employee salaries) )

    (println "\njoin-bm-2: " )
    (criterium.core/quick-bench (trel/join  salaries employee) )

    (println "\njoin-bm-3: " )
    (criterium.core/quick-bench (trel/join  titles employee) )

    (println "\njoin-bm-4: " )
    (criterium.core/quick-bench (trel/join  department_manager department) )

    (println "\njoin-bm-5: " )
    (criterium.core/quick-bench (trel/join (trel/join department_manager employee) department) )

    (println "\njoin-bm-6: " )
    (criterium.core/quick-bench (->> employee
                                     (trel/join department_employee)
                                     (trel/join titles)
                                     (trel/join department)
                                     (trel/join salaries)
                                     (trel/join department_manager)))
    ))


(defn employee-manipulation
  [database]
  (let [first-employee (first @(:employee database))
        insert-bm-1 (my-quickbenchmark  (trel/insert! (:employee database) {:emp_no (uuid), :birth_date "1958-02-19", :first_name "Saniya", :last_name "Kalloufi", :gender "M", :hire_date "1994-09-15"}) 6)
        insert-bm-2 (my-quickbenchmark  (trel/insert! (:titles database) {:emp_no (:emp_no first-employee), :title (uuid), :from_date "YYYYYYY"}) 6)
        insert-bm-3 (my-quickbenchmark  (trel/insert! (:salaries database) {:emp_no (:emp_no first-employee), :salary 10000000, :from_date (uuid), :to_date "1998-02-08"}) 6)
        insert-bm-4 (my-quickbenchmark  (trel/insert! (:department_employee database) {:emp_no (:emp_no first-employee), :dept_no "d008" :from_date (uuid)}) 6)


        delete-bm-1 (my-quickbenchmark (trel/delete! (:salaries database) (trel/tr-fn [t](> (:salary t) 53383))) 1)
        delete-bm-2 (my-quickbenchmark (trel/delete! (:titles database) (trel/tr-fn [t](> (:emp_no t) (:emp_no first-employee)))) 1)
        delete-bm-3 (my-quickbenchmark (trel/delete! (:titles database) (trel/tr-fn [t](= (:title t) "Senior Staff"))) 1)
        delete-bm-4 (my-quickbenchmark (trel/delete! (:department_employee database) (trel/tr-fn [t] true)) 1)


        update-bm-1 (my-quickbenchmark (trel/update! (:employee database) (trel/tr-fn [t] (= (:gender t) "F")) :gender "M") 1)
        update-bm-2 (my-quickbenchmark (trel/update! (:salaries database) (trel/tr-fn [t](= (:emp_no t) (:emp_no first-employee))) :salary 1) 1)
        update-bm-3 (my-quickbenchmark (trel/update! (:department database) (trel/tr-fn [t](= (:dept_no t) "d008")) :dept_name "" ) 1)
        update-bm-4 (my-quickbenchmark (trel/update! (:employee database) (trel/tr-fn [t](= (:emp_no t) (:emp_no first-employee))) :first_name "XXXXXXXX") 1)


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
  (println "\ntr - employee")
  (test-create-employee-database employee-count)
  (let [database (create-employee-database employee-count)]
    (employee-operation-tests database)
    (employee-join-tests database)
    (employee-manipulation database)
    ))


(defn creation-test
  [employee-count]
  (println "\ntr - employee - creating")
  (test-create-employee-database employee-count))

(defn search-test
  [employee-count]
  (println "\ntr - employee - search")
  (employee-operation-tests (create-employee-database employee-count)))

(defn join-test
  [employee-count]
  (println "\ntr - employee - join")
  (employee-join-tests (create-employee-database employee-count)))

(defn manipilation-test
  [employee-count]
  (println "\ntr - employee - manipulation")
  (employee-manipulation (create-employee-database employee-count)))


