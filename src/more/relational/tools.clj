(ns more.relational.tools
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

(defn load-raw-data-employees
  []
  (read-string (slurp  "resources/employees.clj-dump" )))


(defn load-raw-data-salaries
  []
  (read-string (slurp  "resources/salaries.clj-dump" )))

(defn get_emps_by_manager
  [manxrel empxrel empcount]
  (let [empno_in_man (into #{} (map #(:emp_no %) manxrel))
        manager (into #{} (filter #(contains? empno_in_man (:emp_no %)) empxrel))
        not_manger (clojure.set/difference (set empxrel) manager)
        ]
    (apply conj manager (take (- empcount (count manager)) not_manger))))



(defn empno_filter
  [emp_nos xrel]
  (filterv #(contains? emp_nos (:emp_no %)) xrel))

(defn randomID [] (+ 10000000 (rand-int 10000000)))


(defn new-uuid []
      (str (java.util.UUID/randomUUID)))


(defn generate-dump
  [n]
  (mapv (fn[x] (new-uuid)) (range n)))

(defmacro my-time
  "Evaluates expr and prints the time it took.  Returns the value of expr."
  [expr]
  (criterium.core/force-gc)
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


(defn constraints
  [database]
  (let [emp-relvar (:employee database)
        dept-relvar (:department database)]
    {:employee #{{:key :emp_no}},
     :department  #{{:key :dept_no}}
     :salaries #{{:key #{:emp_no, :from_date}}
                 {:foreign-key {:key :emp_no,
                                :referenced-relvar emp-relvar,
                                :referenced-key :emp_no}}}
     :titles #{{:key #{:emp_no, :title, :from_date}}
               {:foreign-key {:key :emp_no,
                              :referenced-relvar emp-relvar,
                              :referenced-key :emp_no}}}
     :department_manager #{{:key #{:emp_no, :dept_no}}
                           {:foreign-key {:key :emp_no,
                                          :referenced-relvar emp-relvar,
                                          :referenced-key :emp_no}}
                           {:foreign-key {:key :dept_no,
                                          :referenced-relvar dept-relvar,
                                          :referenced-key :dept_no}} }
     :department_employee #{{:key #{:emp_no, :dept_no}}
                            {:foreign-key {:key :emp_no,
                                           :referenced-relvar emp-relvar,
                                           :referenced-key :emp_no}}
                            {:foreign-key {:key :dept_no,
                                           :referenced-relvar dept-relvar,
                                           :referenced-key :dept_no}} }}))


(defn create-employee-dummies
  []
  {:emp_no (randomID) :birth_date (new-uuid) :first_name (new-uuid) :last_name (new-uuid) :gender "F" :hire_date (new-uuid)})

