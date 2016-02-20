(ns scripts.sqlconvert
  (:use [clojure.string]))


(def targetFolder "D:/Downloads/employees/clj/")

(def attrs { "employees" [:emp_no :birth_date :first_name :last_name :gender :hire_date]
             "salaries" [:emp_no :salary :from_date :to_date]
             "departments" [:dept_no :dept_name]
             "dept_manager" [:dept_no :emp_no :from_date]
             "dept_emp" [:emp_no :dept_no :from_date]
             "titles" [:emp_no :title :from_date]})




(defn convert
  [file]
(let [rdr (clojure.java.io/reader file)
      first_line (first (line-seq rdr))
      tablename (second (split (first (split first_line  #"VALUES ")) #"`"))
      records-of-insert (fn[insert] (mapv #(zipmap (get attrs tablename) %)
                          (read-string (str "["  (-> insert (split #"VALUES ") second (replace #"\(" "[") (replace #"\)" "]") (replace  #"'" "\"") (replace #";|," " ")) "]"))))
      records (vec (reduce #(concat %1  (records-of-insert %2)) (records-of-insert first_line)  (line-seq rdr)))
      ]
  (spit (str targetFolder tablename ".clj-dump") (str records) :append false)))


#(
(convert "D:/Downloads/employees/load_employees.dump")
(convert "D:/Downloads/employees/load_titles.dump")
(convert "D:/Downloads/employees/load_salaries.dump")
(convert "D:/Downloads/employees/load_departments.dump")
(convert "D:/Downloads/employees/load_dept_manager.dump")
(convert "D:/Downloads/employees/load_dept_emp.dump")
)
