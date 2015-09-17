(ns clj-par-sorts.core
  (:gen-class))


;; merge sort with futures
;; (time (def a (merge-sort        (take 8000 (repeatedly #(rand-int 42))))))
;; (time (def a (merge-sort-serial (take 8000 (repeatedly #(rand-int 42))))))
(declare merge-sort-parr)
(declare merge-sort-serial)
(declare merge-sort)

;; oops this should be tail recursive
(defn lst-merge
  [lst1 lst2]
  (cond
    (nil? lst1) lst2
    (nil? lst2) lst1
    :else (let
            [[f1 & rest1] lst1
             [f2 & rest2] lst2]
            (if (<= f1 f2)
              (cons f1 (lst-merge rest1 lst2))
              (cons f2 (lst-merge lst1 rest2))))))

(defn merge-sort-serial
  [lst]
  (if (<= (count lst) 1)
    lst
    (let
      [sides (split-at (/ (count lst) 2) lst)
       left  (merge-sort-serial (get sides 0))
       right (merge-sort-serial (get sides 1))]
      (lst-merge left right))))

(defn merge-sort
  [lst]
  (if (<= (count lst) 2000)
    (merge-sort-serial lst)
    (merge-sort-parr   lst)))

(defn merge-sort-parr
  [lst]
  (do
    (println "parallel sort called")
    (if (<= (count lst) 1)
      lst
      (let
        [sides (split-at (/ (count lst) 2) lst)
         left  (future (merge-sort (get sides 0)))
         right (future (merge-sort (get sides 1)))]
        (lst-merge @left @right)))))


(defn -main
  "I don't do a whole lot ... yet."
  [& args]
  (println "Hello, World!"))
