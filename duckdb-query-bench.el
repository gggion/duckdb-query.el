;;; duckdb-query-bench.el --- Performance benchmarks -*- lexical-binding: t; -*-

;;; Commentary:

;;; Code:
(require 'duckdb-query)

;;; Customization

(defcustom duckdb-query-bench-test-file nil
  "Path to parquet/database file for benchmarking.
If nil, benchmarks use generated test data.

Used by `duckdb-query-bench-runner' and `duckdb-query-bench-query'."
  :type '(choice (const :tag "Use generated data" nil)
                 (file :tag "Parquet/database file"))
  :group 'duckdb-query)

(defcustom duckdb-query-bench-iterations-execution 3
  "Iterations for execution benchmarks.

Used by `duckdb-query-bench-execution'."
  :type 'integer
  :group 'duckdb-query)

(defcustom duckdb-query-bench-iterations-parsing 5
  "Iterations for parsing benchmarks.

Used by `duckdb-query-bench-parsing'."
  :type 'integer
  :group 'duckdb-query)

(defcustom duckdb-query-bench-iterations-transform 5
  "Iterations for transformation benchmarks.

Used by `duckdb-query-bench-numbers'."
  :type 'integer
  :group 'duckdb-query)

(defcustom duckdb-query-bench-iterations-e2e 3
  "Iterations for end-to-end benchmarks.

Used by `duckdb-query-bench-end-to-end'."
  :type 'integer
  :group 'duckdb-query)

;;; Benchmark Infrastructure

(defun duckdb-query-bench--measure (iterations fn)
  "Run FN ITERATIONS times, return timing statistics.

Returns plist with :mean, :min, :max, :iterations.

Called by all benchmark functions."
  (let ((times nil))
    (dotimes (_ iterations)
      (garbage-collect)
      (let ((start (float-time)))
        (funcall fn)
        (push (- (float-time) start) times)))
    (list :mean (/ (apply #'+ times) (length times))
          :min (apply #'min times)
          :max (apply #'max times)
          :iterations iterations)))

(defun duckdb-query-bench--format-time (seconds)
  "Format SECONDS as human-readable time with appropriate precision.

Shows 2 significant digits after leading zeros.
Appends unit suffix (s, ms, µs) based on magnitude."
  (cond
   ((>= seconds 1.0)
    (format "%.2fs" seconds))
   ((>= seconds 0.001)
    (format "%.2fms" (* seconds 1000)))
   (t
    (format "%.2fµs" (* seconds 1000000)))))

(defun duckdb-query-bench--format-result (label stats)
  "Format STATS for display with LABEL.

STATS is plist from `duckdb-query-bench--measure'.
Shows mean time with unit, range, and iteration count."
  (let ((mean (plist-get stats :mean))
        (min (plist-get stats :min))
        (max (plist-get stats :max))
        (n (plist-get stats :iterations)))
    (format "%s: %s mean (%s-%s range, n=%d)"
            label
            (duckdb-query-bench--format-time mean)
            (duckdb-query-bench--format-time min)
            (duckdb-query-bench--format-time max)
            n)))

;;; Core Benchmarks

(defun duckdb-query-bench-execution (query)
  "Benchmark raw execution of QUERY.

Measures `duckdb-query-execute-raw' without parsing.
Uses `duckdb-query-bench-iterations-execution' iterations."
  (duckdb-query-bench--measure duckdb-query-bench-iterations-execution
    (lambda () (duckdb-query-execute-raw query))))

(defun duckdb-query-bench-parsing (output)
  "Benchmark parsing of OUTPUT string.

Measures `duckdb-query-parse-line-output' on pre-fetched data.
Uses `duckdb-query-bench-iterations-parsing' iterations."
  (duckdb-query-bench--measure duckdb-query-bench-iterations-parsing
    (lambda () (duckdb-query-parse-line-output output))))

(defun duckdb-query-bench-numbers (rows)
  "Benchmark number conversion on ROWS.

Measures `duckdb-query-parse-numbers' transducer.
Uses `duckdb-query-bench-iterations-transform' iterations."
  (duckdb-query-bench--measure duckdb-query-bench-iterations-transform
    (lambda ()
      (transducers-transduce
       (duckdb-query-parse-numbers)
       #'transducers-cons
       rows))))

(defun duckdb-query-bench-end-to-end (query &optional transform-numbers)
  "Benchmark complete execution of QUERY.

Measures full `duckdb-query' pipeline.
Uses `duckdb-query-bench-iterations-e2e' iterations."
  (duckdb-query-bench--measure duckdb-query-bench-iterations-e2e
    (lambda () (duckdb-query query :transform-numbers transform-numbers))))

;;; Test Data Generation

(defun duckdb-query-bench--generate-queries (test-file)
  "Generate benchmark queries using TEST-FILE or generated data.

Returns alist of (SIZE . QUERY) pairs.

Used by `duckdb-query-bench-runner'."
  (if test-file
      `((1000 . ,(format "SELECT * FROM '%s' LIMIT 1000" test-file))
        (10000 . ,(format "SELECT * FROM '%s' LIMIT 10000" test-file))
        (50000 . ,(format "SELECT * FROM '%s' LIMIT 50000" test-file)))
    '((1000 . "SELECT i as id, i*2 as doubled, 'item_'||i as name FROM range(1000) t(i)")
      (10000 . "SELECT i as id, i*2 as doubled, 'item_'||i as name FROM range(10000) t(i)")
      (50000 . "SELECT i as id, i*2 as doubled, 'item_'||i as name FROM range(50000) t(i)"))))

;;; Ad-Hoc Query Benchmarking

(defun duckdb-query-bench-query (query &optional label)
  "Benchmark QUERY through all pipeline stages.

QUERY is SQL string.
LABEL is optional description.

Returns alist of (STAGE . STATS) pairs."
  (interactive "sSQL Query: \nsLabel (optional): ")
  (let* ((label (or label "Custom query"))
         (results nil))

    (message "=== Benchmarking: %s ===" label)

    (let ((exec-stats (duckdb-query-bench-execution query)))
      (message "  Execution: %s" (duckdb-query-bench--format-result "exec" exec-stats))
      (push (cons 'execution exec-stats) results))

    (let* ((output (duckdb-query-execute-raw query))
           (parse-stats (duckdb-query-bench-parsing output)))
      (message "  Parsing:   %s" (duckdb-query-bench--format-result "parse" parse-stats))
      (push (cons 'parsing parse-stats) results))

    (let* ((rows (duckdb-query-parse-line-output (duckdb-query-execute-raw query)))
           (num-stats (duckdb-query-bench-numbers rows)))
      (message "  Numbers:   %s" (duckdb-query-bench--format-result "nums" num-stats))
      (push (cons 'numbers num-stats) results))

    (let ((e2e-stats (duckdb-query-bench-end-to-end query t)))
      (message "  End-to-end: %s" (duckdb-query-bench--format-result "e2e" e2e-stats))
      (push (cons 'end-to-end e2e-stats) results))

    (message "")
    (nreverse results)))

;;; Benchmark Runner

(defun duckdb-query-bench-runner (&optional test-file)
  "Run complete benchmark harness, return results alist.

TEST-FILE overrides `duckdb-query-bench-test-file'.

For ad-hoc queries, use `duckdb-query-bench-query'."
  (let* ((test-file (or test-file duckdb-query-bench-test-file))
         (queries (duckdb-query-bench--generate-queries test-file))
         (results nil))

    (message "=== DUCKDB-QUERY BENCHMARK HARNESS ===")
    (message "Test file: %s\n" (or test-file "generated data"))

    (dolist (test queries)
      (let* ((size (car test))
             (query (cdr test))
             (label (format "%dk rows" (/ size 1000))))

        (message "Testing %s..." label)

        (let ((exec-stats (duckdb-query-bench-execution query)))
          (message "  Execution: %s" (duckdb-query-bench--format-result "exec" exec-stats))
          (push (cons (format "%s-execution" label) exec-stats) results))

        (let* ((output (duckdb-query-execute-raw query))
               (parse-stats (duckdb-query-bench-parsing output)))
          (message "  Parsing:   %s" (duckdb-query-bench--format-result "parse" parse-stats))
          (push (cons (format "%s-parsing" label) parse-stats) results))

        (let* ((rows (duckdb-query-parse-line-output (duckdb-query-execute-raw query)))
               (num-stats (duckdb-query-bench-numbers rows)))
          (message "  Numbers:   %s" (duckdb-query-bench--format-result "nums" num-stats))
          (push (cons (format "%s-numbers" label) num-stats) results))

        (let ((e2e-stats (duckdb-query-bench-end-to-end query t)))
          (message "  End-to-end: %s" (duckdb-query-bench--format-result "e2e" e2e-stats))
          (push (cons (format "%s-end-to-end" label) e2e-stats) results))

        (message "")))

    (message "=== BENCHMARK COMPLETE ===")
    (nreverse results)))

;;; Comparison Utilities

(defun duckdb-query-bench-compare (baseline-results new-results)
  "Compare BASELINE-RESULTS to NEW-RESULTS.

Both arguments are alists from `duckdb-query-bench-runner'."
  (message "\n=== PERFORMANCE COMPARISON ===\n")
  (dolist (baseline baseline-results)
    (let* ((label (car baseline))
           (baseline-mean (plist-get (cdr baseline) :mean))
           (new (assoc label new-results))
           (new-mean (when new (plist-get (cdr new) :mean))))
      (when new-mean
        (let ((speedup (/ baseline-mean new-mean)))
          (message "%s: %.2fx %s"
                   label
                   (abs speedup)
                   (cond
                    ((> speedup 1.05) "FASTER")
                    ((< speedup 0.95) "SLOWER")
                    (t "SAME"))))))))

(defun duckdb-query-bench-save-baseline (results file)
  "Save RESULTS to FILE for future comparisons.

Load with `duckdb-query-bench-load-baseline'."
  (with-temp-file file
    (prin1 results (current-buffer))))

(defun duckdb-query-bench-load-baseline (file)
  "Load baseline results from FILE.

Returns alist for `duckdb-query-bench-compare'."
  (with-temp-buffer
    (insert-file-contents file)
    (read (current-buffer))))

(defun duckdb-query-bench-tabulate (results)
  "Display RESULTS in tabular format.

RESULTS is alist from `duckdb-query-bench-runner' or `duckdb-query-bench-query'."
  (let ((buf (get-buffer-create "*DuckDB Benchmark Results*")))
    (with-current-buffer buf
      (erase-buffer)
      (insert (format "%-20s %12s %12s %12s %5s\n"
                      "Stage" "Mean" "Min" "Max" "N"))
      (insert (make-string 65 ?-) "\n")
      (dolist (result results)
        (let ((label (car result))
              (stats (cdr result)))
          (insert (format "%-20s %12s %12s %12s %5d\n"
                          label
                          (duckdb-query-bench--format-time (plist-get stats :mean))
                          (duckdb-query-bench--format-time (plist-get stats :min))
                          (duckdb-query-bench--format-time (plist-get stats :max))
                          (plist-get stats :iterations)))))
      (goto-char (point-min))
      (view-mode))
    (display-buffer buf)))

(provide 'duckdb-query-bench)
;;; duckdb-query-bench.el ends here
