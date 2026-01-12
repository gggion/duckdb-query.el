;;; duckdb-query-bench.el --- Performance benchmarks -*- lexical-binding: t; -*-

;;; Commentary:

;; Benchmark suite for duckdb-query package.
;;
;; Primary benchmarks:
;; - `duckdb-query-bench-formats' - Compare output format performance
;; - `duckdb-query-bench-columnar' - Measure columnar conversion
;; - `duckdb-query-bench-query' - Ad-hoc query benchmarking
;; - `duckdb-query-bench-runner' - Full benchmark harness

;;; Code:

(require 'duckdb-query)

;;; Customization

(defcustom duckdb-query-bench-test-file nil
  "Path to parquet/CSV/database file for benchmarking.

If nil, benchmarks use generated test data.

Used by `duckdb-query-bench-runner' and `duckdb-query-bench-query'."
  :type '(choice (const :tag "Use generated data" nil)
                 (file :tag "Data file"))
  :group 'duckdb-query)

(defcustom duckdb-query-bench-iterations 5
  "Iterations for benchmark measurements.

Used by all benchmark functions."
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

(defun duckdb-query-bench-formats (query)
  "Benchmark QUERY across all output formats.

Measures `:alist', `:plist', `:hash', `:vector', and `:columnar'.

Returns alist of (FORMAT . STATS) pairs."
  (let ((results nil))
    (dolist (fmt '(:alist :plist :hash :vector :columnar))
      (message "Benchmarking format %s..." fmt)
      (let ((stats (duckdb-query-bench--measure
                    duckdb-query-bench-iterations
                    (lambda () (duckdb-query query :format fmt)))))
        (message "  %s" (duckdb-query-bench--format-result
                         (symbol-name fmt) stats))
        (push (cons fmt stats) results)))
    (nreverse results)))

(defun duckdb-query-bench-columnar (query)
  "Benchmark columnar conversion overhead for QUERY.

Compares `:alist' format (baseline) to `:columnar' format.

Returns alist with entries:
  (\"alist\" . STATS)
  (\"columnar\" . STATS)
  (\"columnar-overhead\" . OVERHEAD-SECONDS)"
  (message "Benchmarking columnar conversion...")

  (let ((alist-stats (duckdb-query-bench--measure
                      duckdb-query-bench-iterations
                      (lambda () (duckdb-query query :format :alist))))
        (columnar-stats (duckdb-query-bench--measure
                         duckdb-query-bench-iterations
                         (lambda () (duckdb-query query :format :columnar)))))

    (let ((overhead (- (plist-get columnar-stats :mean)
                       (plist-get alist-stats :mean))))
      (message "  Alist:    %s"
               (duckdb-query-bench--format-result "alist" alist-stats))
      (message "  Columnar: %s"
               (duckdb-query-bench--format-result "columnar" columnar-stats))
      (message "  Overhead: %s (%.1f%%)"
               (duckdb-query-bench--format-time overhead)
               (* 100 (/ overhead (plist-get alist-stats :mean))))

      ;; Return alist, not plist
      (list (cons "alist" alist-stats)
            (cons "columnar" columnar-stats)
            (cons "columnar-overhead" overhead)))))

(defun duckdb-query-bench-end-to-end (query &optional format)
  "Benchmark complete execution of QUERY.

FORMAT defaults to `:alist'.

Measures full `duckdb-query' pipeline including execution and parsing."
  (duckdb-query-bench--measure
   duckdb-query-bench-iterations
   (lambda () (duckdb-query query :format (or format :alist)))))

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
  "Benchmark QUERY through all formats.

QUERY is SQL string.
LABEL is optional description.

Returns alist of (FORMAT . STATS) pairs."
  (interactive "sSQL Query: \nsLabel (optional): ")
  (let* ((label (or label "Custom query"))
         (results nil))

    (message "=== Benchmarking: %s ===" label)

    (setq results (duckdb-query-bench-formats query))

    (message "\n=== Columnar Conversion Analysis ===")
    (duckdb-query-bench-columnar query)

    (message "")
    results))

;;; Benchmark Runner

(defun duckdb-query-bench-runner (&optional test-file)
  "Run complete benchmark harness, return results alist.

TEST-FILE is path to data file (CSV, Parquet, database).
If nil, uses `duckdb-query-bench-test-file' or generates test data.

Benchmarks queries at 1K, 10K, and 50K row sizes.

For ad-hoc query benchmarking, use `duckdb-query-bench-query'."
  (interactive)
  (let* ((test-file (or test-file duckdb-query-bench-test-file))
         (queries (duckdb-query-bench--generate-queries test-file))
         (results nil))

    (message "=== DUCKDB-QUERY BENCHMARK HARNESS ===")
    (message "Test file: %s" (or test-file "generated data"))
    (message "Iterations: %d\n" duckdb-query-bench-iterations)

    (dolist (test queries)
      (let* ((size (car test))
             (query (cdr test))
             (label (format "%dk rows" (/ size 1000))))

        (message "Testing %s..." label)

        ;; Benchmark all formats
        (let ((format-results (duckdb-query-bench-formats query)))
          (dolist (result format-results)
            (push (cons (format "%s-%s" label (car result))
                        (cdr result))
                  results)))

        ;; Analyze columnar overhead
        (message "\nColumnar analysis for %s:" label)
        (let ((columnar-analysis (duckdb-query-bench-columnar query)))
          ;; Extract overhead value from alist
          (push (cons (format "%s-columnar-overhead" label)
                      (cdr (assoc "columnar-overhead" columnar-analysis)))
                results))

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
           (baseline-mean (if (numberp (cdr baseline))
                              (cdr baseline)
                            (plist-get (cdr baseline) :mean)))
           (new (assoc label new-results))
           (new-mean (when new
                       (if (numberp (cdr new))
                           (cdr new)
                         (plist-get (cdr new) :mean)))))
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
  "Format RESULTS as aligned org-mode table string.

RESULTS is alist from `duckdb-query-bench-runner' or
`duckdb-query-bench-query'.

Returns string containing org-table with columns:
| Benchmark | Mean | Min | Max | N |

For overhead entries (numeric values), displays only benchmark name
and time value."
  (require 'org-table)
  (with-temp-buffer
    (org-mode)
    ;; Insert header
    (insert "| Benchmark | Mean | Min | Max | N |\n")
    (insert "|-\n")

    ;; Insert data rows
    (dolist (result results)
      (let ((label (car result))
            (stats (cdr result)))
        (if (numberp stats)
            ;; Overhead value - span across columns
            (insert (format "| %s | %s | | | |\n"
                            label
                            (duckdb-query-bench--format-time stats)))
          ;; Full stats
          (insert (format "| %s | %s | %s | %s | %d |\n"
                          label
                          (duckdb-query-bench--format-time (plist-get stats :mean))
                          (duckdb-query-bench--format-time (plist-get stats :min))
                          (duckdb-query-bench--format-time (plist-get stats :max))
                          (plist-get stats :iterations))))))

    ;; Align table
    (goto-char (point-min))
    (org-table-align)
    (buffer-string)))

(provide 'duckdb-query-bench)
;;; duckdb-query-bench.el ends here
