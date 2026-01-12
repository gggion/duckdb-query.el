;;; duckdb-query-bench.el --- Benchmark suite for duckdb-query -*- lexical-binding: t; -*-

;; Author: Gino Cornejo
;; Maintainer: Gino Cornejo <gggion123@gmail.com>
;; Homepage: https://github.com/gggion/duckdb-query

;; This file is part of duckdb-query.

;; SPDX-License-Identifier: GPL-3.0-or-later

;; This file is free software: you can redistribute it and/or modify
;; it under the terms of the GNU General Public License as published
;; by the Free Software Foundation, either version 3 of the License,
;; or (at your option) any later version.
;;
;; This file is distributed in the hope that it will be useful,
;; but WITHOUT ANY WARRANTY; without even the implied warranty of
;; MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
;; GNU General Public License for more details.
;;
;; You should have received a copy of the GNU General Public License
;; along with this file.  If not, see <https://www.gnu.org/licenses/>.

;;; Commentary:

;; Benchmark tools for duckdb-query package performance analysis.
;;
;; Basic usage:
;;
;;     (duckdb-query-bench-runner "<FILE>")
;;     ;; Runs full benchmark suite over the specified FILE
;;
;;     (duckdb-query-bench-query "SELECT * FROM range(1000)")
;;     ;; Benchmarks specific query across all formats
;;
;; The package provides:
;; - `duckdb-query-bench-runner' - Full benchmark harness
;; - `duckdb-query-bench-query' - Ad-hoc query benchmarking
;; - `duckdb-query-bench-formats' - Format comparison
;; - `duckdb-query-bench-columnar-detailed' - Detailed columnar analysis
;;
;; Key commands:
;; - `duckdb-query-bench-runner' - Run complete benchmark suite
;; - `duckdb-query-bench-query' - Benchmark custom query

;;; Code:

(require 'duckdb-query)
(require 'cl-lib)
(require 'benchmark)

;;; Customization

(defgroup duckdb-query-bench nil
  "Benchmarking for duckdb-query package."
  :group 'duckdb-query
  :prefix "duckdb-query-bench-")

(defcustom duckdb-query-bench-test-file nil
  "Path to data file for benchmarking.

When nil, benchmarks use generated test data via DuckDB range function.
When non-nil, should be path to Parquet, CSV, or database file.

Used by `duckdb-query-bench-runner' and `duckdb-query-bench--generate-queries'."
  :type '(choice (const :tag "Use generated data" nil)
                 (file :tag "Data file path"))
  :group 'duckdb-query-bench)

(defcustom duckdb-query-bench-iterations 5
  "Number of iterations for each benchmark measurement.

Higher values increase accuracy but take longer to run.
Minimum recommended value is 3 for meaningful statistics.

Used by `duckdb-query-bench--measure'."
  :type 'integer
  :group 'duckdb-query-bench)

;;; Internal Variables

(defvar duckdb-query-bench--gc-cons-threshold-saved nil
  "Saved value of `gc-cons-threshold' before benchmarking.

Restored by `duckdb-query-bench--measure' after each iteration.
Set by `duckdb-query-bench--measure' to minimize GC interference.")

;;; Benchmark Infrastructure

(defun duckdb-query-bench--measure (iterations fn)
  "Run FN ITERATIONS times and return timing statistics.

ITERATIONS is positive integer specifying repetition count.
FN is function to benchmark, called with no arguments.

Returns plist with keys:
  :mean - Average execution time in seconds
  :min - Minimum execution time in seconds
  :max - Maximum execution time in seconds
  :iterations - Number of iterations performed

Forces garbage collection before each iteration to minimize GC
interference with timing measurements.

Uses `benchmark-elapse' from benchmark.el for timing.

Called by `duckdb-query-bench-formats', `duckdb-query-bench-columnar',
and `duckdb-query-bench-end-to-end'."
  (let ((times nil)
        (gc-cons-threshold most-positive-fixnum))
    (cl-loop repeat iterations
             do (progn
                  (garbage-collect)
                  (push (benchmark-elapse (funcall fn)) times)))
    (list :mean (/ (apply #'+ times) (length times))
          :min (apply #'min times)
          :max (apply #'max times)
          :iterations iterations)))

(defun duckdb-query-bench--format-time (seconds)
  "Format SECONDS as human-readable time string.

SECONDS is non-negative float representing duration.

Returns string with 2 decimal places and appropriate unit suffix:
  - \"Ns\" for seconds (>= 1.0)
  - \"Nms\" for milliseconds (< 1.0)

Milliseconds is minimum precision; values below 0.001s round to
nearest millisecond to avoid false precision from measurement noise.

Called by =duckdb-query-bench--format-result' and
`duckdb-query-bench-columnar-detailed-table'."
  (cond
   ((>= seconds 1.0)
    (format "%.2fs" seconds))
   (t
    (format "%.2fms" (* seconds 1000)))))

(defun duckdb-query-bench--format-result (label stats)
  "Format STATS for display with LABEL.

LABEL is string describing the benchmark.
STATS is plist from `duckdb-query-bench--measure' with keys
:mean, :min, :max, :iterations.

Returns formatted string showing mean time with range and iteration count.

Called by `duckdb-query-bench-formats' and `duckdb-query-bench-columnar'."
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

QUERY is SQL string to execute.

Measures execution time for each format:
  :alist, :plist, :hash, :vector, :columnar, :org-table

Returns alist of (FORMAT . STATS) pairs where STATS is plist
from `duckdb-query-bench--measure'.

Prints progress messages and formatted results to echo area.

Called by `duckdb-query-bench-runner' and `duckdb-query-bench-query'."
  (let ((results nil)
        (formats '(:alist :plist :hash :vector :columnar :org-table)))
    (cl-loop for fmt in formats
             do (progn
                  (message "Benchmarking format %s..." fmt)
                  (let ((stats (duckdb-query-bench--measure
                                duckdb-query-bench-iterations
                                (lambda () (duckdb-query query :format fmt)))))
                    (message "  %s" (duckdb-query-bench--format-result
                                     (symbol-name fmt) stats))
                    (push (cons fmt stats) results))))
    (nreverse results)))

(defun duckdb-query-bench-columnar (query)
  "Benchmark columnar conversion overhead for QUERY.

QUERY is SQL string to execute.

Compares :alist format (baseline) to :columnar format to measure
conversion overhead.

Returns alist with entries:
  (\"alist\" . STATS) - Baseline timing
  (\"columnar\" . STATS) - Columnar timing
  (\"columnar-overhead\" . OVERHEAD-SECONDS) - Conversion cost

Prints comparison results to echo area.

Called by `duckdb-query-bench-runner'.
Also see `duckdb-query-bench-columnar-detailed' for phase breakdown."
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

      (list (cons "alist" alist-stats)
            (cons "columnar" columnar-stats)
            (cons "columnar-overhead" overhead)))))

(defun duckdb-query-bench-columnar-detailed (query)
  "Benchmark columnar conversion with detailed phase timing.

QUERY is SQL string to execute.

Measures four phases separately:
  1. DuckDB query execution (raw JSON output)
  2. JSON parsing to alist
  3. Alist to columnar conversion
  4. Total end-to-end time

Returns alist with entries for \"alist\" and \"columnar\" formats.
Each entry contains alist with timing breakdown:
  (\"query-exec\" . SECONDS)
  (\"parsing\" . SECONDS)
  (\"conversion\" . SECONDS)
  (\"total\" . SECONDS)

Prints detailed phase timings to echo area.

Called by `duckdb-query-bench-query'.
For simple overhead measurement, use `duckdb-query-bench-columnar'."
  (message "=== Detailed Columnar Benchmark ===\n")

  (let ((results nil))

    ;; Benchmark alist format
    (message "Benchmarking :alist format...")
    (let ((exec-stats nil)
          (parse-stats nil)
          (total-stats nil)
          (json-output nil))

      ;; Phase 1: Query execution
      (setq exec-stats
            (duckdb-query-bench--measure
             duckdb-query-bench-iterations
             (lambda ()
               (setq json-output (duckdb-query-execute-raw query)))))
      (message "  Query execution: %s"
               (duckdb-query-bench--format-result "exec" exec-stats))

      ;; Phase 2: JSON parsing
      (setq parse-stats
            (duckdb-query-bench--measure
             duckdb-query-bench-iterations
             (lambda ()
               (json-parse-string json-output
                                  :object-type 'alist
                                  :array-type 'list
                                  :null-object duckdb-query-null-value
                                  :false-object duckdb-query-false-value))))
      (message "  JSON parsing: %s"
               (duckdb-query-bench--format-result "parse" parse-stats))

      ;; Phase 3: Total end-to-end
      (setq total-stats
            (duckdb-query-bench--measure
             duckdb-query-bench-iterations
             (lambda ()
               (duckdb-query query :format :alist))))
      (message "  Total: %s\n"
               (duckdb-query-bench--format-result "total" total-stats))

      (push (cons "alist"
                  (list (cons "query-exec" (plist-get exec-stats :mean))
                        (cons "parsing" (plist-get parse-stats :mean))
                        (cons "conversion" 0)
                        (cons "total" (plist-get total-stats :mean))))
            results))

    ;; Benchmark columnar format
    (message "Benchmarking :columnar format...")
    (let ((exec-stats nil)
          (parse-stats nil)
          (conversion-stats nil)
          (total-stats nil)
          (json-output nil)
          (parsed-rows nil))

      ;; Phase 1: Query execution
      (setq exec-stats
            (duckdb-query-bench--measure
             duckdb-query-bench-iterations
             (lambda ()
               (setq json-output (duckdb-query-execute-raw query)))))
      (message "  Query execution: %s"
               (duckdb-query-bench--format-result "exec" exec-stats))

      ;; Phase 2: JSON parsing
      (setq parse-stats
            (duckdb-query-bench--measure
             duckdb-query-bench-iterations
             (lambda ()
               (setq parsed-rows
                     (json-parse-string json-output
                                        :object-type 'alist
                                        :array-type 'list
                                        :null-object duckdb-query-null-value
                                        :false-object duckdb-query-false-value)))))
      (message "  JSON parsing: %s"
               (duckdb-query-bench--format-result "parse" parse-stats))

      ;; Phase 3: Columnar conversion
      (setq conversion-stats
            (duckdb-query-bench--measure
             duckdb-query-bench-iterations
             (lambda ()
               (duckdb-query--to-columnar parsed-rows))))
      (message "  Columnar conversion: %s"
               (duckdb-query-bench--format-result "conversion" conversion-stats))

      ;; Phase 4: Total end-to-end
      (setq total-stats
            (duckdb-query-bench--measure
             duckdb-query-bench-iterations
             (lambda ()
               (duckdb-query query :format :columnar))))
      (message "  Total: %s\n"
               (duckdb-query-bench--format-result "total" total-stats))

      (push (cons "columnar"
                  (list (cons "query-exec" (plist-get exec-stats :mean))
                        (cons "parsing" (plist-get parse-stats :mean))
                        (cons "conversion" (plist-get conversion-stats :mean))
                        (cons "total" (plist-get total-stats :mean))))
            results))

    (message "=== Benchmark Complete ===")
    (nreverse results)))

(defun duckdb-query-bench-columnar-detailed-table (results)
  "Format RESULTS from detailed columnar benchmark as org table.

RESULTS is alist from `duckdb-query-bench-columnar-detailed' with
entries for \"alist\" and \"columnar\" formats.

Returns formatted org-table string with columns:
  | Benchmark | query-exec | parsing | conversion | total |

Includes overhead row showing columnar conversion cost.

Uses `org-table-align' for proper column alignment.

Called interactively after `duckdb-query-bench-columnar-detailed'."
  (require 'org-table)
  (substring-no-properties
   (with-temp-buffer
     (org-mode)

     ;; Insert header
     (insert "| Benchmark | query-exec | parsing | conversion | total |\n")
     (insert "|-\n")

     ;; Insert data rows
     (cl-loop for result in results
              for format-name = (car result)
              for timings = (cdr result)
              for query-exec = (cdr (assoc "query-exec" timings))
              for parsing = (cdr (assoc "parsing" timings))
              for conversion = (cdr (assoc "conversion" timings))
              for total = (cdr (assoc "total" timings))
              do (insert (format "| %s | %s | %s | %s | %s |\n"
                                 format-name
                                 (duckdb-query-bench--format-time query-exec)
                                 (duckdb-query-bench--format-time parsing)
                                 (if (zerop conversion)
                                     "—"
                                   (duckdb-query-bench--format-time conversion))
                                 (duckdb-query-bench--format-time total))))

     ;; Add overhead row
     (let* ((alist-timings (cdr (assoc "alist" results)))
            (columnar-timings (cdr (assoc "columnar" results)))
            (conversion-overhead (cdr (assoc "conversion" columnar-timings)))
            (total-overhead (- (cdr (assoc "total" columnar-timings))
                               (cdr (assoc "total" alist-timings)))))
       (insert "|-\n")
       (insert (format "| overhead | — | — | %s | %s |\n"
                       (duckdb-query-bench--format-time conversion-overhead)
                       (duckdb-query-bench--format-time total-overhead))))

     ;; Align table
     (goto-char (point-min))
     (org-table-align)
     (buffer-string))))

(defun duckdb-query-bench-end-to-end (query &optional format)
  "Benchmark complete execution of QUERY in FORMAT.

QUERY is SQL string to execute.
FORMAT defaults to :alist when nil.

Measures full `duckdb-query' pipeline including execution and parsing.

Returns plist from `duckdb-query-bench--measure' with timing statistics.

Called by `duckdb-query-bench-runner' for simple end-to-end timing."
  (duckdb-query-bench--measure
   duckdb-query-bench-iterations
   (lambda () (duckdb-query query :format (or format :alist)))))

;;; Test Data Generation

(defun duckdb-query-bench--generate-queries (test-file row-sizes offset)
  "Generate benchmark queries using TEST-FILE or generated data.

TEST-FILE is path to data file or nil for generated data.
ROW-SIZES is list of integers specifying row counts to benchmark.
OFFSET is integer specifying starting row (0-based).

When TEST-FILE is non-nil, generates queries reading from file
with LIMIT and OFFSET clauses for each size in ROW-SIZES.

When TEST-FILE is nil, generates queries using DuckDB range function
to create test data with columns: id, doubled, name.

Returns alist of (SIZE . QUERY) pairs where SIZE is row count.

Called by `duckdb-query-bench-runner'."
  (if test-file
      (cl-loop for size in row-sizes
               collect (cons size
                             (format "SELECT * FROM '%s' LIMIT %d OFFSET %d"
                                     test-file size offset)))
    (cl-loop for size in row-sizes
             collect (cons size
                           (format "SELECT i as id, i*2 as doubled, 'item_'||i as name FROM range(%d, %d) t(i)"
                                   offset (+ offset size))))))

;;; Ad-Hoc Query Benchmarking

(defun duckdb-query-bench-query (query &optional label)
  "Benchmark QUERY through all formats with optional LABEL.

QUERY is SQL string to execute.
LABEL is optional description string for display.

Runs `duckdb-query-bench-formats' and `duckdb-query-bench-columnar'
on QUERY and prints results to echo area.

Returns plist with keys:
  :metadata - Plist with :query and :label
  :results  - Alist of (FORMAT . STATS) pairs from format benchmarks

Called interactively for ad-hoc query benchmarking."
  (interactive "sSQL Query: \nsLabel (optional): ")
  (let* ((label (or label "Custom query"))
         (results nil))

    (message "=== Benchmarking: %s ===" label)

    (setq results (duckdb-query-bench-formats query))

    (message "\n=== Columnar Conversion Analysis ===")
    (duckdb-query-bench-columnar query)

    (message "")

    (list :metadata (list :query query
                          :label label
                          :iterations duckdb-query-bench-iterations)
          :results results)))

;;; Benchmark Runner

(cl-defun duckdb-query-bench-runner (&optional test-file
                                               &key
                                               (row-sizes '(1000 10000 50000 100000))
                                               (offset 0)
                                               (iterations duckdb-query-bench-iterations))
  "Run complete benchmark harness and return results.

TEST-FILE is optional path to data file (CSV, Parquet, database).
When nil, uses `duckdb-query-bench-test-file' or generates test data.

ROW-SIZES is list of integers specifying row counts to benchmark.
Defaults to (1000 10000 50000 100000).

OFFSET is integer specifying starting row for queries.
Defaults to 0.

ITERATIONS is number of times to run each benchmark.
Defaults to `duckdb-query-bench-iterations'.

Benchmarks queries at each size in ROW-SIZES.
For each size, measures all output formats and columnar overhead.

Returns plist with keys:
  :metadata - Plist with :test-file, :row-sizes, :offset, :iterations
  :results  - Alist of (LABEL . STATS-OR-VALUE) pairs where:
              - Format benchmarks: (\"Nk-FORMAT\" . STATS-PLIST)
              - Overhead measurements: (\"Nk-columnar-overhead\" . SECONDS)

Metadata enables reproducible comparisons via `duckdb-query-bench-compare'.

Prints progress and results to echo area.

For ad-hoc query benchmarking, use `duckdb-query-bench-query'.

Examples:

  ;; Default: 1k, 10k, 50k, 100k rows with default iterations
  (duckdb-query-bench-runner \"data.csv\")

  ;; Custom sizes: 100, 1k, 10k rows with 10 iterations
  (duckdb-query-bench-runner \"data.csv\"
                             :row-sizes \\='(100 1000 10000)
                             :iterations 10)

  ;; Start at row 1000, test 5k rows, 3 iterations
  (duckdb-query-bench-runner \"data.csv\"
                             :row-sizes \\='(5000)
                             :offset 1000
                             :iterations 3)"
  (interactive)
  (let* ((test-file (or test-file duckdb-query-bench-test-file))
         (queries (duckdb-query-bench--generate-queries test-file row-sizes offset))
         (duckdb-query-bench-iterations iterations)  ; Bind for nested functions
         (results nil))

    (cl-loop for test in queries
             for size = (car test)
             for query = (cdr test)
             for label = (cond
                          ((>= size 1000000)
                           (format "%dM rows" (/ size 1000000)))
                          ((>= size 1000)
                           (format "%dk rows" (/ size 1000)))
                          (t
                           (format "%d rows" size)))
             do (progn
                  (message "Testing %s..." label)

                  ;; Benchmark all formats
                  (let ((format-results (duckdb-query-bench-formats query)))
                    (cl-loop for result in format-results
                             do (push (cons (format "%s-%s" label (car result))
                                            (cdr result))
                                      results)))

                  ;; Analyze columnar overhead
                  (message "\nColumnar analysis for %s:" label)
                  (let ((columnar-analysis (duckdb-query-bench-columnar query)))
                    (push (cons (format "%s-columnar-overhead" label)
                                (cdr (assoc "columnar-overhead" columnar-analysis)))
                          results))

                  (message "")))

    (message "=== BENCHMARK COMPLETE ===")

    (list :metadata (list :test-file test-file
                          :row-sizes row-sizes
                          :offset offset
                          :iterations iterations)
          :results (nreverse results))))

;;; Comparison Utilities

(defun duckdb-query-bench-compare (baseline-data new-data)
  "Compare BASELINE-DATA to NEW-DATA and return comparison results.

BASELINE-DATA is plist from previous `duckdb-query-bench-runner'.
NEW-DATA is plist from current `duckdb-query-bench-runner'.

Both should contain :metadata and :results keys.

Returns alist of (LABEL . COMPARISON-PLIST) pairs where
COMPARISON-PLIST contains:
  :baseline - Baseline mean time in seconds
  :new      - New mean time in seconds
  :speedup  - Ratio of baseline/new (>1 means faster)
  :verdict  - Symbol: faster, slower, or same

Prints performance comparison to echo area.

Speedup > 1.05 shows \"FASTER\".
Speedup < 0.95 shows \"SLOWER\".
Otherwise shows \"SAME\".

Use `duckdb-query-bench-compare-table' to format as org table."
  (let ((baseline-results (plist-get baseline-data :results))
        (new-results (plist-get new-data :results))
        (comparisons nil))

    (message "\n=== PERFORMANCE COMPARISON ===\n")

    (cl-loop for baseline in baseline-results
             for label = (car baseline)
             for baseline-mean = (if (numberp (cdr baseline))
                                     (cdr baseline)
                                   (plist-get (cdr baseline) :mean))
             for new = (assoc label new-results)
             for new-mean = (when new
                              (if (numberp (cdr new))
                                  (cdr new)
                                (plist-get (cdr new) :mean)))
             when new-mean
             do (let* ((speedup (/ baseline-mean new-mean))
                       (verdict (cond
                                 ((> speedup 1.05) 'faster)
                                 ((< speedup 0.95) 'slower)
                                 (t 'same))))
                  (message "%s: %.2fx %s"
                           label
                           (abs speedup)
                           (upcase (symbol-name verdict)))
                  (push (cons label
                              (list :baseline baseline-mean
                                    :new new-mean
                                    :speedup speedup
                                    :verdict verdict))
                        comparisons)))
    (nreverse comparisons)))

(defun duckdb-query-bench-compare-table (comparisons)
  "Format COMPARISONS from `duckdb-query-bench-compare' as org table.

COMPARISONS is alist with entries containing comparison plists.

Returns formatted org-table string with columns:
  | Benchmark | Baseline | New | Speedup | Verdict |

Uses `org-table-align' for proper column alignment."
  (require 'org-table)
  (substring-no-properties
   (with-temp-buffer
     (org-mode)

     ;; Insert header
     (insert "| Benchmark | Baseline | New | Speedup | Verdict |\n")
     (insert "|-\n")

     ;; Insert data rows
     (cl-loop for comparison in comparisons
              for label = (car comparison)
              for data = (cdr comparison)
              for baseline = (plist-get data :baseline)
              for new = (plist-get data :new)
              for speedup = (plist-get data :speedup)
              for verdict = (plist-get data :verdict)
              do (insert (format "| %s | %s | %s | %.2fx | %s |\n"
                                 label
                                 (duckdb-query-bench--format-time baseline)
                                 (duckdb-query-bench--format-time new)
                                 (abs speedup)
                                 (upcase (symbol-name verdict)))))

     ;; Align table
     (goto-char (point-min))
     (org-table-align)
     (buffer-string))))

(defun duckdb-query-bench-save-baseline (benchmark-data file)
  "Save BENCHMARK-DATA to FILE for future comparisons.

BENCHMARK-DATA is plist from `duckdb-query-bench-runner' containing
:metadata and :results keys.

Saved data can be loaded with `duckdb-query-bench-load-baseline'
and compared with `duckdb-query-bench-compare'."
  (with-temp-file file
    (prin1 benchmark-data (current-buffer))))

(defun duckdb-query-bench-load-baseline (file)
  "Load baseline results from FILE.

FILE is path to results saved by `duckdb-query-bench-save-baseline'.

Returns alist suitable for `duckdb-query-bench-compare'."
  (with-temp-buffer
    (insert-file-contents file)
    (read (current-buffer))))

(defun duckdb-query-bench-tabulate (benchmark-data)
  "Format BENCHMARK-DATA as aligned org-mode table string.

BENCHMARK-DATA is plist from `duckdb-query-bench-runner' containing
:metadata and :results keys.

Returns string containing org-table with columns:
  | Benchmark | Mean | Min | Max | N |

For overhead entries (numeric values), displays only benchmark
name and time value.

Uses `org-table-align' for proper column alignment."
  (require 'org-table)
  (let ((results (plist-get benchmark-data :results)))
    (substring-no-properties
     (with-temp-buffer
       (org-mode)
       ;; Insert header
       (insert "| Benchmark | Mean | Min | Max | N |\n")
       (insert "|-\n")

       ;; Insert data rows
       (cl-loop for result in results
                for label = (car result)
                for stats = (cdr result)
                do (if (numberp stats)
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
                                     (plist-get stats :iterations)))))

       ;; Align table
       (goto-char (point-min))
       (org-table-align)
       (buffer-string)))))

(provide 'duckdb-query-bench)
;;; duckdb-query-bench.el ends here
