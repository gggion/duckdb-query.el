;;; duckdb-query.el --- DuckDB query results as native Elisp data structures -*- lexical-binding: t; -*-
;;
;; Author: Gino Cornejo
;; Maintainer: Gino Cornejo <gggion123@gmail.com>
;; Homepage: https://github.com/gggion/duckdb-query.el
;; Keywords: data sql

;; Package-Version: 0.2.0
;; Package-Requires: ((emacs "28.1"))

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

;; Convert DuckDB query results into native Emacs Lisp data structures.
;;
;; This package bridges DuckDB's analytical capabilities with Emacs Lisp's
;; data manipulation facilities by executing SQL queries and returning
;; results as idiomatic Elisp structures.
;;
;; Basic usage:
;;
;;     (duckdb-query "SELECT 42 as answer, 'hello' as greeting")
;;     ;; => ((("answer" . 42) ("greeting" . "hello")))
;;
;;     (duckdb-query "SELECT * FROM 'data.csv'" :format :columnar)
;;     ;; => (("id" . [1 2 3]) ("name" . ["Alice" "Bob" "Carol"]))
;;
;; The package provides:
;; - `duckdb-query' - Execute queries and return Elisp data structures
;; - `duckdb-query-execute-raw' - Low-level CLI execution
;;
;; Supported output formats:
;; - :alist (default) - List of association lists (row-oriented)
;; - :plist - List of property lists (row-oriented)
;; - :hash - List of hash tables (row-oriented)
;; - :vector - Vector of association lists (row-oriented)
;; - :columnar - Association list of vectors (column-oriented)
;; - :org-table - List of lists for org-mode tables
;;
;; Each format is optimized for different use cases:
;; - Row-oriented formats for record processing
;; - Columnar format for analytical operations
;; - Org-table format for display and export
;;
;; For performance benchmarking, see duckdb-query-bench.el.

;;; Code:

(require 'cl-lib)

;;; Customization

(defgroup duckdb-query nil
  "Execute DuckDB queries with transducers."
  :group 'data
  :prefix "duckdb-query-")

(defcustom duckdb-query-executable "duckdb"
  "Path to DuckDB executable.

Used by `duckdb-query-execute-raw'."
  :type 'string
  :group 'duckdb-query)

(defcustom duckdb-query-default-timeout 30
  "Default timeout in seconds for query execution.

Used by `duckdb-query-execute-raw' when TIMEOUT argument is nil."
  :type 'integer
  :group 'duckdb-query)

(defcustom duckdb-query-null-value :null
  "Value representing SQL NULL in query results.

Used by `duckdb-query' when parsing JSON output from DuckDB."
  :type '(choice (const :tag "Keyword :null" :null)
                 (const :tag "Symbol nil" nil)
                 (const :tag "String \"NULL\"" "NULL"))
  :group 'duckdb-query)

(defcustom duckdb-query-false-value :false
  "Value representing SQL FALSE in query results.

Used by `duckdb-query' when parsing JSON output from DuckDB."
  :type '(choice (const :tag "Keyword :false" :false)
                 (const :tag "Symbol nil" nil))
  :group 'duckdb-query)

;;; Core Functions

(defun duckdb-query-execute-raw (query &optional database _timeout)
  "Execute QUERY via DuckDB CLI and return raw JSON string.

QUERY is SQL string.
DATABASE is optional path to database file; nil uses in-memory.
_TIMEOUT is reserved for future use; currently ignored.

Returns raw JSON output string for parsing.
Signals error on non-zero exit code.

Called by `duckdb-query'.
Uses `duckdb-query-executable' for subprocess invocation."
  (with-temp-buffer
    (let* ((cmd (if database
                    (list duckdb-query-executable database "-json" "-c" query)
                  (list duckdb-query-executable "-json" "-c" query)))
           (default-directory temporary-file-directory)
           (process-environment (cons "DUCKDB_NO_COLOR=1" process-environment)))
      (let ((exit-code (apply #'call-process (car cmd) nil t nil (cdr cmd))))
        (if (zerop exit-code)
            (buffer-string)
          (error "DuckDB execution failed (exit %d): %s"
                 exit-code (string-trim (buffer-string))))))))

(defun duckdb-query--to-columnar (rows)
  "Convert ROWS to columnar format.

ROWS is list of alists from JSON parsing.

Returns alist of (COLUMN-NAME . VECTOR) pairs where each vector
contains all values for that column.

Uses hash table for O(1) column lookup and single-pass algorithm
to minimize cache misses.

Called by `duckdb-query' when FORMAT is `:columnar'."
  (when rows
    (let* ((row-count (length rows))
           (columns (mapcar #'car (car rows)))
           (col-count (length columns))
           (col-index (cl-loop with ht = (make-hash-table :test 'equal :size col-count)
                               for col in columns
                               for idx from 0
                               do (puthash col idx ht)
                               finally return ht))
           (col-vectors (make-vector col-count nil)))

      ;; Initialize column vectors
      (dotimes (i col-count)
        (aset col-vectors i (make-vector row-count nil)))

      ;; Fill vectors in single pass
      (cl-loop for row in rows
               for row-idx from 0
               do (dolist (cell row)
                    (when-let ((col-idx (gethash (car cell) col-index)))
                      (aset (aref col-vectors col-idx) row-idx (cdr cell)))))

      ;; Build result alist
      (cl-loop for col in columns
               for col-idx from 0
               collect (cons col (aref col-vectors col-idx))))))

(defun duckdb-query--to-org-table (rows)
  "Convert ROWS to org-table format.

ROWS is list of alists from JSON parsing.

Returns list of lists: first row is headers, remaining rows are values.
Each row is a list of column values in consistent order, with native
Lisp types (numbers, strings, symbols).

Uses hash table for O(1) column lookup and minimizes allocations.

Called by `duckdb-query' when FORMAT is `:org-table'."
  (when rows
    (let* ((headers (mapcar #'car (car rows)))
           (col-count (length headers))
           (col-index (cl-loop with ht = (make-hash-table :test 'equal :size col-count)
                               for col in headers
                               for idx from 0
                               do (puthash col idx ht)
                               finally return ht)))
      (cons headers
            (cl-loop for row in rows
                     collect (let ((row-vec (make-vector col-count nil)))
                               (dolist (cell row)
                                 (when-let ((idx (gethash (car cell) col-index)))
                                   (aset row-vec idx (cdr cell))))
                               (append row-vec nil)))))))

;;; Executor Protocol
(cl-defgeneric duckdb-query-execute (executor query &rest args)
  "Execute QUERY using EXECUTOR strategy, return JSON string.

EXECUTOR controls execution strategy:
  :cli       - Direct CLI invocation (default)
  :session   - Named session (requires session backend)
  function   - Custom executor function
  object     - Custom executor object with methods

QUERY is SQL string to execute.
ARGS are executor-specific parameters passed as plist.

Returns JSON string for parsing by conversion layer.
Signals error on execution failure.

This is a generic function. Implement methods for custom executors:

  (cl-defmethod duckdb-query-execute ((executor my-executor) query &rest args)
    ...)

The default `:cli' executor supports these ARGS:
  :database - Database file path (nil for in-memory)
  :readonly - Open database read-only
  :timeout  - Execution timeout in seconds"
  (error "No executor method defined for %S" executor))

(cl-defmethod duckdb-query-execute ((executor (eql :cli)) query &rest args)
  "Execute QUERY via EXECUTOR with ARGS parameters.

Supported ARGS:
  :database - Database file path (nil for in-memory)
  :readonly - Open database read-only (default t when :database provided)
  :timeout  - Execution timeout in seconds

Returns JSON string from DuckDB output.
Signals error on non-zero exit code.

Uses `duckdb-query-executable' for subprocess invocation.
Uses `duckdb-query-default-timeout' when :timeout not specified."
  (let* ((database (plist-get args :database))
         (readonly (if (plist-member args :readonly)
                       (plist-get args :readonly)
                     ;; Default to readonly when database specified
                     (and database t)))
         (timeout (or (plist-get args :timeout)
                      duckdb-query-default-timeout)))
    ;; Delegate to existing implementation
    (duckdb-query-execute-raw query database timeout)))

(cl-defmethod duckdb-query-execute ((executor function) query &rest args)
  "Execute QUERY by calling EXECUTOR function with QUERY and ARGS.

EXECUTOR must be a function accepting (query &rest args) and returning
JSON string.

This enables custom execution strategies without defining new executor
types.  The function receives QUERY as first argument followed by all
ARGS as keyword parameters.

Example:

  (defun my-executor (query &rest args)
    (let ((db (plist-get args :database)))
      ;; Custom execution logic
      ...))

  (duckdb-query \"SELECT 1\" :executor #\\='my-executor :database \"test.db\")

Returns JSON string from executor function.
Signals error if executor function signals error."
  (apply executor query args))

;; method for symbols that are functions
(cl-defmethod duckdb-query-execute ((executor symbol) query &rest args)
  "Execute QUERY by calling function named by EXECUTOR symbol.

EXECUTOR must be a symbol naming a function that accepts (query &rest args)
and returns JSON string.

This handles the case where users pass function names as symbols rather
than function objects.

Example:

  (duckdb-query \"SELECT 1\" :executor \\='my-executor)
  (duckdb-query \"SELECT 1\" :executor #\\='my-executor)

Returns JSON string from executor function.
Signals error if EXECUTOR is not a function or if function signals error."
  (unless (fboundp executor)
    (error "Symbol %S is not a function" executor))
  (apply executor query args))

(cl-defun duckdb-query (query &key
                              database
                              timeout
                              (format :alist)
                              (executor :cli))
  "Execute QUERY and return results in FORMAT.

QUERY is SQL string to execute.

DATABASE is optional database file path.  When nil, uses in-memory
transient database.

TIMEOUT is optional execution timeout in seconds.  Defaults to
`duckdb-query-default-timeout'.

FORMAT is output structure, one of:
  :alist     - list of alists (default)
  :plist     - list of plists
  :hash      - list of hash-tables
  :vector    - vector of alists
  :columnar  - alist of column vectors
  :org-table - list of lists for `org-mode' tables

EXECUTOR controls execution strategy:
  :cli      - Direct CLI invocation (default)
  function  - Custom executor function
  object    - Custom executor object

Returns nil for empty results.
Returns converted data in FORMAT for successful queries.
Returns raw output string for non-JSON results (errors, non-tabular output).

Uses `duckdb-query-execute' for execution via executor protocol.
Uses `json-parse-string' for C-level JSON parsing.
Uses `duckdb-query-null-value' and `duckdb-query-false-value'
for null/false representation.
Uses `duckdb-query--to-columnar' for columnar conversion.
Uses `duckdb-query--to-org-table' for org-table conversion.

Examples:

  ;; Default: in-memory database, alist format
  (duckdb-query \"SELECT 42 as answer\")
  ;; => ((((\"answer\" . 42))))

  ;; Query database file
  (duckdb-query \"SELECT * FROM users\" :database \"app.db\")

  ;; Columnar format for analysis
  (duckdb-query \"SELECT * FROM data.csv\" :format :columnar)
  ;; => ((\"id\" . [1 2 3]) (\"name\" . [\"Alice\" \"Bob\" \"Carol\"]))

  ;; Custom executor
  (duckdb-query \"SELECT 1\" :executor #\\='my-custom-executor)"
  (let* ((json-output (duckdb-query-execute executor query
                                            :database database
                                            :timeout timeout)))
    (when (and json-output (not (string-empty-p (string-trim json-output))))
      (condition-case err
          ;; Try to parse as JSON with format-specific parameters
          (pcase format
            (:alist
             (json-parse-string json-output
                                :object-type 'alist
                                :array-type 'list
                                :null-object duckdb-query-null-value
                                :false-object duckdb-query-false-value))
            (:plist
             (json-parse-string json-output
                                :object-type 'plist
                                :array-type 'list
                                :null-object duckdb-query-null-value
                                :false-object duckdb-query-false-value))
            (:hash
             (json-parse-string json-output
                                :object-type 'hash-table
                                :array-type 'list
                                :null-object duckdb-query-null-value
                                :false-object duckdb-query-false-value))
            (:vector
             (json-parse-string json-output
                                :object-type 'alist
                                :array-type 'array
                                :null-object duckdb-query-null-value
                                :false-object duckdb-query-false-value))
            (:columnar
             (duckdb-query--to-columnar
              (json-parse-string json-output
                                 :object-type 'alist
                                 :array-type 'list
                                 :null-object duckdb-query-null-value
                                 :false-object duckdb-query-false-value)))
            (:org-table
             (duckdb-query--to-org-table
              (json-parse-string json-output
                                 :object-type 'alist
                                 :array-type 'list
                                 :null-object duckdb-query-null-value
                                 :false-object duckdb-query-false-value)))
            (_
             (error "Unknown format: %s.  Valid: :alist :plist :hash :vector :columnar :org-table"
                    format)))
        (json-parse-error
         ;; Not JSON or malformed - return raw output
         ;; This handles: errors, non-tabular results, etc.
         json-output)))))

(provide 'duckdb-query)

;;; duckdb-query.el ends here
