;;; duckdb-query.el --- DuckDB query results as native Elisp data structures -*- lexical-binding: t; -*-
;;
;; Author: Gino Cornejo
;; Maintainer: Gino Cornejo <gggion123@gmail.com>
;; Homepage: https://github.com/gggion/duckdb-query.el
;; Keywords: data sql

;; Package-Version: 0.4.0
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
;; - `duckdb-query-execute' - Generic function for pluggable executors
;; - `duckdb-query-execute-raw' - Low-level CLI execution
;;
;; Execution strategies (via :executor parameter):
;; - :cli (default) - Direct CLI invocation
;; - function - Custom executor function
;; - Custom objects via cl-defmethod
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

;;;; Customization

(defgroup duckdb-query nil
  "Execute DuckDB queries and parse results into elisp data structures."
  :group 'data
  :prefix "duckdb-query-")

(defcustom duckdb-query-executable "duckdb"
  "Path to DuckDB executable.

Used by `duckdb-query-execute-raw' and the `:cli' executor method."
  :type 'string
  :group 'duckdb-query
  :package-version '(duckdb-query . "0.1.0"))

(defcustom duckdb-query-default-timeout 30
  "Default timeout in seconds for query execution.

Used by `:cli' executor when :timeout argument is nil."
  :type 'integer
  :group 'duckdb-query
  :package-version '(duckdb-query . "0.1.0"))

(defcustom duckdb-query-null-value :null
  "Value representing SQL NULL in query results.

Used when parsing JSON output from DuckDB."
  :type '(choice (const :tag "Keyword :null" :null)
                 (const :tag "Symbol nil" nil)
                 (const :tag "String \"NULL\"" "NULL"))
  :group 'duckdb-query
  :package-version '(duckdb-query . "0.1.0"))

(defcustom duckdb-query-false-value :false
  "Value representing SQL FALSE in query results.

Used when parsing JSON output from DuckDB."
  :type '(choice (const :tag "Keyword :false" :false)
                 (const :tag "Symbol nil" nil))
  :group 'duckdb-query
  :package-version '(duckdb-query . "0.1.0"))

;;;; Internal Variables

(defvar duckdb-query-default-database nil
  "Default database file for `duckdb-query' when not specified.

When non-nil, all queries use this database unless overridden by
explicit :database parameter.

Set via `duckdb-query-set-default-database' or dynamically bound
via `duckdb-with-database'.

Nil means in-memory transient database.")

;;;; Data Extraction Utilities

(defun duckdb-query-value (query &rest args)
  "Return single scalar value from QUERY.

QUERY is SQL string that must return exactly one row with one column.
ARGS are passed to `duckdb-query'.

Returns the scalar value, or nil for empty results.

Signals `user-error' if result has multiple rows or columns.

Example:

  (duckdb-query-value \"SELECT COUNT(*) FROM users\")
  ;; => 42

  (duckdb-query-value \"SELECT MAX(price) FROM orders\"
                      :database \"sales.db\")
  ;; => 99.95

Also see `duckdb-query-column' for extracting entire columns."
  (let* ((result (apply #'duckdb-query query :format :alist args))
         (row (car result)))
    (cond
     ((null result)
      nil)
     ((cdr result)
      (user-error "duckdb-query-value: query returned %d rows, expected 1"
                  (length result)))
     ((cdr row)
      (user-error "duckdb-query-value: query returned %d columns, expected 1"
                  (length row)))
     (t
      (cdar row)))))

(defun duckdb-query-column (query &rest args)
  "Return single column from QUERY as list.

QUERY is SQL string to execute.

ARGS are keyword arguments:
  :column    - Column name string.  When nil, returns first column.
  :as-vector - When non-nil, return vector instead of list.

Other ARGS are passed to `duckdb-query'.

Returns list of column values, or vector if :as-vector is non-nil.
Returns nil for empty results.

Example:

  (duckdb-query-column \"SELECT name FROM users ORDER BY id\")
  ;; => (\"Alice\" \"Bob\" \"Carol\")

  (duckdb-query-column \"SELECT id, name FROM users\"
                       :column \"name\")
  ;; => (\"Alice\" \"Bob\" \"Carol\")

  (duckdb-query-column \"SELECT value FROM data\" :as-vector t)
  ;; => [1 2 3 4 5]

Also see `duckdb-query-value' for scalar extraction.
Also see `duckdb-query' with :format :columnar for multiple columns."
  (let* ((column (plist-get args :column))
         (as-vector (plist-get args :as-vector))
         (clean-args (cl-loop for (k v) on args by #'cddr
                              unless (memq k '(:column :as-vector))
                              append (list k v)))
         (result (apply #'duckdb-query query :format :columnar clean-args))
         (col-data (if column
                       ;; Column names in columnar format are symbols
                       (cdr (assq (intern column) result))
                     (cdar result))))
    (cond
     ((null col-data)
      nil)
     (as-vector
      col-data)
     (t
      (append col-data nil)))))

;;;; Schema Introspection

(defun duckdb-query-describe (source &rest args)
  "Return schema information for SOURCE.

SOURCE can be:
- Table name: \"users\"
- File path: \"/path/to/data.parquet\" or \"~/data.csv\"
- Remote URL: \"https://example.com/data.csv\"
- SELECT query: \"SELECT * FROM users WHERE active\"

ARGS are passed to `duckdb-query'.

For SELECT queries, wraps SOURCE in DESCRIBE(...).
For tables, files, and URLs, uses DESCRIBE directly.

Returns list of alists, one per column:

  (((\"column_name\" . \"id\")
    (\"column_type\" . \"INTEGER\")
    (\"null\" . \"YES\")
    (\"key\" . :null)
    (\"default\" . :null)
    (\"extra\" . :null))
   ...)

The =column_name= and =column_type= fields are always populated.
Other fields may be :null depending on SOURCE type; database tables
with constraints populate =key= and =default=.

Signals `user-error' for DDL statements (CREATE, INSERT, DROP, etc.)
which cannot be described directly.  Create the table first, then
describe it by name.

Uses `duckdb-query-default-database' when SOURCE is a table name.

Example:

  (duckdb-query-describe \"users\")
  (duckdb-query-describe \"~/data/sales.parquet\")
  (duckdb-query-describe \"SELECT a, b FROM t WHERE x > 10\")

Also see `duckdb-query-columns' to extract column names.
Also see `duckdb-query-column-types' for name-to-type mapping."
  (let* ((source-trimmed (string-trim source))
         (query
          (cond
           ;; File path (absolute, home-relative, or relative with extension)
           ((or (string-prefix-p "/" source-trimmed)
                (string-prefix-p "~" source-trimmed)
                (string-match-p "\\.\\(parquet\\|csv\\|json\\)\\'" source-trimmed))
            (format "DESCRIBE '%s'" source-trimmed))
           ;; Remote URL
           ((string-match-p "\\`https?://" source-trimmed)
            (format "DESCRIBE '%s'" source-trimmed))
           ;; SELECT query - wrap in DESCRIBE(...)
           ((string-match-p "\\`SELECT\\b" source-trimmed)
            (format "DESCRIBE (%s)" source-trimmed))
           ;; DDL - reject with guidance
           ((string-match-p
             "\\`\\(CREATE\\|INSERT\\|UPDATE\\|DELETE\\|DROP\\|ALTER\\)\\b"
             source-trimmed)
            (user-error
             "Cannot describe DDL statement; create table first, then describe by name"))
           ;; Assume table name
           (t
            (format "DESCRIBE %s" source-trimmed)))))
    (apply #'duckdb-query query :format :alist args)))

(defun duckdb-query-columns (source &rest args)
  "Return list of column names for SOURCE.

SOURCE can be table name, file path, URL, or SELECT query.
ARGS are passed to `duckdb-query-describe'.

Returns list of column name strings.

Example:

  (duckdb-query-columns \"users\")
  ;; => (\"id\" \"name\" \"email\" \"created_at\")

  (duckdb-query-columns \"~/data/sales.parquet\")
  ;; => (\"date\" \"product\" \"quantity\" \"price\")

Also see `duckdb-query-describe' for full column metadata.
Also see `duckdb-query-column-types' for name-to-type mapping."
  (mapcar (lambda (row) (cdr (assq 'column_name row)))
          (apply #'duckdb-query-describe source args)))

(defun duckdb-query-column-types (source &rest args)
  "Return alist mapping column names to DuckDB types for SOURCE.

SOURCE can be table name, file path, URL, or SELECT query.
ARGS are passed to `duckdb-query-describe'.

Returns alist of (NAME . TYPE) pairs where NAME is column name
string and TYPE is DuckDB type string.

Example:

  (duckdb-query-column-types \"users\")
  ;; => ((\"id\" . \"INTEGER\")
  ;;     (\"name\" . \"VARCHAR\")
  ;;     (\"created_at\" . \"TIMESTAMP\"))

Also see `duckdb-query-describe' for full column metadata.
Also see `duckdb-query-columns' for just column names."
  (mapcar (lambda (row)
            (cons (cdr (assq 'column_name row))
                  (cdr (assq 'column_type row))))
          (apply #'duckdb-query-describe source args)))

;;;; Context Management Macro

(defmacro duckdb-with-database (database &rest body)
  "Execute BODY with DATABASE as default connection.

All `duckdb-query' calls within BODY use DATABASE unless overridden
by explicit :database parameter.

DATABASE is evaluated once and bound dynamically for BODY execution.

Returns value of last form in BODY.

Example:

  (duckdb-with-database \"app.db\"
    (duckdb-query \"CREATE TABLE users (id INTEGER, name TEXT)\"
                  :readonly nil)
    (duckdb-query \"INSERT INTO users VALUES (1, \\='Alice\\=')\"
                  :readonly nil)
    (duckdb-query \"SELECT * FROM users\"))
  ;; => (((\"id\" . 1) (\"name\" . \"Alice\")))"
  (declare (indent 1))
  `(let ((duckdb-query-default-database ,database))
     ,@body))

(defmacro duckdb-with-transient-database (&rest body)
  "Execute BODY with temporary file-based database.

Creates temporary database file, executes BODY with that database as
default, then deletes the file.  Unlike in-memory databases, temporary
file databases persist across multiple CLI invocations within BODY,
enabling queries that build on previous results.

Returns value of last form in BODY.

The temporary database file is deleted even if BODY signals error.

Example:

  (duckdb-with-transient-database
    (duckdb-query \"CREATE TABLE temp (id INTEGER)\" :readonly nil)
    (duckdb-query \"INSERT INTO temp VALUES (1), (2), (3)\" :readonly nil)
    (duckdb-query \"SELECT COUNT(*) as count FROM temp\"))
  ;; => (((\"count\" . 3)))

Note: Cannot use in-memory database because each CLI invocation creates
separate process with separate \":memory:\" database.  File-based approach
ensures state persists across queries."
  (declare (indent 0))
  (let ((db-var (make-symbol "transient-db")))
    `(let ((,db-var (expand-file-name
                     (concat (make-temp-name "duckdb-transient-")
                             ".duckdb")
                     temporary-file-directory)))
       (unwind-protect
           (let ((duckdb-query-default-database ,db-var))
             ,@body)
         (when (file-exists-p ,db-var)
           (delete-file ,db-var))))))

;;;; Interactive Functions
(defun duckdb-query-set-default-database (&optional path)
  "Set default database for `duckdb-query' to PATH.

If PATH is nil or empty string, uses in-memory database.

Returns previous default database.

When called interactively, prompts for database file path.

Example:

  ;; Set global default
  (duckdb-query-set-default-database \"app.db\")

  ;; All subsequent queries use app.db
  (duckdb-query \"SELECT * FROM users\")

  ;; Clear default (use in-memory)
  (duckdb-query-set-default-database nil)"
  (interactive "fDatabase file (blank for in-memory): ")
  (prog1 duckdb-query-default-database
    (setq duckdb-query-default-database
          (if (and path (not (string-empty-p path)))
              (expand-file-name path)
            nil))))

;;;; Executor Protocol
;;;;; Generic Function

(cl-defgeneric duckdb-query-execute (executor query &rest args)
  "Execute QUERY using EXECUTOR strategy, return JSON string.

This is the core extension point for pluggable execution strategies.
Implement methods for custom executors to integrate different backends
while leveraging the unified conversion layer.

EXECUTOR controls execution strategy:
  :cli       - Direct CLI invocation (default)
  :session   - Named session (requires session backend)
  function   - Custom executor function
  symbol     - Function name as symbol
  object     - Custom executor object with methods

QUERY is SQL string to execute.

ARGS are executor-specific parameters passed as plist.  The `:cli'
executor supports:
  :database - Database file path (nil for in-memory)
  :readonly - Open database read-only
  :timeout  - Execution timeout in seconds

Returns JSON string for parsing by conversion layer.
Signals error on execution failure.

To implement a custom executor:

  (cl-defmethod duckdb-query-execute ((executor my-executor) query &rest args)
    \"Execute QUERY using my custom backend.\"
    ;; Your implementation here
    ;; Must return JSON string
    )

See Info node `(duckdb-query) Executors' for details."
  (error "No executor method defined for %S" executor))

;;;;; CLI Executor

(defun duckdb-query--build-cli-args (&rest params)
  "Build DuckDB CLI argument list from PARAMS.

PARAMS is plist with keys:
  :database     - Database file path (nil for in-memory)
  :readonly     - Open database read-only
  :output-mode  - DuckDB output mode (json, csv, etc.)
  :init-file    - SQL file to execute before query
  :separator    - Column separator for CSV mode
  :nullvalue    - String to display for NULL values

Returns list of strings suitable for `call-process' invocation.

Used by `duckdb-query-execute' :cli method to construct command-line
arguments from keyword parameters.

Example:
  (duckdb-query--build-cli-args
   :database \"test.db\"
   :readonly t
   :output-mode \\='json)
  ;; => (\"duckdb\" \"test.db\" \"-readonly\" \"-json\")"
  (let ((args (list duckdb-query-executable))
        (database (plist-get params :database))
        (readonly (plist-get params :readonly))
        (output-mode (plist-get params :output-mode))
        (init-file (plist-get params :init-file))
        (separator (plist-get params :separator))
        (nullvalue (plist-get params :nullvalue)))

    ;; param insertion
    (when database (push (expand-file-name database) args))   ;; Add database file if specified
    (when readonly (push "-readonly" args))                   ;; Add readonly flag
    (when output-mode (push (format "-%s" output-mode) args)) ;; Add output mode
    (when init-file (push "-init" args) (push (expand-file-name init-file) args)) ;; Add init file
    (when separator (push "-separator" args) (push separator args))               ;; Add separator
    (when nullvalue (push "-nullvalue" args) (push nullvalue args))               ;; Add nullvalue

    (nreverse args)))

(defun duckdb-query--invoke-cli (cli-args query _timeout)
  "Invoke DuckDB CLI with CLI-ARGS, executing QUERY with TIMEOUT.

CLI-ARGS is list of command-line arguments from
`duckdb-query--build-cli-args'.
QUERY is SQL string to execute.
TIMEOUT is execution timeout in seconds (currently unused).

Returns output string on success.
Signals error with DuckDB's message on failure.

Uses `call-process' for subprocess invocation.
Sets DUCKDB_NO_COLOR environment variable to disable color codes.

Called by `duckdb-query-execute' :cli method."
  (with-temp-buffer
    (let* ((default-directory temporary-file-directory)
           (process-environment (cons "DUCKDB_NO_COLOR=1" process-environment))
           (exit-code (apply #'call-process
                             (car cli-args) nil t nil
                             (append (cdr cli-args)
                                     (list "-c" query))))
           (output (buffer-string)))
      (if (zerop exit-code)
          output
        (error "DuckDB execution failed (exit %d): %s"
               exit-code (string-trim output))))))

(cl-defmethod duckdb-query-execute ((_executor (eql :cli)) query &rest args)
  "Run QUERY with EXECUTOR via DuckDB CLI with enhanced parameter support.

Supported ARGS:
  :database     - Database file path (nil for in-memory)
  :readonly     - Open database read-only (default t when :database provided)
  :timeout      - Execution timeout in seconds
  :output-mode  - DuckDB output mode (default \\='json)
  :init-file    - SQL file to execute before query
  :separator    - Column separator for CSV mode
  :nullvalue    - String to display for NULL values

Returns JSON string from DuckDB output.
Signals error on non-zero exit code.

Uses `duckdb-query--build-cli-args' to construct command-line arguments.
Uses `duckdb-query--invoke-cli' for subprocess invocation.
Uses `duckdb-query-executable' and `duckdb-query-default-timeout'."
  (let* ((database (plist-get args :database))
         (readonly (if (plist-member args :readonly)
                       (plist-get args :readonly)
                     ;; Default to readonly when database specified
                     (and database t)))
         (timeout (or (plist-get args :timeout)
                      duckdb-query-default-timeout))
         (output-mode (or (plist-get args :output-mode) 'json))
         (init-file (plist-get args :init-file))
         (separator (plist-get args :separator))
         (nullvalue (plist-get args :nullvalue))
         (cli-args (duckdb-query--build-cli-args
                    :database database
                    :readonly readonly
                    :output-mode output-mode
                    :init-file init-file
                    :separator separator
                    :nullvalue nullvalue)))
    (duckdb-query--invoke-cli cli-args query timeout)))

;;;;; Function Executors

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

;;;; Low-Level Execution
(defun duckdb-query-execute-raw (query &optional database _timeout)
  "Execute QUERY via DuckDB CLI and return raw JSON string.

This is the low-level execution primitive used by the `:cli' executor.
Most code should use `duckdb-query' or `duckdb-query-execute' instead.

QUERY is SQL string.
DATABASE is optional path to database file; nil uses in-memory.
_TIMEOUT is reserved for future use; currently ignored.

Returns raw JSON output string for parsing.
Signals error on non-zero exit code.

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

;;;; Format Conversion
;;;;; Columnar Format

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

;;;;; Org-Table Format

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

;;;; Main Entry Point

(cl-defun duckdb-query (query &rest args &key
                              database
                              timeout
                              (format :alist)
                              (executor :cli)
                              &allow-other-keys)
  "Execute QUERY and return results in FORMAT.

This is the main entry point for executing DuckDB queries and converting
results to Elisp data structures.

QUERY is SQL string to execute.

DATABASE is optional database file path.  When nil, uses
`duckdb-query-default-database' if set, otherwise uses in-memory
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
  symbol    - Function name as symbol
  object    - Custom executor object

Additional keyword arguments in ARGS are passed to EXECUTOR.
The :cli executor supports:
  :readonly     - Open database read-only (default t when :database provided)
  :output-mode  - DuckDB output mode (default \\='json)
  :init-file    - SQL file to execute before query
  :separator    - Column separator for CSV mode
  :nullvalue    - String to display for NULL values

Returns nil for empty results.
Returns converted data in FORMAT for successful queries.
Returns raw output string for non-JSON results (errors, non-tabular output).

Execution is delegated to `duckdb-query-execute' generic function,
enabling pluggable backends.  JSON parsing uses `json-parse-string'
with format-specific :object-type parameter for efficient conversion.

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

  ;; Use default database
  (setq duckdb-query-default-database \"app.db\")
  (duckdb-query \"SELECT * FROM users\")

  ;; Columnar format for analysis
  (duckdb-query \"SELECT * FROM data.csv\" :format :columnar)
  ;; => ((\"id\" . [1 2 3]) (\"name\" . [\"Alice\" \"Bob\" \"Carol\"]))

  ;; Custom executor
  (duckdb-query \"SELECT 1\" :executor #\\='my-custom-executor)

  ;; With init file
  (duckdb-query \"SELECT * FROM test\"
                :database \"app.db\"
                :init-file \"setup.sql\")"
  (let* ((db (or database duckdb-query-default-database))
         (json-output (apply #'duckdb-query-execute
                             executor
                             query
                             :database db
                             :timeout timeout
                             args)))
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

(cl-defun duckdb-query-file (file &key database readonly (format :alist))
  "Execute SQL from FILE and return results in FORMAT.

FILE is path to SQL file containing query.
DATABASE is optional database file path (nil for in-memory).
READONLY defaults to t when DATABASE specified.
FORMAT is output structure (:alist, :columnar, :org-table, etc.).

Returns converted results in specified FORMAT.
Returns nil for empty results.
Returns raw output string for non-JSON results.

Uses `duckdb-query' with SQL read from FILE.

Example:
  (duckdb-query-file \"queries/analysis.sql\"
                     :database \"data.db\"
                     :format :columnar)"
  (let ((sql (with-temp-buffer
               (insert-file-contents file)
               (buffer-string))))
    (duckdb-query sql
                  :database database
                  :readonly readonly
                  :format format)))

(provide 'duckdb-query)

;;; duckdb-query.el ends here
