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

;; Execute DuckDB queries and return results as Elisp data structures.
;;
;; Basic usage:
;;
;;     (duckdb-query "SELECT 42 as answer, 'hello' as greeting")
;;     ;; => (((answer . 42) (greeting . "hello")))
;;
;;     (duckdb-query "SELECT * FROM 'data.csv'" :format :columnar)
;;     ;; => ((id . [1 2 3]) (name . ["Alice" "Bob" "Carol"]))
;;
;; Database context management:
;;
;;     (duckdb-with-database "app.db"
;;       (duckdb-query "SELECT * FROM users"))
;;
;; The package provides:
;; - `duckdb-query' - Execute queries with format conversion
;; - `duckdb-query-value' - Extract single scalar value
;; - `duckdb-query-column' - Extract single column as list
;; - `duckdb-query-describe' - Schema introspection
;; - `duckdb-with-database' - Scoped database context
;; - `duckdb-with-transient-database' - Temporary file-based database
;;
;; Output formats via :format parameter:
;; - :alist (default), :plist, :hash, :vector, :columnar, :org-table
;;
;; Execution strategies via :executor parameter:
;; - :cli (default) - Direct CLI invocation
;; - function - Custom executor function
;; - Custom objects via `cl-defmethod'
;;
;; For benchmarking, see duckdb-query-bench.el.
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
      (user-error "Query returned %d rows, expected 1"
                  (length result)))
     ((cdr row)
      (user-error "Query returned %d columns, expected 1"
                  (length row)))
     (t
      (cdar row)))))

(defun duckdb-query-column (query &rest args)
  "Return single column from QUERY as list.

QUERY is SQL string to execute.

ARGS are keyword arguments:
  :column    - Column name string.  When nil, return first column.
  :as-vector - When non-nil, return vector instead of list.

Other ARGS are passed to `duckdb-query'.

Return list of column values, or vector if :as-vector is non-nil.
Return nil for empty results.

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
- Cloud storage: \"s3://bucket/data.parquet\"
- SELECT query: \"SELECT * FROM users WHERE active\"
- CTE query: \"WITH cte AS (...) SELECT ...\"

ARGS are passed to `duckdb-query'.

For SELECT and WITH queries, wrap SOURCE in DESCRIBE(...).
For tables, files, and URLs, use DESCRIBE directly.

Return list of alists, one per column:

  (((column_name . \"id\")
    (column_type . \"INTEGER\")
    (null . \"YES\")
    (key . :null)
    (default . :null)
    (extra . :null))
   ...)

The `column_name' and `column_type' fields are always populated.
Other fields may be :null depending on SOURCE type; database tables
with constraints populate `key' and `default'.

Signal `user-error' for DDL statements (CREATE, INSERT, DROP, etc.)
which cannot be described directly.  Create the table first, then
describe it by name.

Use `duckdb-query-default-database' when SOURCE is a table name.

Example:

  (duckdb-query-describe \"users\")
  (duckdb-query-describe \"~/data/sales.parquet\")
  (duckdb-query-describe \"SELECT a, b FROM t WHERE x > 10\")

Also see `duckdb-query-columns' to extract column names.
Also see `duckdb-query-column-types' for name-to-type mapping."
  (let* ((source-trimmed (string-trim source))
         (source-upper (upcase source-trimmed))
         (query
          (cond
           ;; File path (absolute, home-relative, or with data file extension)
           ((or (string-prefix-p "/" source-trimmed)
                (string-prefix-p "~" source-trimmed)
                (string-match-p "\\.\\(parquet\\|csv\\|json\\|jsonl\\|ndjson\\)\\(?:\\.gz\\)?\\'"
                                source-trimmed))
            (format "DESCRIBE '%s'" source-trimmed))
           ;; Remote URL (http, https, s3, gs, az, hf)
           ((string-match-p "\\`\\(https?\\|s3\\|gs\\|az\\|hf\\)://" source-trimmed)
            (format "DESCRIBE '%s'" source-trimmed))
           ;; SELECT or WITH query - wrap in DESCRIBE(...)
           ((string-match-p "\\`\\(SELECT\\|WITH\\)\\b" source-upper)
            (format "DESCRIBE (%s)" source-trimmed))
           ;; DDL - reject with guidance
           ((string-match-p
             "\\`\\(CREATE\\|INSERT\\|UPDATE\\|DELETE\\|DROP\\|ALTER\\|TRUNCATE\\)\\b"
             source-upper)
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

Return list of column name strings.

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

Return alist of (NAME . TYPE) pairs where NAME is column name
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

Return value of last form in BODY.

Example:

  (duckdb-with-database \"app.db\"
    (duckdb-query \"CREATE TABLE users (id INTEGER, name TEXT)\"
                  :readonly nil)
    (duckdb-query \"INSERT INTO users VALUES (1, \\='Alice\\=')\"
                  :readonly nil)
    (duckdb-query \"SELECT * FROM users\"))
  ;; => (((\"id\" . 1) (\"name\" . \"Alice\")))"
  (declare (indent 1) (debug t))
  `(let ((duckdb-query-default-database ,database))
     ,@body))

(defmacro duckdb-with-transient-database (&rest body)
  "Execute BODY with temporary file-based database.

Create temporary database file, execute BODY with that database as
default, then delete the file.  Unlike in-memory databases, temporary
file databases persist across multiple CLI invocations within BODY,
enabling queries that build on previous results.

Return value of last form in BODY.

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
  (declare (indent 0) (debug t))
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

If PATH is nil or empty string, use in-memory database.

Return previous default database.

When called interactively, prompt for database file path.

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

(cl-defgeneric duckdb-query-execute (executor _query &rest _args)
  "Execute QUERY using EXECUTOR strategy, return JSON string.

This is the core extension point for pluggable execution strategies.
Implement methods for custom executors to integrate different backends
while leveraging the unified conversion layer.

EXECUTOR controls execution strategy:
  :cli       - Direct CLI invocation (default)
  function   - Custom executor function
  symbol     - Function name as symbol
  object     - Custom executor object with `cl-defmethod'

QUERY is SQL string to execute.

ARGS are executor-specific parameters passed as plist.  The `:cli'
executor supports:
  :database - Database file path (nil for in-memory)
  :readonly - Open database read-only
  :timeout  - Execution timeout in seconds

Return JSON string for parsing by conversion layer.
Signal error on execution failure.

To implement custom executor, define method with `cl-defmethod':

  (cl-defmethod duckdb-query-execute ((exec my-type) query &rest args)
    \"Execute QUERY with custom backend.\"
    ...)

Called by `duckdb-query' to delegate execution.
Also see Info node `(duckdb-query) Executors' for details."
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

Return list of strings suitable for `call-process' invocation.

Used by `duckdb-query-execute' `:cli' method to construct command-line
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
    (when database (push (expand-file-name database) args))
    (when readonly (push "-readonly" args))
    (when output-mode (push (format "-%s" output-mode) args))
    (when init-file (push "-init" args) (push (expand-file-name init-file) args))
    (when separator (push "-separator" args) (push separator args))
    (when nullvalue (push "-nullvalue" args) (push nullvalue args))

    (nreverse args)))

(defun duckdb-query--invoke-cli (cli-args query _timeout)
  "Invoke DuckDB CLI with CLI-ARGS, executing QUERY with TIMEOUT.

CLI-ARGS is list of command-line arguments from
`duckdb-query--build-cli-args'.
QUERY is SQL string to execute.
TIMEOUT is execution timeout in seconds (currently unused).

Return output string on success.
Signal error with DuckDB's message on failure.

Use `call-process' for subprocess invocation.
Set DUCKDB_NO_COLOR environment variable to disable color codes.

Called by `duckdb-query-execute' `:cli' method."
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
  "Execute QUERY via DuckDB CLI with ARGS parameters.

Supported ARGS:
  :database     - Database file path (nil for in-memory)
  :readonly     - Open database read-only (default t when :database provided)
  :timeout      - Execution timeout in seconds
  :output-mode  - DuckDB output mode (default \\='json)
  :init-file    - SQL file to execute before query
  :separator    - Column separator for CSV mode
  :nullvalue    - String to display for NULL values

Return JSON string from DuckDB output.
Signal error on non-zero exit code.

Use `duckdb-query--build-cli-args' to construct command-line arguments.
Use `duckdb-query--invoke-cli' for subprocess invocation.
Use `duckdb-query-executable' and `duckdb-query-default-timeout'."
  (let* ((database (plist-get args :database))
         (readonly (if (plist-member args :readonly)
                       (plist-get args :readonly)
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

Return JSON string from executor function.
Signal error if executor function signals error.

Example:

  (defun my-executor (query &rest args)
    (let ((db (plist-get args :database)))
      ;; Custom execution logic
      ...))

  (duckdb-query \"SELECT 1\" :executor #\\='my-executor :database \"test.db\")"
  (apply executor query args))

(cl-defmethod duckdb-query-execute ((executor symbol) query &rest args)
  "Execute QUERY by calling function named by EXECUTOR symbol.

EXECUTOR must be symbol naming function that accepts (query &rest args)
and returns JSON string.

This handles the case where users pass function names as symbols rather
than function objects.

Return JSON string from executor function.
Signal error if EXECUTOR is not a function or if function signals error.

Example:

  (duckdb-query \"SELECT 1\" :executor \\='my-executor)
  (duckdb-query \"SELECT 1\" :executor #\\='my-executor)"
  (unless (fboundp executor)
    (error "Symbol %S is not a function" executor))
  (apply executor query args))

;;;; Low-Level Execution
(defun duckdb-query-execute-raw (query &optional database _timeout)
  "Execute QUERY via DuckDB CLI and return raw JSON string.

This is the low-level execution primitive.  Most code should use
`duckdb-query' or `duckdb-query-execute' instead.

QUERY is SQL string.
DATABASE is optional path to database file; nil uses in-memory.
TIMEOUT is reserved for future use; currently ignored.

Return raw JSON output string for parsing.
Signal error on non-zero exit code.

Use `duckdb-query-executable' for subprocess invocation.
Wrapped by `duckdb-query-execute' `:cli' method."
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

Return alist of (COLUMN-NAME . VECTOR) pairs where each vector
contains all values for that column.

Use hash table for O(1) column lookup and single-pass algorithm
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

Return list of lists: first row is headers, remaining rows are values.
Each row is a list of column values in consistent order, with native
Lisp types (numbers, strings, symbols).

Use hash table for O(1) column lookup and minimize allocations.

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


(defun duckdb-query-org-table-to-alist (org-table)
  "Convert ORG-TABLE to list of alists.

ORG-TABLE is list of lists where first row is headers and
remaining rows are data values (as returned by `duckdb-query'
with :format :org-table).

Return list of alists suitable for use with `duckdb-query' :data
parameter.

Example:
  (duckdb-query-org-table-to-alist
   \\='((id name) (1 \"Alice\") (2 \"Bob\")))
  ;; => (((id . 1) (name . \"Alice\"))
  ;;     ((id . 2) (name . \"Bob\")))"
  (let ((headers (car org-table)))
    (cl-loop for row in (cdr org-table)
             collect (cl-loop for h in headers
                              for v in row
                              collect (cons h v)))))

(defun duckdb-query--alist-to-csv-file (alist-data file)
  "Write ALIST-DATA directly to FILE as CSV.

ALIST-DATA is list of alists where each alist represents a row
and each cons cell is (column . value).

FILE is path to write CSV output.

Writes directly to file buffer, avoiding intermediate string
allocation.  Handles RFC 4180 CSV escaping: strings containing
commas, quotes, or newlines are quoted with doubled internal
quotes.

Called by `duckdb-query--substitute-data-refs' for data
serialization."
  (with-temp-file file
    (let ((headers (mapcar #'car (car alist-data))))
      ;; Header row
      (insert (mapconcat #'symbol-name headers ",") "\n")
      ;; Data rows
      (dolist (row alist-data)
        (insert
         (mapconcat
          (lambda (cell)
            (let ((v (cdr cell)))
              (cond
               ((stringp v)
                (if (string-match-p "[,\"\n]" v)
                    (format "\"%s\"" (replace-regexp-in-string "\"" "\"\"" v))
                  v))
               ((null v) "")
               (t (prin1-to-string v)))))
          row ",")
         "\n")))))

(defun duckdb-query--substitute-data-refs (query data temp-files)
  "Replace @SYMBOL references in QUERY with temp file paths.

DATA is the :data parameter in one of these forms:
  - List of alists: bound to @data implicitly
  - Alist of (SYMBOL . VALUE) pairs: symbols become @symbol references

Values must be actual alist data, not symbols to be evaluated.
Use backquote with comma to pass lexical variables:

  (let ((my-data \\='(((id . 1)))))
    (duckdb-query \"SELECT * FROM @data\" :data \\=`((data . ,my-data))))

TEMP-FILES is hash table mapping symbols to temp file paths for
cleanup tracking.

Return modified query string.

Examples:
  Single source (implicit @data):
    :data \\='(((id . 1) (name . \"Alice\")))
    :data my-variable  ; when my-variable holds list of alists

  Named bindings with backquote:
    :data \\=`((orders . ,my-orders-var))
    :data \\=`((orders . ,orders) (users . ,users))

  Inline data:
    :data \\='((orders . (((id . 1) (product . \"Widget\")))))

Called by `duckdb-query' when DATA parameter is provided.
Uses `duckdb-query--alist-to-csv-file' for serialization."
  (let ((result query)
        (bindings
         (cond
          ;; nil -> no bindings
          ((null data)
           nil)

          ;; Direct data: list of alists -> bind to 'data
          ;; Detect by checking if first element looks like a row (alist)
          ;; rather than a binding (symbol . list-of-alists)
          ;;
          ;; Direct data example: (((id . 1) (name . "Alice")) ((id . 2) ...))
          ;;   (car data) = ((id . 1) (name . "Alice"))  ; a row/alist
          ;;   (caar data) = (id . 1)                    ; first cell of row
          ;;   (caaar data) = id                         ; column name symbol
          ;;
          ;; Binding example: ((orders . (((id . 1) ...))))
          ;;   (car data) = (orders . (((id . 1) ...)))  ; a binding
          ;;   (caar data) = orders                      ; binding name symbol
          ;;
          ;; Key distinction: for direct data, (caar data) is a CONS CELL (column . value)
          ;; For bindings, (caar data) is a SYMBOL (the binding name)
          ((and (listp data)
                (listp (car data))
                (consp (caar data)))  ; First element's car is cons = it's a row
           `((data . ,data)))

          ;; List of bindings: each should be (symbol . alist-data)
          (t
           (mapcar
            (lambda (binding)
              (pcase binding
                ;; Cons cell (sym . value) where value is list of alists
                (`(,(and sym (pred symbolp))
                   . ,(and value
                           (pred listp)
                           (pred (lambda (v)
                                   (and v
                                        (listp (car v))
                                        (consp (caar v)))))))
                 (cons sym value))
                (_
                 (error "Invalid data binding: %S.  Expected (symbol . alist-data); use backquote: \\=`((sym . ,var))" binding))))
            data)))))

    ;; Substitute each binding
    (pcase-dolist (`(,sym . ,alist-data) bindings)
      (let* ((pattern (format "@%s\\b" (regexp-quote (symbol-name sym))))
             (temp-file (make-temp-file
                         (format "duckdb-data-%s-" sym)
                         nil ".csv")))
        (duckdb-query--alist-to-csv-file alist-data temp-file)
        (puthash sym temp-file temp-files)
        (setq result (replace-regexp-in-string
                      pattern
                      (format "'%s'" temp-file)
                      result t t))))
    result))

;;;; Main Entry Point

(cl-defun duckdb-query (query &rest args &key
                              database
                              timeout
                              (format :alist)
                              (executor :cli)
                              data
                              &allow-other-keys)
  "Execute QUERY and return results in FORMAT.

QUERY is SQL string.  May contain @SYMBOL references when DATA provided.

DATABASE defaults to `duckdb-query-default-database'.
TIMEOUT defaults to `duckdb-query-default-timeout'.

FORMAT is output structure:
  :alist, :plist, :hash, :vector, :columnar, :org-table, :raw

EXECUTOR controls execution: :cli (default), function, or custom object.
See `duckdb-query-execute' for extension points.

DATA enables @SYMBOL references to Elisp data.  Use backquote for
lexical variables.  See `duckdb-query--substitute-data-refs' for
binding forms and examples.

Additional keyword arguments in ARGS are passed to EXECUTOR.

Returns converted results or nil for empty results.

Examples:

  (duckdb-query \"SELECT 42 as answer\")
  ;; => (((answer . 42)))

  (let ((users \\='(((id . 1) (name . \"Alice\")))))
    (duckdb-query \"SELECT * FROM @data\" :data users))
  ;; => (((id . 1) (name . \"Alice\")))

  (duckdb-query \"SELECT * FROM @orders\"
                :data \\=`((orders . ,my-orders)))"
  (let* ((temp-files (make-hash-table :test 'eq))
         (processed-query (if data
                              (duckdb-query--substitute-data-refs
                               query data temp-files)
                            query))
         (db (or database duckdb-query-default-database))
         (clean-args (cl-loop for (k v) on args by #'cddr
                              unless (eq k :data)
                              append (list k v)))
         (output nil))
    (unwind-protect
        (progn
          (setq output (apply #'duckdb-query-execute
                              executor
                              processed-query
                              :database db
                              :timeout timeout
                              clean-args))
          (when (and output (not (string-empty-p (string-trim output))))
            (pcase format
              (:raw
               output)
              (:alist
               (json-parse-string output
                                  :object-type 'alist
                                  :array-type 'list
                                  :null-object duckdb-query-null-value
                                  :false-object duckdb-query-false-value))
              (:plist
               (json-parse-string output
                                  :object-type 'plist
                                  :array-type 'list
                                  :null-object duckdb-query-null-value
                                  :false-object duckdb-query-false-value))
              (:hash
               (json-parse-string output
                                  :object-type 'hash-table
                                  :array-type 'list
                                  :null-object duckdb-query-null-value
                                  :false-object duckdb-query-false-value))
              (:vector
               (json-parse-string output
                                  :object-type 'alist
                                  :array-type 'array
                                  :null-object duckdb-query-null-value
                                  :false-object duckdb-query-false-value))
              (:columnar
               (duckdb-query--to-columnar
                (json-parse-string output
                                   :object-type 'alist
                                   :array-type 'list
                                   :null-object duckdb-query-null-value
                                   :false-object duckdb-query-false-value)))
              (:org-table
               (duckdb-query--to-org-table
                (json-parse-string output
                                   :object-type 'alist
                                   :array-type 'list
                                   :null-object duckdb-query-null-value
                                   :false-object duckdb-query-false-value)))
              (_
               (error "Unknown format: %s.  Valid: :alist :plist :hash :vector :columnar :org-table :raw"
                      format)))))
      ;; Cleanup temp files
      (maphash (lambda (_sym file)
                 (when (file-exists-p file)
                   (delete-file file)))
               temp-files))))

(cl-defun duckdb-query-file (file &key database readonly (format :alist))
  "Execute SQL from FILE and return results in FORMAT.

FILE is path to SQL file containing query.
DATABASE is optional database file path (nil for in-memory).
READONLY defaults to t when DATABASE specified.
FORMAT is output structure (:alist, :columnar, :org-table, etc.).

Return converted results in specified FORMAT.
Return nil for empty results.
Return raw output string for non-JSON results.

Use `duckdb-query' with SQL read from FILE.

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
