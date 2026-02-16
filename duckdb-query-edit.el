;;; duckdb-query-edit.el --- Editing utilities for duckdb-query forms -*- lexical-binding: t; -*-
;;
;; Author: Gino Cornejo <gggion123@gmail.com>
;; Maintainer: Gino Cornejo <gggion123@gmail.com>
;; Homepage: https://github.com/gggion/duckdb-query.el

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

;; Structural editing utilities for `duckdb-query' forms.
;;
;; Extract inline SQL text into parameterized references:
;;
;;     ;; Select a region inside a query string, then:
;;     M-x duckdb-query-edit-extract-to-ref
;;
;;     ;; Or bind specific types:
;;     (define-key my-map (kbd "C-c v") #'duckdb-query-edit-extract-to-val)
;;     (define-key my-map (kbd "C-c s") #'duckdb-query-edit-extract-to-sql)
;;
;; The primitives compose for programmatic use:
;;
;;     (duckdb-query-edit--insert-binding :val 'threshold "100")
;;     (duckdb-query-edit--replace-with-ref beg end :val "threshold")

;;; Code:

(require 'cl-lib)
(require 'duckdb-query-parse)

;;;; Reference Format Constants

(defconst duckdb-query-edit--ref-types '(:val :sql :data)
  "Reference types supported by extraction commands.

Does not include :org since org references resolve from buffer
context rather than inline bindings.")

(defconst duckdb-query-edit--ref-format-alist
  '((:val . "@val:%s")
    (:sql . "@sql:%s")
    (:data . "@data:%s"))
  "Alist mapping reference types to format strings for substitution.")

;;;; Internal: Locate Parameter

(defun duckdb-query-edit--find-param (form-beg form-end keyword)
  "Find KEYWORD parameter in form between FORM-BEG and FORM-END.

KEYWORD is a keyword symbol (e.g., :val, :sql, :data).

Return plist with :key-beg, :key-end, :val-beg, :val-end if found.
Return nil if KEYWORD is not present in the form.

Uses `duckdb-query--parse-params' for structural extraction."
  (let ((params (duckdb-query--parse-params form-beg form-end)))
    (cl-find-if (lambda (p) (eq (plist-get p :key) keyword)) params)))

;;;; Internal: Insert Binding

(defun duckdb-query-edit--insert-binding (form-beg form-end keyword name value)
  "Insert binding (NAME . VALUE) into KEYWORD parameter of enclosing form.

FORM-BEG and FORM-END delimit the `duckdb-query' form.
KEYWORD is :val, :sql, or :data.
NAME is binding name as string.
VALUE is binding value as string (Elisp literal representation).

If KEYWORD parameter exists, append (NAME . VALUE) to its alist.
If KEYWORD parameter does not exist, insert it before the closing
parenthesis of the form.

For :val, VALUE is an Elisp literal (e.g., \"\\\"path/to/file\\\"\").
For :sql, VALUE is a quoted string (e.g., \"\\\"SELECT * FROM t\\\"\").
For :data, VALUE is an Elisp data expression.

Return position after inserted text.

Caller is responsible for proper VALUE formatting.
Uses `duckdb-query-edit--find-param' for parameter location."
  (let ((param (duckdb-query-edit--find-param form-beg form-end keyword)))
    (if param
        (duckdb-query-edit--append-to-param param name value)
      (duckdb-query-edit--insert-new-param form-end keyword name value))))

(defun duckdb-query-edit--find-last-binding-end (val-beg val-end)
  "Find position after last binding entry in alist between VAL-BEG and VAL-END.

Navigate into the quoted alist structure and locate the closing
parenthesis of the last cons cell.  Return position after that
paren, or nil if structure is not recognized.

VAL-BEG is position of the quote or backquote before the alist.
VAL-END is position after the entire value form."
  (ignore val-end)
  (save-excursion
    (goto-char val-beg)
    ;; Skip quote/backquote
    (when (memq (char-after) '(?' ?`))
      (forward-char 1))
    ;; Now at opening paren of alist
    (when (eq (char-after) ?\()
      (let ((list-start (point))
            (list-end (save-excursion
                        (when (duckdb-query--forward-sexp-safe)
                          (point)))))
        (when list-end
          ;; Walk forward through the alist entries to find last one
          (goto-char list-start)
          (forward-char 1)
          (let ((last-entry-end nil))
            (while (< (point) (1- list-end))
              (duckdb-query--skip-whitespace-and-comments)
              ;; Skip unquote markers
              (when (eq (char-after) ?,)
                (forward-char 1)
                (when (eq (char-after) ?@)
                  (forward-char 1)))
              (duckdb-query--skip-whitespace-and-comments)
              (when (and (< (point) (1- list-end))
                         (duckdb-query--forward-sexp-safe))
                (setq last-entry-end (point))))
            last-entry-end))))))

(defun duckdb-query-edit--append-to-param (param name value)
  "Append binding (NAME . VALUE) to existing PARAM alist.

PARAM is plist from `duckdb-query-edit--find-param'.
NAME is binding name string.
VALUE is Elisp literal string.

Insert after the last binding entry in the alist.
Return position after inserted text."
  (let* ((val-beg (plist-get param :val-beg))
         (val-end (plist-get param :val-end))
         (last-end (duckdb-query-edit--find-last-binding-end val-beg val-end)))
    (unless last-end
      (user-error "Cannot parse parameter alist structure"))
    (save-excursion
      (goto-char last-end)
      (let ((indent (duckdb-query-edit--param-indent param)))
        (insert "\n" indent "(" name " . " value ")")
        (point)))))

(defun duckdb-query-edit--param-indent (param)
  "Compute indentation string for bindings inside PARAM.

PARAM is plist from `duckdb-query-edit--find-param'.
Return whitespace string matching the indentation of existing
bindings, or reasonable default based on parameter position."
  (save-excursion
    (goto-char (plist-get param :val-beg))
    ;; Skip quote/backquote and opening paren to find first binding
    (skip-chars-forward "'`(")
    (if (eq (char-after) ?\()
        ;; At first binding's opening paren
        (make-string (current-column) ?\s)
      ;; Fallback: indent relative to keyword
      (make-string (+ (save-excursion
                        (goto-char (plist-get param :key-beg))
                        (current-column))
                      1)
                   ?\s))))

(defun duckdb-query-edit--insert-new-param (form-end keyword name value)
  "Insert new KEYWORD parameter with binding (NAME . VALUE).

FORM-END is position after closing paren of form.
KEYWORD is :val, :sql, or :data.
NAME is binding name string.
VALUE is Elisp literal string.

Insert before the closing parenthesis of the `duckdb-query' form.
Return position after inserted text."
  (save-excursion
    (goto-char (1- form-end))
    (let* ((form-col (save-excursion
                       (backward-up-list 1)
                       (current-column)))
           (indent (make-string (+ form-col 14) ?\s)))
      (insert "\n" indent (symbol-name keyword)
              " '((" name " . " value "))")
      (point))))


(provide 'duckdb-query-edit)

;;;; Internal: Replace with Reference

(defun duckdb-query-edit--replace-with-ref (beg end ref-type name)
  "Replace text between BEG and END with @REF-TYPE:NAME reference.

BEG and END delimit the text to replace (inside a string).
REF-TYPE is :val, :sql, or :data.
NAME is reference name string.

Return position after inserted reference."
  (let ((ref-string (format (cdr (assq ref-type
                                       duckdb-query-edit--ref-format-alist))
                            name)))
    (save-excursion
      (goto-char beg)
      (delete-region beg end)
      (insert ref-string)
      (point))))

;;;; Internal: Value Formatting

(defun duckdb-query-edit--format-value (text ref-type)
  "Format extracted TEXT as Elisp literal for REF-TYPE binding.

TEXT is the raw string extracted from the SQL query.
REF-TYPE is :val, :sql, or :data.

For :val, strip surrounding SQL single-quotes if present and
produce an Elisp string literal.  Numeric strings are left as
numbers.

For :sql, wrap in double quotes as Elisp string literal.

For :data, return TEXT as-is (caller provides Elisp expression).

Return formatted string suitable for insertion as binding value."
  (pcase ref-type
    (:val
     (let ((stripped (if (and (>= (length text) 2)
                              (eq (aref text 0) ?')
                              (eq (aref text (1- (length text))) ?'))
                         (substring text 1 -1)
                       text)))
       (cond
        ;; Integer
        ((string-match-p "\\`-?[0-9]+\\'" stripped)
         stripped)
        ;; Float
        ((string-match-p "\\`-?[0-9]*\\.[0-9]+\\'" stripped)
         stripped)
        ;; String
        (t
         (format "%S" stripped)))))
    (:sql
     (format "%S" text))
    (:data
     text)))
;;;; Internal: Detect Reference at Point

(defun duckdb-query-edit--ref-at-point ()
  "Detect @type:name reference at or around point.

Return plist with :type, :name, :beg, :end if point is on a
reference, or nil otherwise.

Searches backward from point for @ character, then validates
the reference pattern forward.  Handles cursor positioned
anywhere within the @type:name text."
  (save-excursion
    (let ((orig (point))
          (str-start (duckdb-query--in-string-p)))
      (when str-start
        ;; Search backward for @ within current string
        (let ((search-start (max str-start (- orig 30))))
          (goto-char orig)
          (while (and (>= (point) search-start)
                      (not (eq (char-after) ?@)))
            (backward-char 1))
          (when (and (eq (char-after) ?@)
                     (looking-at "@\\(sql\\|data\\|val\\|org\\):\\([a-zA-Z_][a-zA-Z0-9_]*\\)"))
            (let ((ref-end (match-end 0)))
              ;; Verify original point was within the match
              (when (and (<= (match-beginning 0) orig)
                         (<= orig ref-end))
                (list :type (intern (concat ":" (match-string 1)))
                      :name (match-string 2)
                      :beg (match-beginning 0)
                      :end ref-end)))))))))

;;;; Internal: Extract Binding Value Text

(defun duckdb-query-edit--extract-binding-value (form-beg form-end keyword name)
  "Extract the value text of binding NAME from KEYWORD parameter.

FORM-BEG and FORM-END delimit the `duckdb-query' form.
KEYWORD is :val, :sql, or :data.
NAME is binding name as string.

Walk the parameter alist structure to find the cons cell
with NAME as car, then extract the cdr text.

Return the value as a string (the Elisp literal as written in
the buffer), or nil if binding not found.

For :val with string value like \"\\\"hello\\\"\", returns the
inner string content (without surrounding double quotes).
For :val with numeric value like \"42\", returns \"42\".
For :sql with value like \"\\\"SELECT ...\\\"\", returns the inner
SQL text without surrounding double quotes."
  (let ((param (duckdb-query-edit--find-param form-beg form-end keyword)))
    (when param
      (save-excursion
        (goto-char (plist-get param :val-beg))
        ;; Skip quote/backquote
        (when (memq (char-after) '(?' ?`))
          (forward-char 1))
        ;; Enter the alist
        (when (eq (char-after) ?\()
          (let ((list-end (save-excursion
                            (when (duckdb-query--forward-sexp-safe)
                              (1- (point))))))
            (when list-end
              (forward-char 1)
              (catch 'found
                (while (< (point) list-end)
                  (duckdb-query--skip-whitespace-and-comments)
                  ;; Skip unquote markers
                  (when (eq (char-after) ?,)
                    (forward-char 1)
                    (when (eq (char-after) ?@)
                      (forward-char 1)))
                  (duckdb-query--skip-whitespace-and-comments)
                  (when (and (< (point) list-end)
                             (eq (char-after) ?\())
                    (let ((entry-start (point)))
                      (forward-char 1)
                      (duckdb-query--skip-whitespace-and-comments)
                      (when (looking-at (regexp-quote name))
                        (goto-char (match-end 0))
                        (duckdb-query--skip-whitespace-and-comments)
                        (when (eq (char-after) ?\.)
                          (forward-char 1)
                          (duckdb-query--skip-whitespace-and-comments)
                          ;; Now at the value
                          (let ((val-start (point)))
                            (goto-char entry-start)
                            (duckdb-query--forward-sexp-safe)
                            ;; Back up past closing paren of entry
                            (let ((val-text (string-trim
                                            (buffer-substring-no-properties
                                             val-start (1- (point))))))
                              (throw 'found val-text)))))
                      ;; Not this entry, skip it
                      (goto-char entry-start)
                      (unless (duckdb-query--forward-sexp-safe)
                        (forward-char 1)))))))))))))

(defun duckdb-query-edit--strip-elisp-string-quotes (text)
  "Strip surrounding double quotes and unescape from Elisp string literal TEXT.

Return inner content if TEXT is a quoted string literal.
Return TEXT unchanged if not double-quoted.

Used by `duckdb-query-edit--unquote-value' for both :sql and :val
branches."
  (if (and (>= (length text) 2)
           (eq (aref text 0) ?\")
           (eq (aref text (1- (length text))) ?\"))
      (replace-regexp-in-string
       "\\\\\""  "\""
       (substring text 1 -1))
    text))

(defun duckdb-query-edit--unquote-value (text ref-type)
  "Convert binding value TEXT to inline form for REF-TYPE.

TEXT is the raw Elisp literal as extracted from the buffer.

For :sql, strip surrounding double quotes to get raw SQL.
For :val, strip surrounding double quotes for strings, or
return numeric text as-is.
For :data, return as-is.

Return string suitable for insertion into a SQL string.

Uses `duckdb-query-edit--strip-elisp-string-quotes' for
double-quote stripping."
  (pcase ref-type
    ((or :sql :val) (duckdb-query-edit--strip-elisp-string-quotes text))
    (_ text)))

;;;; Internal: Remove Binding

(defun duckdb-query-edit--remove-binding (form-beg form-end keyword name)
  "Remove binding NAME from KEYWORD parameter alist.

FORM-BEG and FORM-END delimit the `duckdb-query' form.
KEYWORD is :val, :sql, or :data.
NAME is binding name as string.

If this is the last binding in the parameter, remove the entire
parameter (keyword and value) from the form.

If other bindings remain, remove only the (NAME . VALUE) entry.

Return non-nil if removal succeeded."
  (let ((param (duckdb-query-edit--find-param form-beg form-end keyword)))
    (when param
      (save-excursion
        (goto-char (plist-get param :val-beg))
        ;; Skip quote/backquote
        (when (memq (char-after) '(?' ?`))
          (forward-char 1))
        (when (eq (char-after) ?\()
          (let* ((list-start (point))
                 (list-end (save-excursion
                             (when (duckdb-query--forward-sexp-safe)
                               (point))))
                 (entries nil))
            (when list-end
              ;; Collect all entries with their positions
              (goto-char list-start)
              (forward-char 1)
              (while (< (point) (1- list-end))
                (duckdb-query--skip-whitespace-and-comments)
                (when (eq (char-after) ?,)
                  (forward-char 1)
                  (when (eq (char-after) ?@)
                    (forward-char 1)))
                (duckdb-query--skip-whitespace-and-comments)
                (when (and (< (point) (1- list-end))
                           (eq (char-after) ?\())
                  (let ((entry-beg (point))
                        (entry-name nil))
                    (save-excursion
                      (forward-char 1)
                      (duckdb-query--skip-whitespace-and-comments)
                      (when (looking-at "\\([a-zA-Z_][a-zA-Z0-9_-]*\\)")
                        (setq entry-name (match-string 1))))
                    (when (duckdb-query--forward-sexp-safe)
                      (push (list :name entry-name
                                  :beg entry-beg
                                  :end (point))
                            entries)))))
              (setq entries (nreverse entries))
              ;; Find target entry
              (let ((target (cl-find-if
                             (lambda (e) (equal (plist-get e :name) name))
                             entries)))
                (when target
                  (if (= (length entries) 1)
                      ;; Last binding: remove entire parameter
                      (duckdb-query-edit--remove-param param)
                    ;; Remove just the entry and surrounding whitespace
                    (duckdb-query-edit--remove-entry target entries))
                  t)))))))))

(defun duckdb-query-edit--remove-param (param)
  "Remove entire parameter PARAM (keyword and value) from form.

PARAM is plist from `duckdb-query-edit--find-param'.
Delete from keyword start to value end, including leading
whitespace on the same line."
  (let ((key-beg (plist-get param :key-beg))
        (val-end (plist-get param :val-end)))
    ;; Extend backward to consume leading whitespace and newline
    (save-excursion
      (goto-char key-beg)
      (let ((line-start (line-beginning-position)))
        (skip-chars-backward " \t" line-start)
        (when (eq (char-before) ?\n)
          (setq key-beg (1- (point)))))
      (delete-region key-beg val-end))))

(defun duckdb-query-edit--remove-entry (target entries)
  "Remove TARGET entry from among ENTRIES in parameter alist.

TARGET is plist with :name, :beg, :end.
ENTRIES is list of all entry plists in order.

Remove the entry and surrounding whitespace/newline."
  (let ((entry-beg (plist-get target :beg))
        (entry-end (plist-get target :end)))
    (save-excursion
      ;; Extend to consume surrounding whitespace and newline
      (goto-char entry-beg)
      (let ((line-start (line-beginning-position)))
        (skip-chars-backward " \t" line-start)
        (when (or (eq (char-before) ?\n)
                  (bolp))
          (setq entry-beg (if (eq (char-before) ?\n)
                              (1- (point))
                            (point)))))
      ;; Also consume trailing whitespace up to newline
      (goto-char entry-end)
      (skip-chars-forward " \t")
      (when (eq (char-after) ?\n)
        (setq entry-end (1+ (point))))
      (delete-region entry-beg entry-end))))

;;;; Internal: Reference Count Check

(defun duckdb-query-edit--count-refs (form-beg form-end ref-type name)
  "Count occurrences of @REF-TYPE:NAME in form between FORM-BEG and FORM-END.

REF-TYPE is :val, :sql, or :data.
NAME is reference name string.

Search all string literals in the form for matching references.

Return integer count."
  (let* ((type-str (substring (symbol-name ref-type) 1))
         (pattern (format "@%s:%s\\b" (regexp-quote type-str)
                          (regexp-quote name)))
         (strings (duckdb-query--collect-strings-in-region form-beg form-end))
         (count 0))
    (save-excursion
      (dolist (bounds strings)
        (goto-char (1+ (car bounds)))
        (let ((str-end (1- (cdr bounds))))
          (while (re-search-forward pattern str-end t)
            (cl-incf count)))))
    count))
;;;; Interactive: Extract to Reference

(defun duckdb-query-edit--require-region-in-string ()
  "Validate preconditions for extraction commands.

Signal `user-error' if point is not inside a `duckdb-query' form,
no region is active, or region is not inside a string.

Called by `interactive' specs of extraction commands."
  (unless (duckdb-query--find-enclosing-form)
    (user-error "Not inside a duckdb-query form"))
  (unless (use-region-p)
    (user-error "No active region; select text to extract"))
  (unless (duckdb-query--in-string-p)
    (user-error "Region must be inside a string")))

(defun duckdb-query-edit--read-binding-name (type-str)
  "Prompt for binding name with TYPE-STR prefix.

Signal `user-error' if name is empty.
Return name string."
  (let ((name (read-string (format "@%s: name: " type-str))))
    (when (string-empty-p name)
      (user-error "Binding name cannot be empty"))
    name))

;;;###autoload
(defun duckdb-query-edit-extract-to-ref (beg end ref-type name)
  "Extract region BEG..END into REF-TYPE parameter with NAME.

Replace selected text with @REF-TYPE:NAME reference and insert
corresponding binding into the parameter alist.

When called interactively with active region inside a `duckdb-query'
SQL string, prompt for REF-TYPE and NAME.

REF-TYPE is :val, :sql, or :data.
NAME is binding name string.

The extracted text is formatted appropriately for the target
parameter type:

  :val - SQL quotes stripped, stored as Elisp literal
  :sql - Stored as Elisp string
  :data - Stored as-is (for manual editing)

Uses markers to track region positions across buffer modifications,
ensuring correct replacement regardless of where the binding is
inserted relative to the region.

Example workflow:

  ;; Given:
  (duckdb-query \"SELECT * FROM \\='./data/users.csv\\='\")

  ;; Select \\='./data/users.csv\\=', invoke with :val and \"csv_path\":
  (duckdb-query \"SELECT * FROM @val:csv_path\"
                :val \\='((csv_path . \"./data/users.csv\")))

Uses `duckdb-query-edit--insert-binding' and
`duckdb-query-edit--replace-with-ref'.
Also see `duckdb-query-edit-extract-to-val' for pre-typed variant.
Also see `duckdb-query-edit-extract-to-sql' for pre-typed variant."
  (interactive
   (progn
     (duckdb-query-edit--require-region-in-string)
     (let* ((type-str (completing-read
                       "Reference type: "
                       '("val" "sql" "data")
                       nil t))
            (ref-type (intern (concat ":" type-str)))
            (name (duckdb-query-edit--read-binding-name type-str)))
       (list (region-beginning) (region-end) ref-type name))))
  (let ((form-bounds (duckdb-query--find-enclosing-form)))
    (unless form-bounds
      (user-error "Not inside a duckdb-query form"))
    (let* ((text (buffer-substring-no-properties beg end))
           (value (duckdb-query-edit--format-value text ref-type))
           ;; Use markers to survive buffer modifications
           (beg-marker (copy-marker beg))
           (end-marker (copy-marker end))
           (form-beg-marker (copy-marker (car form-bounds)))
           (form-end-marker (copy-marker (cdr form-bounds))))
      (unwind-protect
          (progn
            ;; Insert binding first.  Markers track region positions
            ;; automatically regardless of insertion location.
            (duckdb-query-edit--insert-binding
             (marker-position form-beg-marker)
             (marker-position form-end-marker)
             ref-type name value)
            ;; Replace original text with reference.  Markers have
            ;; adjusted for any text inserted before the region.
            (duckdb-query-edit--replace-with-ref
             (marker-position beg-marker)
             (marker-position end-marker)
             ref-type name)
            ;; Re-indent the form
            (indent-region (marker-position form-beg-marker)
                           (save-excursion
                             (goto-char (marker-position form-beg-marker))
                             (forward-sexp 1)
                             (point))))
        ;; Clean up markers
        (set-marker beg-marker nil)
        (set-marker end-marker nil)
        (set-marker form-beg-marker nil)
        (set-marker form-end-marker nil)))))

;;;###autoload
(defun duckdb-query-edit-extract-to-val (beg end name)
  "Extract region BEG..END into :val parameter with NAME.

Convenience wrapper around `duckdb-query-edit-extract-to-ref' with
REF-TYPE pre-set to :val.

When called interactively, prompt only for NAME.

Also see `duckdb-query-edit-extract-to-ref' for full control.
Also see `duckdb-query-edit-extract-to-sql' for SQL fragments."
  (interactive
   (progn
     (duckdb-query-edit--require-region-in-string)
     (let ((name (duckdb-query-edit--read-binding-name "val")))
       (list (region-beginning) (region-end) name))))
  (duckdb-query-edit-extract-to-ref beg end :val name))

;;;###autoload
(defun duckdb-query-edit-extract-to-sql (beg end name)
  "Extract region BEG..END into :sql parameter with NAME.

Convenience wrapper around `duckdb-query-edit-extract-to-ref' with
REF-TYPE pre-set to :sql.

When called interactively, prompt only for NAME.

Also see `duckdb-query-edit-extract-to-ref' for full control.
Also see `duckdb-query-edit-extract-to-val' for literal values."
  (interactive
   (progn
     (duckdb-query-edit--require-region-in-string)
     (let ((name (duckdb-query-edit--read-binding-name "sql")))
       (list (region-beginning) (region-end) name))))
  (duckdb-query-edit-extract-to-ref beg end :sql name))

;;;###autoload
(defun duckdb-query-edit-extract-to-data (beg end name)
  "Extract region BEG..END into :data parameter with NAME.

Convenience wrapper around `duckdb-query-edit-extract-to-ref' with
REF-TYPE pre-set to :data.

When called interactively, prompt only for NAME.

The extracted text is inserted as-is into the :data binding.
Edit the binding value manually to provide the Elisp data
expression.

Also see `duckdb-query-edit-extract-to-ref' for full control.
Also see `duckdb-query-edit-extract-to-val' for literal values."
  (interactive
   (progn
     (duckdb-query-edit--require-region-in-string)
     (let ((name (duckdb-query-edit--read-binding-name "data")))
       (list (region-beginning) (region-end) name))))
  (duckdb-query-edit-extract-to-ref beg end :data name))

;;;; Interactive: Inline Reference

;;;###autoload
(defun duckdb-query-edit-inline-ref ()
  "Replace @type:name reference at point with its binding value.

Look up the binding value from the corresponding parameter alist
and replace the reference text with the value.

For :sql bindings, the SQL fragment text is inserted directly.
For :val bindings, the Elisp value is unquoted for insertion.

Does not remove the binding entry.  Orphaned bindings are visible
via `duckdb-query-font-lock-mode' and can be removed manually or
with `duckdb-query-edit-remove-binding'.

Inverse of `duckdb-query-edit-extract-to-ref'.

Also see `duckdb-query-edit-extract-to-val'.
Also see `duckdb-query-edit-extract-to-sql'."
  (interactive)
  (let ((ref (duckdb-query-edit--ref-at-point)))
    (unless ref
      (user-error "No @type:name reference at point"))
    (let* ((ref-type (plist-get ref :type))
           (ref-name (plist-get ref :name))
           (ref-beg (plist-get ref :beg))
           (ref-end (plist-get ref :end))
           (form-bounds (duckdb-query--find-enclosing-form)))
      (unless form-bounds
        (user-error "Not inside a duckdb-query form"))
      (when (eq ref-type :org)
        (user-error "@org: references resolve from buffer context, not parameters"))
      (let* ((form-beg (car form-bounds))
             (form-end (cdr form-bounds))
             (raw-value (duckdb-query-edit--extract-binding-value
                         form-beg form-end ref-type ref-name)))
        (unless raw-value
          (user-error "No binding for %s in %s parameter"
                      ref-name (symbol-name ref-type)))
        (let* ((inline-text (duckdb-query-edit--unquote-value raw-value ref-type))
               (form-beg-marker (copy-marker form-beg)))
          (unwind-protect
              (progn
                (goto-char ref-beg)
                (delete-region ref-beg ref-end)
                (insert inline-text)
                ;; Re-indent
                (indent-region (marker-position form-beg-marker)
                               (save-excursion
                                 (goto-char (marker-position form-beg-marker))
                                 (forward-sexp 1)
                                 (point))))
            (set-marker form-beg-marker nil)))))))
;;; duckdb-query-edit.el ends here
