;;; duckdb-query-parse.el --- Parser for duckdb-query forms -*- lexical-binding: t; -*-
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

;; Structural parser for `duckdb-query' forms in Elisp buffers.
;;
;; Extracts form bounds, SQL strings, keyword parameters, binding
;; definitions, and reference locations for use by fontification,
;; linting, and completion systems.
;;
;; Basic usage:
;;
;;     (duckdb-query--parse-at-point)
;;     ;; => #s(duckdb-query-parse-result ...)
;;
;;     (duckdb-query-parse-buffer)
;;     ;; => list of parse results for all forms
;;
;; The parser handles comments, quoted and backquoted forms, and
;; validates references against declared bindings.

;;; Code:

(require 'cl-lib)

;;;; Character Constants

(defconst duckdb-query--char-lparen ?\(
  "Left parenthesis character.")

(defconst duckdb-query--char-rparen ?\)
  "Right parenthesis character.")

(defconst duckdb-query--char-quote ?\'
  "Quote character.")

(defconst duckdb-query--char-backquote ?\`
  "Backquote character.")

(defconst duckdb-query--char-comma ?\,
  "Comma (unquote) character.")

(defconst duckdb-query--char-at ?\@
  "At-sign character, used as splice marker after comma.")

(defconst duckdb-query--char-dot ?\.
  "Dot character, used as cons pair separator.")

(defconst duckdb-query--char-colon ?\:
  "Colon character, used as keyword prefix.")

(defconst duckdb-query--char-double-quote ?\"
  "Double-quote character, used as string delimiter.")

(defconst duckdb-query--char-semicolon ?\;
  "Semicolon character, used as comment prefix.")

;;;; Core Position Utilities

(defsubst duckdb-query--in-string-p ()
  "Return start of string if point is inside one, nil otherwise.
Uses `syntax-ppss' to determine parse state."
  (nth 8 (syntax-ppss)))

(defsubst duckdb-query--in-comment-p ()
  "Return non-nil if point is inside a comment.
Uses `syntax-ppss' to determine parse state."
  (nth 4 (syntax-ppss)))

(defsubst duckdb-query--in-string-or-comment-p ()
  "Return start position if point is in string or comment, nil otherwise.
For strings, returns string start position.
For comments, returns current point."
  (let ((state (syntax-ppss)))
    (or (nth 8 state)
        (and (nth 4 state) (point)))))

(defun duckdb-query--skip-whitespace-and-comments ()
  "Skip forward over whitespace and comments.
Handles both inline comments and comment-only lines."
  (while (or (looking-at-p "[ \t\n]")
             (duckdb-query--in-comment-p)
             (eq (char-after) duckdb-query--char-semicolon))
    (if (or (duckdb-query--in-comment-p)
            (eq (char-after) duckdb-query--char-semicolon))
        (forward-line 1)
      (skip-chars-forward " \t\n"))))

(defun duckdb-query--forward-sexp-safe ()
  "Move forward one sexp.
Return end position on success, nil on scan error."
  (condition-case nil
      (progn (forward-sexp 1) (point))
    (scan-error nil)))

(defun duckdb-query--backward-up-list-safe ()
  "Move backward up one list level.
Return new position on success, nil on scan error."
  (condition-case nil
      (progn (backward-up-list 1) (point))
    (scan-error nil)))

;;;; Form Location

(defun duckdb-query--find-enclosing-form ()
  "Find enclosing `duckdb-query' form containing point.
Return (BEG . END) cons cell with form bounds, or nil if point
is not inside a `duckdb-query' form.

Checks if point is directly on a `duckdb-query' form first,
then walks up the list structure checking each enclosing list."
  (save-excursion
    (when-let ((str-start (duckdb-query--in-string-p)))
      (goto-char str-start))
    (catch 'found
      ;; Check if directly on a duckdb-query form
      (when (looking-at "(duckdb-query\\_>")
        (let ((beg (point)))
          (when (duckdb-query--forward-sexp-safe)
            (throw 'found (cons beg (point))))))
      ;; Walk up list structure
      (while (duckdb-query--backward-up-list-safe)
        (when (looking-at "(duckdb-query\\_>")
          (let ((beg (point)))
            (when (duckdb-query--forward-sexp-safe)
              (throw 'found (cons beg (point)))))))
      nil)))

;;;; String Extraction

(defun duckdb-query--string-bounds-at (pos)
  "Return bounds of string at POS as (BEG . END), or nil.
POS must be inside a string for bounds to be returned."
  (save-excursion
    (goto-char pos)
    (let ((state (syntax-ppss)))
      (when (nth 3 state)
        (let ((beg (nth 8 state)))
          (goto-char beg)
          (when (duckdb-query--forward-sexp-safe)
            (cons beg (point))))))))

(defun duckdb-query--collect-strings-in-region (beg end)
  "Collect all string literal bounds between BEG and END.
Return list of (START . END) cons cells for each string.
Skips strings inside comments."
  (save-excursion
    (goto-char beg)
    (let (strings)
      (while (and (< (point) end)
                  (search-forward "\"" end t))
        (let ((quote-pos (1- (point))))
          (unless (duckdb-query--in-comment-p)
            (goto-char quote-pos)
            (let ((state (syntax-ppss)))
              (if (nth 3 state)
                  (let ((str-beg (nth 8 state)))
                    (goto-char str-beg)
                    (when (duckdb-query--forward-sexp-safe)
                      (push (cons str-beg (point)) strings)))
                (when (duckdb-query--forward-sexp-safe)
                  (push (cons quote-pos (point)) strings)))))))
      (nreverse strings))))

;;;; Reference Pattern Matching

(defconst duckdb-query--reference-regexp
  "@\\(sql\\|data\\|val\\|org\\):\\([a-zA-Z_][a-zA-Z0-9_]*\\)"
  "Regexp matching @type:name reference patterns.
Group 1 captures the reference type (sql, data, val, org).
Group 2 captures the reference name.")

(defun duckdb-query--find-references-in-string (str-beg str-end)
  "Find all references in string between STR-BEG and STR-END.
Return list of plists, each with keys :type, :name, :beg, :end.
STR-BEG and STR-END should include the quote characters."
  (save-excursion
    (goto-char (1+ str-beg))
    (let ((content-end (1- str-end))
          refs)
      (while (re-search-forward duckdb-query--reference-regexp content-end t)
        (push (list :type (intern (match-string 1))
                    :name (intern (match-string 2))
                    :beg (match-beginning 0)
                    :end (match-end 0))
              refs))
      (nreverse refs))))

;;;; Keyword Parameter Extraction

(defconst duckdb-query--param-keywords
  '(:sql :data :val :format :database :timeout :executor :output-via
    :data-format :preserve-nested)
  "Known `duckdb-query' keyword parameters.")

(defconst duckdb-query--binding-keywords '(:sql :data :val)
  "Parameters that define reference bindings.
References of type @sql:, @data:, and @val: must have corresponding
bindings in these parameters.")

(defun duckdb-query--parse-params (form-beg form-end)
  "Parse keyword parameters in `duckdb-query' form.
FORM-BEG is the buffer position of the opening parenthesis.
FORM-END is the buffer position after the closing parenthesis.

Skip the function symbol and main SQL string, then collect all
keyword-value pairs. Handle quoted and backquoted value forms.

Return list of plists, each with keys:
  :key     - keyword symbol (e.g., :sql, :data, :val)
  :key-beg - buffer position of keyword start
  :key-end - buffer position after keyword
  :val-beg - buffer position of value start (including quote)
  :val-end - buffer position after value

Called by `duckdb-query--parse-at-point'."
  (save-excursion
    (goto-char form-beg)
    (forward-char 1)
    (duckdb-query--forward-sexp-safe)
    (duckdb-query--skip-whitespace-and-comments)
    (when (eq (char-after) duckdb-query--char-double-quote)
      (duckdb-query--forward-sexp-safe))
    (let (params)
      (while (< (point) form-end)
        (duckdb-query--skip-whitespace-and-comments)
        (when (>= (point) form-end)
          (cl-return))
        (cond
         ((eq (char-after) duckdb-query--char-colon)
          (let ((key-beg (point)))
            (if (looking-at ":\\([-a-zA-Z0-9]+\\)")
                (let ((key (intern (match-string 0)))
                      (key-end (match-end 0)))
                  (goto-char key-end)
                  (duckdb-query--skip-whitespace-and-comments)
                  (when (< (point) form-end)
                    (let ((val-beg (point)))
                      (when (memq (char-after) (list duckdb-query--char-quote
                                                     duckdb-query--char-backquote))
                        (forward-char 1))
                      (if (duckdb-query--forward-sexp-safe)
                          (push (list :key key
                                      :key-beg key-beg
                                      :key-end key-end
                                      :val-beg val-beg
                                      :val-end (point))
                                params)
                        (goto-char key-end)))))
              (forward-char 1))))
         (t
          (unless (duckdb-query--forward-sexp-safe)
            (forward-char 1)))))
      (nreverse params))))

;;;; Binding Extraction

(defun duckdb-query--extract-binding-names (val-beg val-end)
  "Extract binding names from parameter value.
VAL-BEG and VAL-END delimit the parameter value to parse.

Handle quoted alists like ((name . value) ...) and backquoted
forms like `((name . ,expr) ...).

Skip nested forms like (sql ...) that are values, not bindings.
A valid binding is a cons pair with symbol as car and dot separator.

Return list of binding name symbols."
  (save-excursion
    (goto-char val-beg)
    (duckdb-query--skip-whitespace-and-comments)
    (when (memq (char-after) (list duckdb-query--char-quote
                                   duckdb-query--char-backquote))
      (forward-char 1))
    (let (names)
      (when (eq (char-after) duckdb-query--char-lparen)
        (let ((list-end (save-excursion
                          (when (duckdb-query--forward-sexp-safe)
                            (1- (point))))))
          (when (and list-end (< list-end val-end))
            (forward-char 1)
            (while (< (point) list-end)
              (duckdb-query--skip-whitespace-and-comments)
              (when (eq (char-after) duckdb-query--char-comma)
                (forward-char 1)
                (when (eq (char-after) duckdb-query--char-at)
                  (forward-char 1)))
              (duckdb-query--skip-whitespace-and-comments)
              (when (and (< (point) list-end)
                         (eq (char-after) duckdb-query--char-lparen))
                (let ((pair-start (point)))
                  (forward-char 1)
                  (duckdb-query--skip-whitespace-and-comments)
                  (when (looking-at "\\([a-zA-Z_][a-zA-Z0-9_-]*\\)")
                    (let ((maybe-name (intern (match-string 1))))
                      (goto-char (match-end 0))
                      (duckdb-query--skip-whitespace-and-comments)
                      (when (eq (char-after) duckdb-query--char-dot)
                        (push maybe-name names))))
                  (goto-char pair-start)))
              (unless (duckdb-query--forward-sexp-safe)
                (forward-char 1))))))
      (nreverse names))))

;;;; Parse Result Structure

(cl-defstruct (duckdb-query-parse-result
               (:constructor duckdb-query-parse-result--create)
               (:copier nil))
  "Result of parsing a `duckdb-query' form.

Slots:
  form-beg   - Start position of entire form.
  form-end   - End position of entire form.
  sql-beg    - Start position of main SQL string, or nil.
  sql-end    - End position of main SQL string, or nil.
  params     - List of parameter plists with positional info.
  bindings   - Alist mapping parameter keywords to binding names.
  references - List of reference plists with type, name, position, context."
  (form-beg nil :documentation "Start of duckdb-query form.")
  (form-end nil :documentation "End of duckdb-query form.")
  (sql-beg nil :documentation "Start of main SQL string.")
  (sql-end nil :documentation "End of main SQL string.")
  (params nil :documentation "List of parameter plists.")
  (bindings nil :documentation "Alist of (TYPE . (NAME ...)) for defined bindings.")
  (references nil :documentation "List of reference plists with :type :name :beg :end :context."))

;;;; Complete Form Parser

(defun duckdb-query--parse-at-point ()
  "Parse `duckdb-query' form at point.
Return `duckdb-query-parse-result' struct containing form bounds,
SQL string bounds, parameters, bindings, and references.

Return nil if point is not inside a `duckdb-query' form.

Parse extracts:
- Form boundaries for overlay/fontification scope
- Main SQL string position for reference detection
- Keyword parameters with their positions
- Binding names from :sql, :data, :val parameters
- All @type:name references with their contexts"
  (when-let ((form-bounds (duckdb-query--find-enclosing-form)))
    (let* ((form-beg (car form-bounds))
           (form-end (cdr form-bounds))
           sql-beg sql-end
           params bindings all-refs)
      (save-excursion
        (goto-char form-beg)
        (forward-char 1)
        (duckdb-query--forward-sexp-safe)
        (duckdb-query--skip-whitespace-and-comments)
        (when (eq (char-after) duckdb-query--char-double-quote)
          (setq sql-beg (point))
          (duckdb-query--forward-sexp-safe)
          (setq sql-end (point))))
      (setq params (duckdb-query--parse-params form-beg form-end))
      (dolist (param params)
        (let ((key (plist-get param :key)))
          (when (memq key duckdb-query--binding-keywords)
            (let ((names (duckdb-query--extract-binding-names
                          (plist-get param :val-beg)
                          (plist-get param :val-end))))
              (when names
                (push (cons key names) bindings))))))
      (when (and sql-beg sql-end)
        (dolist (ref (duckdb-query--find-references-in-string sql-beg sql-end))
          (push (append ref (list :context :main-sql)) all-refs)))
      (dolist (param params)
        (let ((key (plist-get param :key))
              (val-beg (plist-get param :val-beg))
              (val-end (plist-get param :val-end)))
          (dolist (str-bounds (duckdb-query--collect-strings-in-region val-beg val-end))
            (dolist (ref (duckdb-query--find-references-in-string
                          (car str-bounds) (cdr str-bounds)))
              (push (append ref (list :context key)) all-refs)))))
      (duckdb-query-parse-result--create
       :form-beg form-beg
       :form-end form-end
       :sql-beg sql-beg
       :sql-end sql-end
       :params params
       :bindings (nreverse bindings)
       :references (nreverse all-refs)))))

;;;; Validation

(defun duckdb-query--validate-reference (ref bindings)
  "Validate REF against BINDINGS.
REF is a reference plist with :type, :name, :context.
BINDINGS is an alist mapping keywords to lists of bound names.

Return nil if reference is valid.
Return error keyword if invalid:
  :invalid-sql-in-val - @sql: reference inside :val parameter
  :invalid-org-in-val - @org: reference inside :val parameter
  :undefined          - reference name not found in bindings"
  (let ((type (plist-get ref :type))
        (name (plist-get ref :name))
        (context (plist-get ref :context)))
    (cond
     ((and (eq type 'sql) (eq context :val))
      :invalid-sql-in-val)
     ((and (eq type 'org) (eq context :val))
      :invalid-org-in-val)
     ((not (memq name (cdr (assq (intern (format ":%s" type)) bindings))))
      (if (and (eq type 'data)
               (memq name (cdr (assq :data bindings))))
          nil
        :undefined))
     (t nil))))

(defun duckdb-query--validate-all-references (parse-result)
  "Validate all references in PARSE-RESULT.
PARSE-RESULT is a `duckdb-query-parse-result' struct.

Return list of (REF . ERROR-TYPE) cons cells for invalid references.
Return nil if all references are valid."
  (let ((bindings (duckdb-query-parse-result-bindings parse-result))
        invalid)
    (dolist (ref (duckdb-query-parse-result-references parse-result))
      (when-let ((err (duckdb-query--validate-reference ref bindings)))
        (push (cons ref err) invalid)))
    (nreverse invalid)))

;;;; Buffer-Wide Parsing

(defun duckdb-query-parse-buffer ()
  "Parse all `duckdb-query' forms in current buffer.
Return list of `duckdb-query-parse-result' structs, one per form.

Skip forms that appear inside strings or comments."
  (save-excursion
    (goto-char (point-min))
    (let (results)
      (while (re-search-forward "(duckdb-query\\_>" nil t)
        (goto-char (match-beginning 0))
        (unless (duckdb-query--in-string-or-comment-p)
          (when-let ((result (duckdb-query--parse-at-point)))
            (push result results)
            (goto-char (duckdb-query-parse-result-form-end result))))
        (unless (eobp)
          (forward-char 1)))
      (nreverse results))))

(defun duckdb-query-parse-buffer-with-validation ()
  "Parse and validate all `duckdb-query' forms in current buffer.
Return list of (PARSE-RESULT . INVALID-REFS) cons cells.

PARSE-RESULT is a `duckdb-query-parse-result' struct.
INVALID-REFS is a list of (REF . ERROR-TYPE) for invalid references,
or nil if all references in that form are valid."
  (mapcar (lambda (result)
            (cons result (duckdb-query--validate-all-references result)))
          (duckdb-query-parse-buffer)))

;;; duckdb-query-parse.el ends here
