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

;;;; Value Formatting

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
;;; duckdb-query-edit.el ends here
