;;; duckdb-query-font-lock.el --- Font-lock for duckdb-query SQL strings -*- lexical-binding: t; -*-

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

;; Highlight @type:name references in `duckdb-query' SQL strings.
;;
;; Basic usage:
;;
;;     (add-hook 'emacs-lisp-mode-hook #'duckdb-query-font-lock-mode)
;;
;; Select highlighting style interactively:
;;
;;     M-x duckdb-query-font-lock-select-preset
;;
;; Highlighted reference types:
;; - @org:name   Org table references
;; - @data:name  Elisp data bindings
;; - @val:name   SQL variable literals
;; - @sql:name   SQL fragment substitution
;;
;; Invalid references (e.g., @sql: inside (sql ...) wrapper) use
;; `duckdb-query-reference-invalid-face' to signal resolution failure.
;;
;; Also see `duckdb-query' for reference syntax documentation.

;;; Code:

(require 'font-lock)
(require 'cl-lib)

(defgroup duckdb-query-font-lock nil
  "Font-lock support for `duckdb-query' SQL strings."
  :group 'duckdb-query
  :prefix "duckdb-query-font-lock-")

;;;; Faces

(defface duckdb-query-reference-prefix-face
  '((t :inherit shadow))
  "Face for @type: prefix in `duckdb-query' references.

Applied by `duckdb-query-font-lock--fontify-references' to the
prefix portion of valid references (e.g., \"@val:\" in \"@val:name\").

Customized via `duckdb-query-font-lock-preset' or directly."
  :group 'duckdb-query-font-lock
  :package-version '(duckdb-query . "0.7.0"))

(defface duckdb-query-reference-name-face
  '((t :inherit font-lock-constant-face :weight bold))
  "Face for the name portion of @type:name references.

Applied by `duckdb-query-font-lock--fontify-references' to the
name portion of valid references (e.g., \"name\" in \"@val:name\").

Customized via `duckdb-query-font-lock-preset' or directly."
  :group 'duckdb-query-font-lock
  :package-version '(duckdb-query . "0.7.0"))

(defface duckdb-query-reference-invalid-face
  '((t :inherit warning :weight bold))
  "Face for references in semantically invalid positions.

Applied to @sql: and @org: references inside (sql ...) wrappers,
where substitution order prevents their resolution.  The @sql:
substitution occurs before @val: processing, so @sql: references
in (sql ...) expressions remain as literal text and cause parser
errors.

Also see `duckdb-query--substitute-sql-refs' for substitution order."
  :group 'duckdb-query-font-lock
  :package-version '(duckdb-query . "0.7.0"))

;;;; Presets

(defvar duckdb-query-font-lock-presets
  '((shadow-bold-constant
     :prefix (:inherit shadow)
     :name (:inherit font-lock-constant-face :weight bold))

    (shadow-bold-variable
     :prefix (:inherit shadow)
     :name (:inherit font-lock-variable-name-face :weight bold))

    (keyword-variable
     :prefix (:inherit font-lock-keyword-face)
     :name (:inherit font-lock-variable-name-face :weight bold))

    (uniform-type
     :prefix (:inherit font-lock-type-face)
     :name (:inherit font-lock-type-face))

    (uniform-keyword
     :prefix (:inherit font-lock-keyword-face)
     :name (:inherit font-lock-keyword-face))

    (italic-shadow-bold-constant
     :prefix (:inherit shadow :slant italic)
     :name (:inherit font-lock-constant-face :weight bold))

    (light-extra-bold
     :prefix (:inherit font-lock-constant-face :weight light)
     :name (:inherit font-lock-constant-face :weight extra-bold))

    (shadow-completions
     :prefix (:inherit shadow)
     :name (:inherit completions-common-part :weight bold))

    (italic-shadow-bold-underline
     :prefix (:inherit shadow :slant italic)
     :name (:inherit font-lock-constant-face :weight bold :underline t)))
  "Alist of highlighting presets for `duckdb-query' references.

Each entry is (NAME :prefix FACE-ATTRS :name FACE-ATTRS) where
FACE-ATTRS is a plist of face attributes.

Applied by `duckdb-query-font-lock-apply-preset'.
Selected via `duckdb-query-font-lock-select-preset'.")

(defcustom duckdb-query-font-lock-preset 'shadow-bold-constant
  "Current highlighting preset for `duckdb-query' references.

Controls appearance of `duckdb-query-reference-prefix-face' and
`duckdb-query-reference-name-face'.

Use `duckdb-query-font-lock-select-preset' to change interactively
with live preview."
  :type `(choice ,@(mapcar (lambda (p) `(const ,(car p)))
                           duckdb-query-font-lock-presets))
  :group 'duckdb-query-font-lock
  :set (lambda (sym val)
         (set-default sym val)
         (when (fboundp 'duckdb-query-font-lock-apply-preset)
           (duckdb-query-font-lock-apply-preset val)))
  :package-version '(duckdb-query . "0.7.0"))

;;;; Preset Application

(defun duckdb-query-font-lock-apply-preset (preset)
  "Apply PRESET to `duckdb-query' reference faces.

PRESET is a symbol naming an entry in `duckdb-query-font-lock-presets'.

Resets face attributes before applying new values.  Triggers
`font-lock-flush' in all buffers with `duckdb-query-font-lock-mode'.

Called by `duckdb-query-font-lock-select-preset' and the :set
function of `duckdb-query-font-lock-preset'."
  (let ((entry (assq preset duckdb-query-font-lock-presets)))
    (unless entry
      (error "Unknown preset: %s" preset))
    (let ((prefix-attrs (plist-get (cdr entry) :prefix))
          (name-attrs (plist-get (cdr entry) :name)))
      ;; Reset faces to default state
      (set-face-attribute 'duckdb-query-reference-prefix-face nil
                          :inherit nil :weight 'normal :slant 'normal
                          :underline nil :foreground 'unspecified
                          :background 'unspecified :box nil)
      (set-face-attribute 'duckdb-query-reference-name-face nil
                          :inherit nil :weight 'normal :slant 'normal
                          :underline nil :foreground 'unspecified
                          :background 'unspecified :box nil)
      ;; Apply new attributes
      (apply #'set-face-attribute 'duckdb-query-reference-prefix-face nil
             prefix-attrs)
      (apply #'set-face-attribute 'duckdb-query-reference-name-face nil
             name-attrs)
      ;; Refontify buffers with mode enabled
      (dolist (buf (buffer-list))
        (with-current-buffer buf
          (when (bound-and-true-p duckdb-query-font-lock-mode)
            (font-lock-flush)))))))

;;;; Preset Selection

(defvar duckdb-query-font-lock--select-history nil
  "Minibuffer history for `duckdb-query-font-lock-select-preset'.")

(defvar duckdb-query-font-lock--saved-preset nil
  "Saved preset for restoration on cancel.

Bound by `duckdb-query-font-lock-select-preset' before entering
minibuffer.  Restored if user cancels with \\[keyboard-quit].")

(defvar duckdb-query-font-lock--last-previewed nil
  "Last previewed preset to avoid redundant applications.

Compared against current candidate in `duckdb-query-font-lock--preview-preset'
to prevent re-applying the same preset on every keystroke.")

(defun duckdb-query-font-lock--preview-preset ()
  "Preview the currently selected preset in minibuffer.

Extracts current candidate from vertico, icomplete, or standard
completion and applies it via `duckdb-query-font-lock-apply-preset'.

Called by `post-command-hook' in minibuffer during preset selection."
  (when (minibufferp)
    (when-let* ((candidate (or
                            ;; Vertico
                            (and (bound-and-true-p vertico--index)
                                 (bound-and-true-p vertico--candidates)
                                 (>= vertico--index 0)
                                 (nth vertico--index vertico--candidates))
                            ;; Icomplete
                            (and (bound-and-true-p icomplete-mode)
                                 (car completion-all-sorted-completions))
                            ;; Standard completion
                            (let ((c (minibuffer-contents-no-properties)))
                              (when (assq (intern-soft c)
                                          duckdb-query-font-lock-presets)
                                c))))
                (preset (intern-soft candidate))
                ((assq preset duckdb-query-font-lock-presets))
                ((not (eq preset duckdb-query-font-lock--last-previewed))))
      (setq duckdb-query-font-lock--last-previewed preset)
      (duckdb-query-font-lock-apply-preset preset))))

(defun duckdb-query-font-lock--setup-preview ()
  "Setup preview hooks in minibuffer.

Installs `duckdb-query-font-lock--preview-preset' on `post-command-hook'
for live preview during `duckdb-query-font-lock-select-preset'."
  (setq duckdb-query-font-lock--last-previewed nil)
  (add-hook 'post-command-hook #'duckdb-query-font-lock--preview-preset nil t))

;;;###autoload
(defun duckdb-query-font-lock-select-preset (preset)
  "Select a highlighting PRESET with live preview.

Interactively, show completion with preview as you navigate
candidates.  Works with vertico, icomplete, fido-mode, and
standard completion.

PRESET is a symbol from `duckdb-query-font-lock-presets'.

On \\[keyboard-quit], restore the previous preset.

Also see `duckdb-query-font-lock-preset' for programmatic access."
  (interactive
   (let ((duckdb-query-font-lock--saved-preset duckdb-query-font-lock-preset)
         (selected nil))
     (minibuffer-with-setup-hook #'duckdb-query-font-lock--setup-preview
       (unwind-protect
           (setq selected
                 (intern
                  (completing-read
                   (format-prompt "Preset" duckdb-query-font-lock-preset)
                   (mapcar (lambda (p) (symbol-name (car p)))
                           duckdb-query-font-lock-presets)
                   nil t nil
                   'duckdb-query-font-lock--select-history
                   (symbol-name duckdb-query-font-lock-preset))))
         ;; On C-g, restore saved preset
         (unless selected
           (duckdb-query-font-lock-apply-preset
            duckdb-query-font-lock--saved-preset))))
     (list selected)))
  (when preset
    (setq duckdb-query-font-lock-preset preset)
    (duckdb-query-font-lock-apply-preset preset)
    (message "Applied preset: %s" preset)))

;;;; Context Detection

(defconst duckdb-query-font-lock--query-functions
  '(duckdb-query duckdb-query-value duckdb-query-row duckdb-query-column)
  "Functions whose first string argument is a SQL query.

Used by `duckdb-query-font-lock--find-duckdb-query-form' to identify
forms containing highlightable references.")

(defconst duckdb-query-font-lock--reference-regexp
  (rx (group "@" (or "org" "data" "val" "sql") ":")
      (group (any "a-zA-Z_") (* (any "a-zA-Z0-9_:/-"))))
  "Regexp matching @type:name references.

Group 1: @type: prefix.
Group 2: name.

Used by `duckdb-query-font-lock--fontify-references'.")

(defun duckdb-query-font-lock--find-duckdb-query-form ()
  "Find the containing `duckdb-query' form if point is inside one.

Return (FORM-START . FORM-END) if found, nil otherwise.

Traverses upward through sexp structure, checking each containing
list for a `duckdb-query' family function at its head.  Handles
quoted data structures correctly.

Called by `duckdb-query-font-lock--fontify-references'."
  (save-excursion
    (let ((depth 0))
      (catch 'found
        (while (< depth 50)  ; Safety limit
          (let ((paren-pos (nth 1 (syntax-ppss))))
            (unless paren-pos
              (throw 'found nil))
            (goto-char paren-pos)
            (cl-incf depth)
            ;; Check if this paren starts a duckdb-query call
            (save-excursion
              (forward-char 1)
              (skip-chars-forward " \t\n")
              (when (looking-at "\\_<\\(duckdb-query[-a-z]*\\)\\_>")
                (let ((func-name (intern (match-string 1))))
                  (when (memq func-name duckdb-query-font-lock--query-functions)
                    ;; Verify not quoted at top level
                    (goto-char paren-pos)
                    (let ((outer-paren (nth 1 (syntax-ppss))))
                      (when (or outer-paren
                                (not (memq (char-before paren-pos) '(?\' ?\`))))
                        (goto-char paren-pos)
                        (forward-sexp 1)
                        (throw 'found (cons paren-pos (point)))))))))))))))

(defun duckdb-query-font-lock--inside-sql-wrapper-p (pos)
  "Return non-nil if POS is inside a (sql ...) wrapper.

Checks parent sexps for a list starting with symbol `sql'.

Called by `duckdb-query-font-lock--context-at-pos'."
  (save-excursion
    (goto-char pos)
    (let ((depth 0))
      (catch 'found
        (while (< depth 20)  ; Safety limit
          (let ((paren-pos (nth 1 (syntax-ppss))))
            (unless paren-pos
              (throw 'found nil))
            (goto-char paren-pos)
            (when (looking-at "([ \t\n]*sql\\_>")
              (throw 'found t))
            (cl-incf depth)))
        nil))))

(defun duckdb-query-font-lock--in-sql-param-p (pos form-bounds)
  "Return non-nil if POS is inside a :sql parameter value in FORM-BOUNDS.

FORM-BOUNDS is (START . END) of the containing `duckdb-query' form.

Called by `duckdb-query-font-lock--context-at-pos'."
  (save-excursion
    (let ((form-start (car form-bounds))
          (form-end (cdr form-bounds)))
      (goto-char form-start)
      (forward-char 1)
      (skip-chars-forward " \t\n")
      ;; Skip function name
      (forward-sexp 1)
      ;; Parse keyword arguments
      (catch 'done
        (while (< (point) form-end)
          (skip-chars-forward " \t\n")
          (when (>= (point) form-end)
            (throw 'done nil))
          (cond
           ;; Keyword argument
           ((eq (char-after) ?:)
            (let ((kw-start (point)))
              (forward-sexp 1)
              (let ((keyword (buffer-substring-no-properties kw-start (point))))
                (skip-chars-forward " \t\n")
                (let ((val-start (point)))
                  (condition-case nil
                      (progn
                        (forward-sexp 1)
                        (when (string= keyword ":sql")
                          (when (and (>= pos val-start)
                                     (< pos (point)))
                            (throw 'done t))))
                    (scan-error (throw 'done nil)))))))
           ;; Non-keyword - skip
           (t
            (condition-case nil
                (forward-sexp 1)
              (scan-error (throw 'done nil))))))
        nil))))

(defun duckdb-query-font-lock--in-val-param-p (pos form-bounds)
  "Return non-nil if POS is inside a :val parameter value in FORM-BOUNDS.

FORM-BOUNDS is (START . END) of the containing `duckdb-query' form.

Called by `duckdb-query-font-lock--context-at-pos'."
  (save-excursion
    (let ((form-start (car form-bounds))
          (form-end (cdr form-bounds)))
      (goto-char form-start)
      (forward-char 1)
      (skip-chars-forward " \t\n")
      ;; Skip function name
      (forward-sexp 1)
      ;; Parse keyword arguments
      (catch 'done
        (while (< (point) form-end)
          (skip-chars-forward " \t\n")
          (when (>= (point) form-end)
            (throw 'done nil))
          (cond
           ;; Keyword argument
           ((eq (char-after) ?:)
            (let ((kw-start (point)))
              (forward-sexp 1)
              (let ((keyword (buffer-substring-no-properties kw-start (point))))
                (skip-chars-forward " \t\n")
                (let ((val-start (point)))
                  (condition-case nil
                      (progn
                        (forward-sexp 1)
                        (when (string= keyword ":val")
                          (when (and (>= pos val-start)
                                     (< pos (point)))
                            (throw 'done t))))
                    (scan-error (throw 'done nil)))))))
           ;; Non-keyword - skip
           (t
            (condition-case nil
                (forward-sexp 1)
              (scan-error (throw 'done nil))))))
        nil))))

(defun duckdb-query-font-lock--context-at-pos (pos form-bounds)
  "Determine the context of POS within `duckdb-query' FORM-BOUNDS.

Return one of:
  :query       Inside the query string argument
  :sql-binding Inside a string in :sql parameter
  :sql-wrapper Inside a string in (sql ...) wrapper in :val
  nil          Not in a recognized string context

FORM-BOUNDS is (START . END) of the containing `duckdb-query' form.

Called by `duckdb-query-font-lock--get-context'."
  (save-excursion
    (goto-char pos)
    (let ((ppss (syntax-ppss)))
      ;; Must be inside a string
      (unless (nth 3 ppss)
        (throw 'context-result nil))
      (let ((string-start (nth 8 ppss)))
        ;; Check for (sql ...) wrapper first - most specific
        (when (duckdb-query-font-lock--inside-sql-wrapper-p string-start)
          (throw 'context-result :sql-wrapper))
        ;; Check if in :sql parameter
        (when (duckdb-query-font-lock--in-sql-param-p string-start form-bounds)
          (throw 'context-result :sql-binding))
        ;; Check if in :val parameter (but not in sql wrapper)
        (when (duckdb-query-font-lock--in-val-param-p string-start form-bounds)
          ;; In :val but not in (sql ...) - alist values, not SQL strings
          (throw 'context-result nil))
        ;; Otherwise, assume query string or similar valid context
        :query))))

(defun duckdb-query-font-lock--get-context (pos form-bounds)
  "Get context for POS within FORM-BOUNDS.

Wrapper for `duckdb-query-font-lock--context-at-pos' that handles
the throw/catch control flow.

FORM-BOUNDS is (START . END) of the containing `duckdb-query' form.

Called by `duckdb-query-font-lock--fontify-references'."
  (catch 'context-result
    (duckdb-query-font-lock--context-at-pos pos form-bounds)))

(defun duckdb-query-font-lock--reference-valid-p (ref-type context)
  "Return non-nil if REF-TYPE is valid in CONTEXT.

REF-TYPE is a string: \"sql\", \"data\", \"val\", or \"org\".
CONTEXT is one of :query, :sql-binding, :sql-wrapper.

Validity rules:
- :query and :sql-binding allow all reference types
- :sql-wrapper only allows @data: and @val: (not @sql: or @org:)

The restriction on :sql-wrapper exists because @sql: substitution
occurs before @val: processing.  References to @sql: inside (sql ...)
expressions would remain as literal text.

Called by `duckdb-query-font-lock--fontify-references'."
  (pcase context
    ((or :query :sql-binding)
     t)
    (:sql-wrapper
     (member ref-type '("data" "val")))
    (_
     nil)))

;;;; Font-lock Implementation

(defun duckdb-query-font-lock--fontify-references (limit)
  "Fontify @type:name references up to LIMIT.

Search for references matching `duckdb-query-font-lock--reference-regexp'.
For each match inside a `duckdb-query' form, apply appropriate faces:
- Valid references: `duckdb-query-reference-prefix-face' and
  `duckdb-query-reference-name-face'
- Invalid references: `duckdb-query-reference-invalid-face'

Return non-nil if any references were fontified.

Installed by `duckdb-query-font-lock-mode' via `font-lock-add-keywords'."
  (let ((found nil))
    (while (re-search-forward duckdb-query-font-lock--reference-regexp limit t)
      (let* ((prefix-beg (match-beginning 1))
             (prefix-end (match-end 1))
             (name-beg (match-beginning 2))
             (name-end (match-end 2))
             (ref-type (buffer-substring-no-properties
                        (1+ prefix-beg)
                        (1- prefix-end)))
             (form-bounds (save-excursion
                            (goto-char prefix-beg)
                            (duckdb-query-font-lock--find-duckdb-query-form))))
        (when form-bounds
          (let ((context (duckdb-query-font-lock--get-context
                          prefix-beg form-bounds)))
            (when context
              (setq found t)
              (if (duckdb-query-font-lock--reference-valid-p ref-type context)
                  ;; Valid reference - normal highlighting
                  (progn
                    (add-face-text-property prefix-beg prefix-end
                                            'duckdb-query-reference-prefix-face)
                    (add-face-text-property name-beg name-end
                                            'duckdb-query-reference-name-face))
                ;; Invalid reference - warning highlighting
                (add-face-text-property prefix-beg name-end
                                        'duckdb-query-reference-invalid-face)))))))
    found))

(defconst duckdb-query-font-lock-keywords
  '((duckdb-query-font-lock--fontify-references))
  "Font-lock keywords for `duckdb-query' references.

Installed by `duckdb-query-font-lock-mode'.")

;;;; Minor Mode

;;;###autoload
(define-minor-mode duckdb-query-font-lock-mode
  "Highlight @type:name references in `duckdb-query' forms.

When enabled, references like @org:table, @data:name, @val:value,
and @sql:fragment are highlighted within SQL string arguments to
`duckdb-query' and related functions.

References are highlighted in:
- The query string (first argument)
- Strings inside :sql parameter bindings
- Strings inside (sql ...) wrappers in :val bindings

References in invalid positions (e.g., @sql: inside a (sql ...)
wrapper) are highlighted with `duckdb-query-reference-invalid-face'.

To enable globally:

    (add-hook \\='emacs-lisp-mode-hook #\\='duckdb-query-font-lock-mode)

Use `duckdb-query-font-lock-select-preset' to change highlighting
style interactively with live preview.

Also see `duckdb-query' for reference syntax documentation."
  :lighter " DQ"
  :group 'duckdb-query-font-lock
  (if duckdb-query-font-lock-mode
      (progn
        (duckdb-query-font-lock-apply-preset duckdb-query-font-lock-preset)
        (font-lock-add-keywords nil duckdb-query-font-lock-keywords 'append))
    (font-lock-remove-keywords nil duckdb-query-font-lock-keywords))
  (font-lock-flush))

(provide 'duckdb-query-font-lock)

;;; duckdb-query-font-lock.el ends here
