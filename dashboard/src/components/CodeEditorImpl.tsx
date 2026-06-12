import { css } from '@codemirror/lang-css'
import { json, jsonParseLinter } from '@codemirror/lang-json'
import { python } from '@codemirror/lang-python'
import { StreamLanguage } from '@codemirror/language'
import { toml as tomlMode } from '@codemirror/legacy-modes/mode/toml'
import { linter } from '@codemirror/lint'
import { oneDark } from '@codemirror/theme-one-dark'
import { EditorView, ViewPlugin } from '@codemirror/view'
import CodeMirror from '@uiw/react-codemirror'

import { useTheme } from '@/components/use-theme'

import type { CodeEditorProps, Language } from './CodeEditor'

// eslint-disable-next-line @typescript-eslint/no-explicit-any
const languageExtensions: Record<Language, any[]> = {
  python: [python()],
  json: [json(), linter(jsonParseLinter())],
  toml: [StreamLanguage.define(tomlMode)],
  css: [css()],
  text: [],
}

const dashboardCodeScrollerMarker = ViewPlugin.fromClass(
  class {
    constructor(view: EditorView) {
      // 标记 CodeMirror 的真实 scrollDOM，避免依赖它内部 class 的注入顺序。
      view.scrollDOM.dataset.dashboardCodeScroller = 'true'
    }
  }
)

export default function CodeEditorImpl({
  value,
  onChange,
  language = 'text',
  readOnly = false,
  height = '400px',
  minHeight,
  maxHeight,
  placeholder,
  theme,
  className = '',
}: CodeEditorProps) {
  const { resolvedTheme } = useTheme()

  const extensions = [
    ...(languageExtensions[language] || []),
    EditorView.lineWrapping,
    // 应用 JetBrains Mono 字体
    EditorView.theme({
      '&': {
        fontFamily: '"JetBrains Mono", "Fira Code", "Consolas", "Monaco", monospace',
        minHeight: 0,
      },
      '.cm-content': {
        fontFamily: '"JetBrains Mono", "Fira Code", "Consolas", "Monaco", monospace',
      },
      '.cm-gutters': {
        fontFamily: '"JetBrains Mono", "Fira Code", "Consolas", "Monaco", monospace',
      },
      '.cm-scroller': {
        fontFamily: '"JetBrains Mono", "Fira Code", "Consolas", "Monaco", monospace',
        minHeight: 0,
        overflow: 'auto !important',
        overscrollBehavior: 'contain',
        touchAction: 'pan-x pan-y',
      },
    }),
    dashboardCodeScrollerMarker,
  ]

  if (readOnly) {
    extensions.push(EditorView.editable.of(false))
  }

  // 如果外部传了 theme prop 则使用，否则从 context 自动获取
  const effectiveTheme = theme ?? resolvedTheme

  return (
    <div
      data-dashboard-code-editor="true"
      className={`custom-scrollbar min-h-0 overflow-hidden rounded-md border ${className}`}
    >
      <CodeMirror
        className="min-h-0"
        value={value}
        height={height}
        minHeight={minHeight}
        maxHeight={maxHeight}
        style={{ height, minHeight, maxHeight }}
        theme={effectiveTheme === 'dark' ? oneDark : undefined}
        extensions={extensions}
        onChange={onChange}
        placeholder={placeholder}
        basicSetup={{
          lineNumbers: true,
          highlightActiveLineGutter: true,
          highlightSpecialChars: true,
          history: true,
          foldGutter: true,
          drawSelection: true,
          dropCursor: true,
          allowMultipleSelections: true,
          indentOnInput: true,
          syntaxHighlighting: true,
          bracketMatching: true,
          closeBrackets: true,
          autocompletion: true,
          rectangularSelection: true,
          crosshairCursor: true,
          highlightActiveLine: true,
          highlightSelectionMatches: true,
          closeBracketsKeymap: true,
          defaultKeymap: true,
          searchKeymap: true,
          historyKeymap: true,
          foldKeymap: true,
          completionKeymap: true,
          lintKeymap: true,
        }}
      />
    </div>
  )
}
