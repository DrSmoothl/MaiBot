import { Link, useParams } from '@tanstack/react-router'
import { useCallback, useEffect, useRef, useState } from 'react'
import { useTranslation } from 'react-i18next'

import { ErrorBoundary } from '@/components/error-boundary'
import { PluginWebUIRenderer } from '@/components/plugin-webui-renderer'
import { Alert, AlertDescription } from '@/components/ui/alert'
import {
  AlertDialog,
  AlertDialogAction,
  AlertDialogCancel,
  AlertDialogContent,
  AlertDialogDescription,
  AlertDialogFooter,
  AlertDialogHeader,
  AlertDialogTitle,
} from '@/components/ui/alert-dialog'
import { Button } from '@/components/ui/button'
import {
  invokePluginWebUI,
  refreshPluginWebUI,
  usePluginWebUI,
} from '@/lib/plugin-webui'
import type { APIBinding, Scalar, WebUIPage, WebUINode } from '@/lib/plugin-webui'

function initialValues(nodes: WebUINode[]): Record<string, Scalar> {
  const values: Record<string, Scalar> = {}
  const pending = [...nodes]
  while (pending.length) {
    const node = pending.pop()!
    if (node.name !== null && (node.value === null || typeof node.value !== 'object'))
      values[node.name] = node.value
    pending.push(...node.children)
  }
  return values
}

function argumentsFor(binding: APIBinding, values: Record<string, Scalar>): Record<string, Scalar> {
  return Object.fromEntries(
    Object.keys(binding.parameters)
      .filter((name) => values[name] !== undefined && values[name] !== null)
      .map((name) => [name, values[name]])
  )
}

function ExtensionPage({ pluginId, page }: { pluginId: string; page: WebUIPage }) {
  const { t } = useTranslation()
  const [values, setValues] = useState(() => initialValues(page.content))
  const valuesRef = useRef(values)
  const [data, setData] = useState<Record<string, unknown>>({})
  const [busy, setBusy] = useState(false)
  const busyRef = useRef(false)
  const [loaded, setLoaded] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [message, setMessage] = useState<string | null>(null)
  const [confirmation, setConfirmation] = useState<string | null>(null)
  const [revision, setRevision] = useState(0)
  const alive = useRef(true)
  const controller = useRef<AbortController | null>(null)

  const loadQueries = useCallback(
    async (signal: AbortSignal) => {
      const results: Record<string, unknown> = {}
      // 顺序执行，遵守插件网关的并发上限；整个快照成功后才替换页面数据。
      for (const [name, binding] of Object.entries(page.queries)) {
        results[name] = await invokePluginWebUI(
          pluginId,
          page.id,
          'queries',
          name,
          argumentsFor(binding, valuesRef.current),
          false,
          signal
        )
      }
      if (!signal.aborted && alive.current) {
        setData(results)
        setLoaded(true)
        setRevision((value) => value + 1)
      }
    },
    [page, pluginId]
  )

  const refresh = useCallback(async () => {
    if (busyRef.current) return
    busyRef.current = true
    setBusy(true)
    setError(null)
    controller.current = new AbortController()
    const operation = controller.current
    try {
      await loadQueries(operation.signal)
    } catch (error) {
      if (alive.current && !operation.signal.aborted) setError(String(error))
    } finally {
      if (controller.current === operation) {
        busyRef.current = false
        if (alive.current) setBusy(false)
      }
    }
  }, [loadQueries])

  useEffect(() => {
    alive.current = true
    void refresh()
    return () => {
      alive.current = false
      controller.current?.abort()
      busyRef.current = false
    }
  }, [refresh])

  const execute = async (name: string, confirmed = false) => {
    if (busyRef.current) return
    const binding = page.actions[name]
    if (binding.confirmation && !confirmed) {
      setConfirmation(name)
      return
    }
    setConfirmation(null)
    busyRef.current = true
    setBusy(true)
    setError(null)
    setMessage(null)
    controller.current = new AbortController()
    const operation = controller.current
    try {
      await invokePluginWebUI(
        pluginId,
        page.id,
        'actions',
        name,
        argumentsFor(binding, valuesRef.current),
        confirmed,
        operation.signal
      )
      if (alive.current && !operation.signal.aborted) setMessage(t('pluginWebUI.completed'))
      await loadQueries(operation.signal)
    } catch (error) {
      if (alive.current && !operation.signal.aborted) setError(String(error))
    } finally {
      if (controller.current === operation) {
        busyRef.current = false
        if (alive.current) setBusy(false)
      }
    }
  }

  return (
    <div className="mx-auto max-w-7xl space-y-6 p-4 md:p-6">
      <div className="flex items-start justify-between gap-4">
        <div>
          <h1 className="text-2xl font-semibold">{page.title}</h1>
          <p className="text-muted-foreground text-sm">{page.description}</p>
          <p className="text-muted-foreground mt-1 text-xs">
            {t('pluginWebUI.providedBy', { plugin: pluginId })}
          </p>
        </div>
        <Button
          variant="outline"
          disabled={busy}
          onClick={() => {
            void refresh()
          }}
        >
          {t('pluginWebUI.refresh')}
        </Button>
      </div>
      {error && (
        <Alert variant="destructive">
          <AlertDescription>{error}</AlertDescription>
        </Alert>
      )}
      {message && (
        <p role="status" className="text-muted-foreground text-sm">
          {message}
        </p>
      )}
      {!loaded && busy && <p role="status">{t('pluginWebUI.loading')}</p>}
      {
        <ErrorBoundary
          key={revision}
          fallback={
            <Alert variant="destructive">
              <AlertDescription>{t('pluginWebUI.invalidData')}</AlertDescription>
            </Alert>
          }
        >
          <div className="space-y-4">
            <PluginWebUIRenderer
              nodes={page.content}
              data={data}
              values={values}
              busy={busy}
              pendingData={!loaded}
              onChange={(name, value) => {
                const next = { ...valuesRef.current, [name]: value }
                valuesRef.current = next
                setValues(next)
              }}
              onAction={(name) => {
                void execute(name)
              }}
            />
          </div>
        </ErrorBoundary>
      }
      <AlertDialog
        open={confirmation !== null}
        onOpenChange={(open) => {
          if (!open) setConfirmation(null)
        }}
      >
        <AlertDialogContent>
          <AlertDialogHeader>
            <AlertDialogTitle>{t('pluginWebUI.confirmTitle')}</AlertDialogTitle>
            <AlertDialogDescription>
              {confirmation ? page.actions[confirmation].confirmation : ''}
            </AlertDialogDescription>
          </AlertDialogHeader>
          <AlertDialogFooter>
            <AlertDialogCancel>{t('pluginWebUI.cancel')}</AlertDialogCancel>
            <AlertDialogAction
              onClick={() => {
                if (confirmation) void execute(confirmation, true)
              }}
            >
              {t('pluginWebUI.confirm')}
            </AlertDialogAction>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>
    </div>
  )
}

export function PluginWebUIPage() {
  const { pluginId, pageId } = useParams({ from: '/protected/extensions/$pluginId/$pageId' })
  const registry = usePluginWebUI()
  const { t } = useTranslation()
  const extension = registry.extensions.find((item) => item.plugin_id === pluginId)
  const page = extension?.pages.find((item) => item.id === pageId)
  if (registry.loading) return <p className="p-6">{t('pluginWebUI.loading')}</p>
  if (registry.error)
    return (
      <div className="space-y-4 p-6">
        <Alert variant="destructive">
          <AlertDescription>{registry.error}</AlertDescription>
        </Alert>
        <Button
          onClick={() => {
            void refreshPluginWebUI()
          }}
        >
          {t('pluginWebUI.refresh')}
        </Button>
      </div>
    )
  if (!page || registry.preferences.hidden.includes(pluginId))
    return (
      <p className="p-6">
        {t('pluginWebUI.unavailable')}{' '}
        <Link to="/plugin-config" hash="webui-extensions">
          {t('pluginWebUI.manage')}
        </Link>
      </p>
    )
  // 注册表刷新得到相同声明时保留表单；声明发生变化时重建页面状态。
  return (
    <ExtensionPage
      key={`${pluginId}/${pageId}/${JSON.stringify(page)}`}
      pluginId={pluginId}
      page={page}
    />
  )
}
